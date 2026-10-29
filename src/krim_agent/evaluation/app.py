"""普通Eval装配；readonly不创建adapter，也不进入Krim启动恢复。"""

from __future__ import annotations

import asyncio
from pathlib import Path
from typing import Any

from sqlalchemy import select, update

from krim_agent.contracts import ServiceError

from . import domain, repository
from .adapters.local_process import LocalProcessAdapter
from .adapters.process import ProcessOwner
from .adapters.profiles import (
    CampaignBootstrap,
    CampaignStoreBinding,
    NativeProfile,
    bind_bootstrap,
)
from .domain import TaskSpec
from .kernel import EvalKernel
from .lifecycle import settled
from .ports import AdapterLease
from .repository import Repository


class EvalApplication:
    def __init__(
        self,
        repository: Repository,
        adapter: Any,
        *,
        leases: list[AdapterLease] | None = None,
        owner: ProcessOwner | None = None,
        owns_owner: bool = True,
        campaign_binding: CampaignStoreBinding | None = None,
        scope: str = "local",
        actor: str = "local-user",
        sources: dict[str, Any] | None = None,
        clock: Any = None,
    ) -> None:
        self.repository, self.adapter = repository, adapter
        self.kernel = EvalKernel(repository)
        self._close_task: asyncio.Task[None] | None = None
        self.leases = leases or ([] if adapter is None else [AdapterLease(adapter, owned=True)])
        self.owner, self.owns_owner = owner, owns_owner
        self.campaign_binding = campaign_binding
        from .continuous import ContinuousEvaluation
        from .continuous_metrics import register_metrics
        from .sources import EvalSourceReader

        register_metrics(repository.metric_registry)
        self.continuous = ContinuousEvaluation(
            repository,
            self.kernel,
            scope=scope,
            actor=actor,
            sources={"current-eval": EvalSourceReader(repository, scope=scope)} | (sources or {}),
            clock=clock,
        )

    async def run(self, request: TaskSpec) -> dict[str, Any]:
        # 在首await之前脱离调用者的可变嵌套容器。
        frozen = request.model_dump(mode="json")
        return await self.kernel.submit(frozen)

    async def close(self) -> None:
        self.kernel._external()
        if self._close_task is None:
            self._close_task = asyncio.create_task(self._close())
        await settled(self._close_task)

    async def _close(self) -> None:
        failure: BaseException | None = None
        try:
            await self.kernel.close()
        except BaseException as error:
            failure = error
        try:
            owned = {id(lease.adapter): lease.adapter for lease in self.leases if lease.owned}
            results = await asyncio.gather(
                *(a.close() for a in owned.values()),
                return_exceptions=True,
            )
            for value in results:
                if isinstance(value, BaseException):
                    failure = failure or value
                elif not value.closed:
                    failure = failure or ServiceError("execution_unknown")
            if self.owner is not None and self.owns_owner:
                try:
                    result = await self.owner.close()
                    if not result.closed:
                        failure = failure or ServiceError("execution_unknown")
                except BaseException as error:
                    failure = failure or error
        finally:
            await self.repository.close()
        if failure is not None:
            raise failure

    async def __aenter__(self) -> EvalApplication:
        return self

    async def __aexit__(self, *args: Any) -> None:
        await self.close()


async def open_eval(
    path: Path,
    *,
    workspace: Path | None = None,
    readonly: bool = False,
    native_profiles: list[NativeProfile] | None = None,
    campaign_bootstrap: CampaignBootstrap | None = None,
    adapters: dict[str, AdapterLease] | None = None,
    process_owner: ProcessOwner | None = None,
    execution_enabled: bool = True,
    scope: str = "local",
    actor: str = "local-user",
    sources: dict[str, Any] | None = None,
    clock: Any = None,
) -> EvalApplication:
    profiles = list(native_profiles or [])
    if not execution_enabled and (
        profiles or adapters or campaign_bootstrap is not None or process_owner is not None
    ):
        raise ServiceError("invalid_input", 422)
    if readonly and (profiles or campaign_bootstrap is not None or adapters):
        raise ServiceError("readonly")
    binding: CampaignStoreBinding | None = None
    if campaign_bootstrap is not None:
        campaign_bootstrap.check()
        present = await settled(asyncio.to_thread(lambda: path.exists() or path.is_symlink()))
        if (
            profiles
            or adapters
            or readonly
            or path != campaign_bootstrap.root / "eval.sqlite"
            or present
        ):
            raise ServiceError("invalid_input")
        campaign_bootstrap.root.mkdir(parents=True, exist_ok=False)
    elif profiles:
        binding = CampaignStoreBinding(domain.clone(profiles[0].binding.admission))
        for profile in profiles:
            profile.assert_current()
            if profile.binding.admission != binding.admission:
                raise ServiceError("target_binding_changed")
        binding.assert_store(path)
        reader = await Repository.open(path, readonly=True)
        try:
            async with reader.engine.begin() as conn:
                marker = (await conn.execute(select(repository.meta.c.body))).scalar_one()
                if marker.get("native_campaign") != binding.marker:
                    raise ServiceError("target_binding_changed")
                binding.assert_store(path)
        finally:
            await settled(reader.close())
        binding.assert_store(path)
    repo = await Repository.open(path, readonly=readonly)
    owner: ProcessOwner | None = None
    leases: list[AdapterLease] = []
    try:
        if campaign_bootstrap is not None:
            binding = bind_bootstrap(campaign_bootstrap)

            async def initialize(conn: Any) -> None:
                marker = (await conn.execute(select(repository.meta.c.body))).scalar_one()
                used = (
                    await conn.execute(select(repository.tasks.c.id).limit(1))
                ).scalar_one_or_none()
                if used is not None or "native_campaign" in marker:
                    raise ServiceError("key_conflict")
                assert binding is not None
                binding.assert_store(path, writer_fd=repo._writer_fd)
                await conn.execute(
                    update(
                        repository.meta,
                    ).values(body=marker | {"native_campaign": binding.marker}),
                )
                binding.assert_store(path, writer_fd=repo._writer_fd)

            await repo._write(initialize)
            return EvalApplication(repo, None, leases=[], campaign_binding=binding)
        if binding is not None:
            binding.assert_store(path, writer_fd=repo._writer_fd)
            async with repo.engine.begin() as conn:
                marker = (await conn.execute(select(repository.meta.c.body))).scalar_one()
                if not repo.healthy or marker.get("native_campaign") != binding.marker:
                    raise ServiceError("execution_unknown")
            binding.assert_store(path, writer_fd=repo._writer_fd)
        owner = None if readonly or not execution_enabled else process_owner or ProcessOwner()
        adapter = (
            None
            if readonly or not execution_enabled
            else LocalProcessAdapter(
                workspace or path.parent / "trials",
                owner,
            )
        )
        if adapter is not None:
            repo.register("reconcile-reference", adapter)
            leases.append(AdapterLease(adapter, owned=True))
        for key, lease in (adapters or {}).items():
            repo.register(key, lease.adapter)
            leases.append(lease)

        async def reservation(trial: Any, ref: Any) -> None:
            assert binding is not None
            binding.assert_store(path, writer_fd=repo._writer_fd)
            snapshot = await repo.snapshot(trial.task_id)
            binding.assert_store(path, writer_fd=repo._writer_fd)
            receipt = snapshot["environment"].get("native_admission")
            item = next(
                i
                for i in snapshot["items"]
                if i["case_id"] == trial.case_id and i["repetition"] == trial.repetition
            )
            if (
                not isinstance(receipt, dict)
                or receipt.get("task_id") != trial.task_id
                or receipt.get(
                    "store_binding_digest",
                )
                != binding.admission["store_binding_digest"]
                or receipt.get("request_hash") != snapshot["request_hash"]
                or receipt.get("reserved") is not True
                or item["status"] != "executing"
                or snapshot["stop_requested"]
                or snapshot["status"] != "running"
                or receipt.get("case_id") != trial.case_id
                or receipt.get("repetition") != trial.repetition
                or item["execution_ref"] != ref.id
            ):
                raise ServiceError("execution_unknown")

        if profiles:
            from .adapters.claude_code import ClaudeCodeAdapter
            from .adapters.codex_cli import CodexCLIAdapter

            assert owner is not None
            for profile in profiles:
                kind = CodexCLIAdapter if profile.family == "codex" else ClaudeCodeAdapter
                native = kind(profile, owner, reservation)
                repo.register(profile.key, native)
                leases.append(AdapterLease(native, owned=True))
        return EvalApplication(
            repo,
            adapter,
            leases=leases,
            owner=owner,
            owns_owner=process_owner is None,
            campaign_binding=binding,
            scope=scope,
            actor=actor,
            sources=sources,
            clock=clock,
        )
    except BaseException:
        await asyncio.gather(
            *(lease.adapter.close() for lease in leases if lease.owned),
            return_exceptions=True,
        )
        if owner is not None and process_owner is None:
            await settled(owner.close())
        await settled(repo.close())
        raise

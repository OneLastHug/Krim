"""独立 shadow Git 检查点；只接收已校验内容，不读取项目 Git 配置。

保留引用按持有者独立管理：refs/holds/<project>/<checkpoint>/<kind>/<owner>，
kind 限 job/restore-target/restore-before/capture-staging/user；用户引用固定
owner='owner'，其余 owner 为严格 UUID，不接受路径。旧 refs/retained 是 legacy
保护，永不自动释放，只能显式迁移。GC 走两阶段：cleanup_preview 落盘计划，
cleanup_confirm 持锁重校验候选与保护根后以 update-ref --stdin 事务删引用，
再 reflog expire + gc 并计量物理回收；失败存 reclaim_pending，同键可重试。
"""

from __future__ import annotations

import builtins
import contextlib
import fcntl
import hashlib
import json
import os
import re
import signal
import stat
import subprocess
import time
import uuid
from collections.abc import Iterator
from contextlib import contextmanager
from contextvars import ContextVar
from pathlib import Path
from typing import Any

from krim_agent.contracts import ServiceError
from krim_agent.sandbox.filesystem import MAX_FILE, _parts
from krim_agent.sandbox.quota import QuotaLimits
from krim_agent.sandbox.xfs import internal_id, private_directory

_DEADLINE: ContextVar[float | None] = ContextVar("checkpoint_deadline", default=None)

OID = re.compile(r"[0-9a-f]{40}\Z")
PLAN_DIGEST = re.compile(r"[0-9a-f]{64}\Z")
OWNER_KINDS = ("job", "restore-target", "restore-before", "capture-staging", "user")
USER_OWNER = "owner"
PLAN_TTL_SECONDS = 900
PLAN_MAX_BYTES = 64 * 1024
PLAN_MAX_CANDIDATES = 1000
CANDIDATE_REASONS = ("expired", "count_excess")


def _owner(kind: object, owner: object) -> tuple[str, str]:
    """校验持有者：kind 白名单；owner 是严格 UUID，user 固定 'owner'，不接受路径。"""
    if type(kind) is not str or type(owner) is not str or kind not in OWNER_KINDS:
        raise ServiceError("invalid_input", 422)
    if kind == "user":
        if owner != USER_OWNER:
            raise ServiceError("invalid_input", 422)
        return kind, owner
    try:
        if str(uuid.UUID(owner)) != owner:
            raise ValueError("owner不是规范UUID")
    except (ValueError, AttributeError):
        raise ServiceError("invalid_input", 422) from None
    return kind, owner


def _terminate(process: subprocess.Popen[bytes]) -> None:
    """超时后杀掉整个维护进程组，避免 git 子进程残留；再以有界等待收尸。"""
    with contextlib.suppress(OSError, ProcessLookupError):
        os.killpg(os.getpgid(process.pid), signal.SIGKILL)
    try:
        process.communicate(timeout=5)
    except (OSError, subprocess.TimeoutExpired, ValueError):
        with contextlib.suppress(OSError):
            process.kill()
        with contextlib.suppress(OSError, subprocess.TimeoutExpired, ValueError):
            process.wait(timeout=5)


class ShadowCheckpoints:
    """对象存储基础；调用方须持有项目租约并另行保证检查点区硬配额。"""

    def __init__(self, root: Path) -> None:
        self.root = private_directory(root)
        self.identity = (root.stat().st_dev, root.stat().st_ino)

    def _current(self) -> None:
        private_directory(self.root)
        info = self.root.stat()
        if (info.st_dev, info.st_ino) != self.identity:
            raise ServiceError("artifact_invalid")

    @contextmanager
    def _account(self, environment_id: str) -> Iterator[Path]:
        self._current()
        internal_id(environment_id)
        account = self.root / environment_id
        account.mkdir(mode=0o700, exist_ok=True)
        private_directory(account)
        if account.stat().st_mode & 0o077 or account.stat().st_uid != os.geteuid():
            raise ServiceError("artifact_invalid")
        token = _DEADLINE.set(time.monotonic() + 30)
        fd = os.open(account / "maintenance.lock", os.O_RDWR | os.O_CREAT | os.O_NOFOLLOW, 0o600)
        try:
            info = os.fstat(fd)
            if not stat.S_ISREG(info.st_mode) or info.st_nlink != 1:
                raise ServiceError("facts_corrupt")
            try:
                fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError:
                raise ServiceError("workspace_busy") from None
            repo = account / "objects.git"
            if not repo.exists():
                self._git(repo, ["init", "--bare", "--template=", str(repo)], initializing=True)
            private_directory(repo)
            yield repo
            self._current()
        finally:
            os.close(fd)
            _DEADLINE.reset(token)

    def _git(
        self,
        repo: Path,
        arguments: list[str],
        data: bytes | None = None,
        *,
        initializing: bool = False,
    ) -> bytes:
        self._current()
        deadline = _DEADLINE.get()
        remaining = 15.0 if deadline is None else min(15.0, deadline - time.monotonic())
        if remaining <= 0:
            raise ServiceError("sandbox_limit")
        environment = {
            "PATH": "/usr/bin:/bin",
            "HOME": "/nonexistent",
            "LANG": "C.UTF-8",
            "GIT_CONFIG_NOSYSTEM": "1",
            "GIT_CONFIG_GLOBAL": "/dev/null",
            "GIT_CONFIG_SYSTEM": "/dev/null",
            "GIT_TERMINAL_PROMPT": "0",
            "GIT_AUTHOR_NAME": "Krim Checkpoint",
            "GIT_AUTHOR_EMAIL": "checkpoint@localhost",
            "GIT_COMMITTER_NAME": "Krim Checkpoint",
            "GIT_COMMITTER_EMAIL": "checkpoint@localhost",
        }
        command = [
            "/usr/bin/git",
            "-c",
            "core.hooksPath=/dev/null",
            "-c",
            "core.fsmonitor=false",
            "-c",
            "core.attributesFile=/dev/null",
            "-c",
            "gc.auto=0",
            "-c",
            "protocol.allow=never",
            "-c",
            "safe.directory=" + str(repo),
        ]
        if not initializing:
            command += ["--git-dir=" + str(repo)]
        try:
            process = subprocess.Popen(
                command + arguments,
                stdin=subprocess.PIPE if data is not None else subprocess.DEVNULL,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                cwd=self.root,
                env=environment,
                start_new_session=True,
            )
        except OSError:
            raise ServiceError("sandbox_backend_unavailable", 503) from None
        try:
            stdout, _ = process.communicate(input=data, timeout=remaining)
        except (OSError, subprocess.TimeoutExpired):
            _terminate(process)
            raise ServiceError("sandbox_backend_unavailable", 503) from None
        if process.returncode:
            raise ServiceError("sandbox_backend_unavailable", 503)
        if len(stdout) > MAX_FILE + 1:
            raise ServiceError("artifact_limit")
        return stdout

    def _oid(self, repo: Path, arguments: list[str], data: bytes | None = None) -> str:
        oid = self._git(repo, arguments, data).decode().strip()
        if not OID.fullmatch(oid):
            raise ServiceError("facts_corrupt")
        return oid

    @staticmethod
    def _size(repo: Path) -> int:
        total = 0
        count = 0
        for directory, folders, files in os.walk(repo, followlinks=False):
            for name in [*folders, *files]:
                count += 1
                if count > 20000:
                    raise ServiceError("artifact_limit")
                info = os.lstat(Path(directory) / name)
                if not (stat.S_ISDIR(info.st_mode) or stat.S_ISREG(info.st_mode)):
                    raise ServiceError("facts_corrupt")
                total += info.st_blocks * 512
        return total

    def _hold_ref(self, project_id: str, checkpoint_id: str, kind: str, owner: str) -> str:
        return f"refs/holds/{project_id}/{checkpoint_id}/{kind}/{owner}"

    def _references(self, repo: Path, project_id: str) -> dict[str, list[dict[str, str]]]:
        listing = (
            self._git(
                repo, ["for-each-ref", "--format=%(refname)", "refs/holds/" + project_id + "/"]
            )
            .decode()
            .splitlines()
        )
        if len(listing) > 2000:
            raise ServiceError("artifact_limit")
        holds: dict[str, list[dict[str, str]]] = {}
        for refname in listing:
            parts = refname.split("/")
            if len(parts) != 6 or parts[1] != "holds" or parts[2] != project_id:
                raise ServiceError("facts_corrupt")
            checkpoint, kind, owner = parts[3], parts[4], parts[5]
            try:
                internal_id(checkpoint)
            except ServiceError:
                raise ServiceError("facts_corrupt") from None
            if kind not in OWNER_KINDS:
                raise ServiceError("facts_corrupt")
            if kind == "user":
                if owner != USER_OWNER:
                    raise ServiceError("facts_corrupt")
            else:
                try:
                    uuid.UUID(owner)
                except (ValueError, AttributeError):
                    raise ServiceError("facts_corrupt") from None
            holds.setdefault(checkpoint, []).append({"kind": kind, "owner": owner})
        for entries in holds.values():
            entries.sort(key=lambda item: (item["kind"], item["owner"]))
        return holds

    def _metadata(self, repo: Path, project_id: str) -> list[dict[str, Any]]:
        refs = (
            self._git(
                repo,
                [
                    "for-each-ref",
                    "--format=%(refname) %(objectname)",
                    "refs/krim/" + project_id + "/",
                ],
            )
            .decode()
            .splitlines()
        )
        if len(refs) > 1000:
            raise ServiceError("artifact_limit")
        retained = set(
            self._git(
                repo, ["for-each-ref", "--format=%(refname)", "refs/retained/" + project_id + "/"]
            )
            .decode()
            .splitlines()
        )
        holds = self._references(repo, project_id)
        result = []
        for line in refs:
            ref, commit = line.split()
            if not OID.fullmatch(commit):
                raise ServiceError("facts_corrupt")
            metadata = json.loads(self._git(repo, ["show", commit + ":manifest.json"]))
            checkpoint = ref.rsplit("/", 1)[-1]
            internal_id(checkpoint)
            if (
                metadata.get("checkpoint_id") != checkpoint
                or metadata.get("project_id") != project_id
            ):
                raise ServiceError("facts_corrupt")
            references = holds.get(checkpoint, [])
            result.append(
                {
                    **metadata,
                    "ref": ref,
                    "commit": commit,
                    "references": references,
                    "pinned": f"refs/retained/{project_id}/{checkpoint}" in retained
                    or any(item["kind"] == "user" for item in references),
                }
            )
        return sorted(result, key=lambda row: (row["created_at"], row["checkpoint_id"]))

    @staticmethod
    def _protected(row: dict[str, Any]) -> bool:
        return bool(row["pinned"]) or bool(row["references"])

    def _prune(self, repo: Path, project_id: str, limits: QuotaLimits, *, reserve: int = 0) -> None:
        rows = self._metadata(repo, project_id)
        cutoff = time.time() - limits.checkpoint_retention_seconds
        remove = []
        for row in rows:
            if row["created_at"] < cutoff and not self._protected(row):
                remove.append(row)
        remaining = [row for row in rows if row not in remove]
        excess = max(0, len(remaining) + reserve - limits.checkpoint_count)
        candidates = [row for row in remaining if not self._protected(row)]
        if len(candidates) < excess:
            raise ServiceError("sandbox_limit")
        remove.extend(candidates[:excess])
        for row in remove:
            self._git(repo, ["update-ref", "-d", row["ref"], row["commit"]])
        if remove:
            self._reclaim(repo)

    def _reclaim(self, repo: Path) -> None:
        self._git(repo, ["reflog", "expire", "--expire=now", "--all"])
        self._git(repo, ["gc", "--prune=now", "--quiet"])

    def list(self, environment_id: str, project_id: str) -> list[dict[str, Any]]:
        internal_id(project_id)
        with self._account(environment_id) as repo:
            return self._metadata(repo, project_id)

    def prune(self, environment_id: str, project_id: str, limits: QuotaLimits) -> dict[str, int]:
        internal_id(project_id)
        with self._account(environment_id) as repo:
            self._prune(repo, project_id, limits)
            self._reclaim(repo)
            return {
                "count": len(self._metadata(repo, project_id)),
                "physical_bytes": self._size(repo),
            }

    def capture(
        self,
        environment_id: str,
        project_id: str,
        files: dict[str, bytes],
        excluded: builtins.list[dict[str, str]],
        limits: QuotaLimits,
    ) -> dict[str, Any]:
        internal_id(project_id)
        if (
            not limits.checkpoint_bytes
            or not limits.checkpoint_count
            or not limits.checkpoint_retention_seconds
        ):
            raise ServiceError("sandbox_limit")
        if type(files) is not dict or len(files) > 4096 or len(excluded) > 4096:
            raise ServiceError("artifact_limit")
        total = 0
        for name, body in files.items():
            _parts(name)
            if type(body) is not bytes or len(body) > MAX_FILE:
                raise ServiceError("artifact_limit")
            total += len(body)
        if total > 32 * 1024**2:
            raise ServiceError("artifact_limit")
        digest = hashlib.sha256(
            json.dumps(
                [[name, hashlib.sha256(body).hexdigest()] for name, body in sorted(files.items())],
                ensure_ascii=False,
                separators=(",", ":"),
            ).encode()
        ).hexdigest()
        with self._account(environment_id) as repo:
            existing = self._metadata(repo, project_id)
            if (
                existing
                and existing[-1]["content_digest"] == digest
                and existing[-1]["excluded"] == excluded
                and existing[-1]["created_at"] >= time.time() - limits.checkpoint_retention_seconds
            ):
                return {**existing[-1], "unchanged": True}
            if sum(self._protected(row) for row in existing) >= limits.checkpoint_count:
                raise ServiceError("sandbox_limit")
            physical = self._size(repo)
            if physical >= limits.checkpoint_bytes * 0.85:
                raise ServiceError("sandbox_limit")
            if physical + total + 65536 > limits.checkpoint_bytes:
                raise ServiceError("sandbox_limit")
            checkpoint_id = str(uuid.uuid4())
            manifest = []
            tree = bytearray()
            for index, (name, body) in enumerate(sorted(files.items())):
                oid = self._oid(repo, ["hash-object", "-w", "--stdin"], body)
                tree.extend(f"100644 blob {oid}\tdata-{index:04d}".encode() + b"\0")
                manifest.append(
                    {
                        "path": name,
                        "object": oid,
                        "sha256": hashlib.sha256(body).hexdigest(),
                        "size": len(body),
                    }
                )
            metadata = {
                "checkpoint_id": checkpoint_id,
                "project_id": project_id,
                "content_digest": digest,
                "created_at": time.time(),
                "files": manifest,
                "excluded": excluded,
                "total_bytes": total,
            }
            payload = json.dumps(metadata, ensure_ascii=False, allow_nan=False).encode()
            if len(payload) > 1024**2:
                raise ServiceError("artifact_limit")
            meta_oid = self._oid(repo, ["hash-object", "-w", "--stdin"], payload)
            tree.extend(f"100644 blob {meta_oid}\tmanifest.json".encode() + b"\0")
            tree_oid = self._oid(repo, ["mktree", "-z"], bytes(tree))
            commit = self._oid(repo, ["commit-tree", tree_oid, "-m", "Project checkpoint"])
            ref = f"refs/krim/{project_id}/{checkpoint_id}"
            self._git(repo, ["update-ref", ref, commit, "0" * 40])
            if self._size(repo) > limits.checkpoint_bytes:
                self._git(repo, ["update-ref", "-d", ref, commit])
                self._reclaim(repo)
                raise ServiceError("sandbox_limit")
            # 新引用成功后才清理旧项；准备失败不能删除最后可用的回滚点。
            self._prune(repo, project_id, limits)
            return {**metadata, "ref": ref, "commit": commit, "unchanged": False}

    def _get(self, repo: Path, project_id: str, checkpoint_id: str) -> dict[str, Any]:
        internal_id(checkpoint_id)
        row = next(
            (
                row
                for row in self._metadata(repo, project_id)
                if row["checkpoint_id"] == checkpoint_id
            ),
            None,
        )
        if row is None:
            raise ServiceError("not_found", 404)
        return row

    def reference(
        self,
        environment_id: str,
        project_id: str,
        checkpoint_id: str,
        owner_kind: str,
        owner_id: str,
    ) -> None:
        """为持有者建立独立保留引用；同一持有者重复引用幂等。"""
        internal_id(project_id)
        kind, owner = _owner(owner_kind, owner_id)
        with self._account(environment_id) as repo:
            row = self._get(repo, project_id, checkpoint_id)
            ref = self._hold_ref(project_id, checkpoint_id, kind, owner)
            self._git(repo, ["update-ref", ref, row["commit"]])

    def release_reference(
        self,
        environment_id: str,
        project_id: str,
        checkpoint_id: str,
        owner_kind: str,
        owner_id: str,
    ) -> None:
        """只释放自己的引用；引用缺失（含检查点已被清理）视为已完成。"""
        internal_id(project_id)
        kind, owner = _owner(owner_kind, owner_id)
        with self._account(environment_id) as repo:
            ref = self._hold_ref(project_id, checkpoint_id, kind, owner)
            if self._git(repo, ["for-each-ref", "--format=%(refname)", ref]).strip():
                self._git(repo, ["update-ref", "-d", ref])

    def pin(
        self, environment_id: str, project_id: str, checkpoint_id: str, *, pinned: bool
    ) -> None:
        """用户固定走 user 引用；pin(False) 只删用户引用，legacy refs/retained 不动。"""
        internal_id(project_id)
        if type(pinned) is not bool:
            raise ServiceError("invalid_input", 422)
        with self._account(environment_id) as repo:
            row = self._get(repo, project_id, checkpoint_id)
            ref = self._hold_ref(project_id, checkpoint_id, "user", USER_OWNER)
            if pinned:
                self._git(repo, ["update-ref", ref, row["commit"]])
            elif self._git(repo, ["for-each-ref", "--format=%(refname)", ref]).strip():
                self._git(repo, ["update-ref", "-d", ref])

    def preview_restore(
        self, environment_id: str, project_id: str, checkpoint_id: str, current: dict[str, bytes]
    ) -> dict[str, Any]:
        """只生成预览与当前hash前置条件，不执行多文件覆盖。

        当前新增而检查点没有的文件默认保留：action=keep 并单列 preserved，
        不构成删除授权；主会话恢复计划自行确认差异后才可删除。
        """
        internal_id(project_id)
        with self._account(environment_id) as repo:
            row = self._get(repo, project_id, checkpoint_id)
            target = {item["path"]: item for item in row["files"]}
            changes = []
            preserved = []
            for name in sorted(set(target) | set(current)):
                _parts(name)
                before = hashlib.sha256(current[name]).hexdigest() if name in current else None
                after = target[name]["sha256"] if name in target else None
                if before == after:
                    continue
                if after is None:
                    preserved.append(name)
                    changes.append(
                        {
                            "path": name,
                            "expected_sha256": before,
                            "target_sha256": None,
                            "action": "keep",
                        }
                    )
                else:
                    changes.append(
                        {
                            "path": name,
                            "expected_sha256": before,
                            "target_sha256": after,
                            "action": "write",
                        }
                    )
            return {
                "checkpoint_id": checkpoint_id,
                "changes": changes,
                "preserved": preserved,
                "excluded": row["excluded"],
                "requires_confirmation": True,
            }

    def content(self, environment_id: str, project_id: str, checkpoint_id: str, path: str) -> bytes:
        internal_id(project_id)
        _parts(path)
        with self._account(environment_id) as repo:
            row = self._get(repo, project_id, checkpoint_id)
            item = next((entry for entry in row["files"] if entry["path"] == path), None)
            if item is None:
                raise ServiceError("not_found", 404)
            oid = item["object"]
            if not isinstance(oid, str) or not OID.fullmatch(oid):
                raise ServiceError("facts_corrupt")
            size = int(self._git(repo, ["cat-file", "-s", oid]))
            if size > MAX_FILE or size != item["size"]:
                raise ServiceError("artifact_limit")
            data = self._git(repo, ["cat-file", "blob", oid])
            if hashlib.sha256(data).hexdigest() != item["sha256"]:
                raise ServiceError("facts_corrupt")
            return data

    # ---- GC：计划落盘与两阶段物理回收 ----

    def _records(self, environment_id: str) -> Path:
        """受管 repo 之外的维护记录区（同账号私有目录内）。"""
        return self.root / environment_id / "maintenance" / "gc"

    @staticmethod
    def _encode(record: dict[str, Any]) -> bytes:
        return json.dumps(
            record, ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False
        ).encode()

    def _write_record(self, path: Path, record: dict[str, Any]) -> None:
        payload = self._encode(record)
        if len(payload) > PLAN_MAX_BYTES:
            raise ServiceError("artifact_limit")
        path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
        private_directory(path.parent)
        temporary = path.parent / f".{path.name}.{uuid.uuid4().hex}.tmp"
        descriptor = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
        try:
            view = memoryview(payload)
            while view:
                view = view[os.write(descriptor, view) :]
            os.fsync(descriptor)
        finally:
            os.close(descriptor)
        os.replace(temporary, path)
        folder = os.open(path.parent, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
        try:
            os.fsync(folder)
        finally:
            os.close(folder)

    def _read_record(self, path: Path) -> dict[str, Any]:
        try:
            descriptor = os.open(path, os.O_RDONLY | os.O_NOFOLLOW)
        except FileNotFoundError:
            raise ServiceError("not_found", 404) from None
        try:
            info = os.fstat(descriptor)
            if (
                not stat.S_ISREG(info.st_mode)
                or info.st_nlink != 1
                or info.st_size > PLAN_MAX_BYTES
            ):
                raise ServiceError("artifact_invalid")
            data = os.read(descriptor, info.st_size + 1)
        finally:
            os.close(descriptor)
        if len(data) > PLAN_MAX_BYTES:
            raise ServiceError("artifact_limit")
        try:
            record = json.loads(data)
        except ValueError:
            raise ServiceError("facts_corrupt") from None
        if type(record) is not dict:
            raise ServiceError("facts_corrupt")
        return record

    def _candidates(
        self, repo: Path, project_id: str, limits: QuotaLimits
    ) -> tuple[
        builtins.list[dict[str, str]],
        builtins.list[str],
        int,
        builtins.list[dict[str, Any]],
    ]:
        rows = self._metadata(repo, project_id)
        cutoff = time.time() - limits.checkpoint_retention_seconds
        candidates: list[dict[str, str]] = []
        chosen: set[str] = set()
        logical = 0
        state = [
            {
                "checkpoint_id": row["checkpoint_id"],
                "commit": row["commit"],
                "pinned": row["pinned"],
                "references": row["references"],
            }
            for row in rows
        ]
        for row in rows:
            if row["created_at"] < cutoff and not self._protected(row):
                candidates.append(
                    {
                        "checkpoint_id": row["checkpoint_id"],
                        "commit": row["commit"],
                        "reason": "expired",
                    }
                )
                chosen.add(row["checkpoint_id"])
                logical += int(row["total_bytes"])
        excess = max(0, len(rows) - len(chosen) - limits.checkpoint_count)
        pool = [
            row for row in rows if not self._protected(row) and row["checkpoint_id"] not in chosen
        ]
        for row in pool[:excess]:
            candidates.append(
                {
                    "checkpoint_id": row["checkpoint_id"],
                    "commit": row["commit"],
                    "reason": "count_excess",
                }
            )
            logical += int(row["total_bytes"])
        if limits.checkpoint_bytes and self._size(repo) >= limits.checkpoint_bytes * 0.70:
            chosen = {entry["checkpoint_id"] for entry in candidates}
            # 最新有效回滚点不因水位预先删除，已受保护内容也不动。
            for row in rows[:-1]:
                if row["checkpoint_id"] in chosen or self._protected(row):
                    continue
                candidates.append(
                    {
                        "checkpoint_id": row["checkpoint_id"],
                        "commit": row["commit"],
                        "reason": "space_watermark",
                    }
                )
                logical += int(row["total_bytes"])
                if logical >= self._size(repo) - limits.checkpoint_bytes * 0.60:
                    break
        protected = [row["checkpoint_id"] for row in rows if self._protected(row)]
        return candidates, protected, logical, state

    def cleanup_preview(
        self, environment_id: str, project_id: str, limits: QuotaLimits
    ) -> dict[str, Any]:
        """生成有期限的GC计划并落盘维护记录；只列候选与保护根，不删除引用。"""
        internal_id(project_id)
        if type(limits) is not QuotaLimits:
            raise ServiceError("invalid_input", 422)
        with self._account(environment_id) as repo:
            candidates, protected, logical, state = self._candidates(repo, project_id, limits)
            if len(candidates) > PLAN_MAX_CANDIDATES:
                raise ServiceError("artifact_limit")
            created = time.time()
            plan_id = str(uuid.uuid4())
            core = {
                "version": 1,
                "plan_id": plan_id,
                "project_id": project_id,
                "created_at": created,
                "expires_at": created + PLAN_TTL_SECONDS,
                "candidates": candidates,
                "protected": protected,
                "state": state,
            }
            stored = {
                **core,
                "plan_digest": hashlib.sha256(self._encode(core)).hexdigest(),
                "logical_bytes": logical,
            }
            self._write_record(
                self._records(environment_id) / "plans" / project_id / (plan_id + ".json"),
                stored,
            )
            return {**stored}

    @staticmethod
    def _request_digest(request_key: str) -> str:
        return hashlib.sha256(request_key.encode()).hexdigest()

    def _confirm_plan(
        self,
        repo: Path,
        records: Path,
        project_id: str,
        plan_id: str,
        plan_digest: str,
        *,
        accepted: bool = False,
    ) -> dict[str, Any]:
        plan_path = records / "plans" / project_id / (plan_id + ".json")
        plan = self._read_record(plan_path)
        if plan.get("project_id") != project_id:
            raise ServiceError("facts_corrupt")
        stored_digest = plan.get("plan_digest")
        if type(stored_digest) is not str or stored_digest != plan_digest:
            raise ServiceError("artifact_conflict")
        core = {k: v for k, v in plan.items() if k not in ("plan_digest", "logical_bytes")}
        if hashlib.sha256(self._encode(core)).hexdigest() != stored_digest:
            raise ServiceError("facts_corrupt")
        if not accepted and time.time() > plan.get("expires_at", 0.0):
            raise ServiceError("approval_expired")
        candidates = plan.get("candidates")
        protected = plan.get("protected")
        if type(candidates) is not list or type(protected) is not list:
            raise ServiceError("facts_corrupt")
        if len(candidates) > PLAN_MAX_CANDIDATES:
            raise ServiceError("artifact_limit")
        for item in candidates:
            checkpoint = item.get("checkpoint_id") if type(item) is dict else None
            if (
                type(item) is not dict
                or item.get("reason") not in CANDIDATE_REASONS
                or type(checkpoint) is not str
                or type(item.get("commit")) is not str
                or not OID.fullmatch(item["commit"])
            ):
                raise ServiceError("facts_corrupt")
            try:
                internal_id(checkpoint)
            except ServiceError:
                raise ServiceError("facts_corrupt") from None
        for checkpoint in protected:
            try:
                internal_id(checkpoint)
            except (ServiceError, TypeError):
                raise ServiceError("facts_corrupt") from None
        state = plan.get("state")
        if type(state) is not list or len(state) > PLAN_MAX_CANDIDATES:
            raise ServiceError("facts_corrupt")
        for entry in state:
            if (
                type(entry) is not dict
                or type(entry.get("checkpoint_id")) is not str
                or type(entry.get("commit")) is not str
                or not OID.fullmatch(entry["commit"])
                or type(entry.get("pinned")) is not bool
                or type(entry.get("references")) is not list
            ):
                raise ServiceError("facts_corrupt")
        return plan

    def cleanup_confirm(
        self,
        environment_id: str,
        project_id: str,
        plan_id: str,
        plan_digest: str,
        request_key: str,
    ) -> dict[str, Any]:
        """持维护锁重校验后执行GC；同 request_key 幂等，失败可原键重试。"""
        internal_id(project_id)
        internal_id(plan_id)
        if type(plan_digest) is not str or not PLAN_DIGEST.fullmatch(plan_digest):
            raise ServiceError("invalid_input", 422)
        if (
            type(request_key) is not str
            or not 1 <= len(request_key) <= 128
            or any(not 33 <= ord(char) <= 126 for char in request_key)
        ):
            raise ServiceError("invalid_input", 422)
        with self._account(environment_id) as repo:
            records = self._records(environment_id)
            receipt_path = (
                records / "receipts" / project_id / (self._request_digest(request_key) + ".json")
            )
            if receipt_path.exists():
                receipt = self._read_record(receipt_path)
                if receipt.get("plan_id") != plan_id or receipt.get("plan_digest") != plan_digest:
                    raise ServiceError("key_conflict")
                if receipt.get("status") == "completed":
                    return receipt
                # reclaim_pending：同键继续，不再重删引用。
            intent_path = records / "intents" / project_id / (plan_id + ".json")
            intent_pre_existing = intent_path.exists()
            accepted = False
            if intent_pre_existing:
                original = self._read_record(intent_path)
                accepted = original.get("plan_digest") == plan_digest and original.get(
                    "request_key_sha256"
                ) == self._request_digest(request_key)
            plan = self._confirm_plan(
                repo, records, project_id, plan_id, plan_digest, accepted=accepted
            )

            # 持锁重校验：候选 ref/OID 与保护根任何变化即冲突，零删除。
            rows = self._metadata(repo, project_id)
            current_state = {
                row["checkpoint_id"]: {
                    "checkpoint_id": row["checkpoint_id"],
                    "commit": row["commit"],
                    "pinned": row["pinned"],
                    "references": row["references"],
                }
                for row in rows
            }
            plan_state = {
                entry["checkpoint_id"]: entry
                for entry in plan["state"]
                if type(entry) is dict and type(entry.get("checkpoint_id")) is str
            }
            for checkpoint, entry in current_state.items():
                if plan_state.get(checkpoint) != entry:
                    raise ServiceError("revision_conflict")
            allowed = {item["checkpoint_id"] for item in plan["candidates"]}
            gone = [cid for cid in plan_state if cid not in current_state]
            # 已消失的只能是本次计划的候选（上次尝试已删）；其余任何变化即冲突。
            if (gone and not intent_pre_existing) or any(cid not in allowed for cid in gone):
                raise ServiceError("revision_conflict")
            live: dict[str, str] = {}
            for line in (
                self._git(
                    repo,
                    [
                        "for-each-ref",
                        "--format=%(refname) %(objectname)",
                        "refs/krim/" + project_id + "/",
                    ],
                )
                .decode()
                .splitlines()
            ):
                ref, oid = line.split()
                live[ref] = oid
            physical_before = self._size(repo)
            transaction = bytearray(b"start\n")
            for item in plan["candidates"]:
                ref = f"refs/krim/{project_id}/{item['checkpoint_id']}"
                current = live.get(ref)
                if current is None:
                    if not intent_pre_existing:
                        raise ServiceError("revision_conflict")
                    continue  # 上次尝试已删；按 ref 状态核实，不重删新引用。
                if current != item["commit"]:
                    raise ServiceError("revision_conflict")
                transaction.extend(f"delete {ref} {item['commit']}\n".encode())
            # 校验后每个候选要么本次删除、要么已确认缺失（上次尝试已删）。
            removed = [item["checkpoint_id"] for item in plan["candidates"]]
            removed_refs = [
                f"refs/krim/{project_id}/{item['checkpoint_id']}" for item in plan["candidates"]
            ]
            if len(transaction) > len(b"start\n") or not intent_pre_existing:
                # 事务前持久化意图；之后任何中断都能按 ref 状态续作且不重删。
                self._write_record(
                    intent_path,
                    {
                        "plan_id": plan_id,
                        "plan_digest": plan_digest,
                        "request_key_sha256": self._request_digest(request_key),
                        "candidates": plan["candidates"],
                        "created_at": time.time(),
                    },
                )
            if len(transaction) > len(b"start\n"):
                transaction.extend(b"commit\n")
                self._git(repo, ["update-ref", "--stdin"], bytes(transaction))
            removed = [ref.rsplit("/", 1)[-1] for ref in removed_refs]
            try:
                self._reclaim(repo)
            except ServiceError:
                self._write_record(
                    receipt_path,
                    {
                        "status": "reclaim_pending",
                        "plan_id": plan_id,
                        "plan_digest": plan_digest,
                        "request_key_sha256": self._request_digest(request_key),
                        "removed": removed,
                        "references_removed": removed_refs,
                        "physical_bytes_before": physical_before,
                        "logical_bytes": plan.get("logical_bytes", 0),
                    },
                )
                raise
            physical_after = self._size(repo)
            result = {
                "status": "completed",
                "plan_id": plan_id,
                "plan_digest": plan_digest,
                "request_key_sha256": self._request_digest(request_key),
                "removed": removed,
                "references_removed": removed_refs,
                "physical_bytes_before": physical_before,
                "physical_bytes_after": physical_after,
                "physical_reclaim_bytes": max(0, physical_before - physical_after),
                "logical_bytes": plan.get("logical_bytes", 0),
                "completed_at": time.time(),
            }
            self._write_record(receipt_path, result)
            with contextlib.suppress(FileNotFoundError):
                os.remove(intent_path)
            return result

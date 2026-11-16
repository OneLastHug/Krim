"""Secret delivery through the production SQLite proof projection."""

from dataclasses import replace

import pytest
from sqlalchemy import delete, update

from krim_agent.contracts import ServiceError
from krim_agent.development.contracts import OperationReceipt
from krim_agent.project_access.service import ProjectAccessService
from krim_agent.storage.development_models import (
    development_environments,
    development_operations,
    development_processes,
)
from krim_agent.storage.models import runs, turns
from tests.coding_agent.test_project_secret_authorization import prepare_command


def use_production_repository(fixture):
    return ProjectAccessService(
        fixture.access_repository, fixture.service._vault,
        secret_sink=fixture.service._secret_sink,
        git_sink=fixture.service._git_sink,
        command_sink=fixture.sink,
    )


async def test_unapproved_command_delivery_and_completed_launch_use_current_sqlite(tmp_path):
    fixture = await prepare_command(tmp_path / "actual.sqlite")
    service = use_production_repository(fixture)
    try:
        assert fixture.request.authorization.approval_id is None
        await service.deliver_command_secrets(fixture.binding)
        await service.release_command_secrets(fixture.binding)
        assert fixture.sink.accepted == [(b"fixture-secret",)]
        assert fixture.sink.released == [fixture.binding]
        await fixture.development.operations.save_receipt(OperationReceipt(
            identity=fixture.identity, state="completed", result_ref=fixture.identity.call_id,
        ))
        await service.validate_command_secrets(fixture.binding)
        with pytest.raises(ServiceError, match="permission_denied"):
            await service.deliver_command_secrets(fixture.binding)
    finally:
        await fixture.store.close()


@pytest.mark.parametrize("mutation", [
    "missing_process", "missing_operation", "operation_digest", "process_digest",
    "operation_run", "environment_generation", "environment_index", "turn_owner",
    "run_stopped", "unknown", "completed_wrong_result",
])
async def test_current_sqlite_rejects_missing_or_changed_command_proof(tmp_path, mutation):
    fixture = await prepare_command(tmp_path / (mutation + ".sqlite"))
    service = use_production_repository(fixture)
    try:
        async with fixture.store.engine.begin() as conn:
            if mutation == "missing_process":
                await conn.execute(delete(development_processes))
            elif mutation == "missing_operation":
                await conn.execute(delete(development_processes))
                await conn.execute(delete(development_operations))
            elif mutation == "operation_digest":
                await conn.execute(update(development_operations).values(command_digest="0" * 64))
            elif mutation == "process_digest":
                await conn.execute(update(development_processes).values(request_digest="0" * 64))
            elif mutation == "operation_run":
                raw = fixture.identity.model_copy(update={
                    "ownership": fixture.identity.ownership.model_copy(update={"run_id": "other"}),
                }).model_dump(mode="json")
                await conn.execute(update(development_operations).values(identity=raw))
            elif mutation == "environment_generation":
                await conn.execute(update(development_environments).values(generation=999))
            elif mutation == "environment_index":
                await conn.execute(update(development_environments).values(revision=999))
            elif mutation == "turn_owner":
                await conn.execute(update(turns).values(owner_id="other"))
            elif mutation == "run_stopped":
                await conn.execute(update(runs).values(stop_requested=True))
            else:
                receipt = OperationReceipt(
                    identity=fixture.identity,
                    state="unknown" if mutation == "unknown" else "completed",
                    result_ref=None if mutation == "unknown" else "wrong-call",
                )
                await conn.execute(update(development_operations).values(
                    receipt=receipt.model_dump(mode="json"),
                ))
        with pytest.raises(ServiceError, match="permission_denied"):
            await service.deliver_command_secrets(fixture.binding)
        assert fixture.sink.accepted == []
    finally:
        await fixture.store.close()


async def test_sqlite_proof_is_not_a_bearer_for_a_changed_execution(tmp_path):
    fixture = await prepare_command(tmp_path / "binding.sqlite")
    service = use_production_repository(fixture)
    try:
        altered = replace(fixture.binding, execution_id="f" * 64)
        with pytest.raises(ServiceError, match="permission_denied"):
            await service.deliver_command_secrets(altered)
        assert fixture.sink.accepted == []
    finally:
        await fixture.store.close()

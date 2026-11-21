"""Storage diagnostics identify code paths without disclosing persisted values."""

import logging

import pytest

from krim_agent.contracts import ServiceError
from krim_agent.storage.sqlite import SQLiteStore


async def test_write_failure_logs_types_without_sql_or_parameter_values(tmp_path, caplog):
    store = await SQLiteStore.open(tmp_path / "private-errors.sqlite")
    marker = "private-parameter-never-log"

    async def fail(_connection):
        try:
            raise ValueError(marker)
        except ValueError as error:
            raise RuntimeError("INSERT INTO private_values: " + marker) from error

    try:
        with caplog.at_level(logging.ERROR, logger="krim_agent.storage.sqlite"):
            with pytest.raises(ServiceError, match="storage_unavailable"):
                await store._write(fail)
        assert store.healthy is False
        assert "RuntimeError" in caplog.text and "ValueError" in caplog.text
        assert "frames=" in caplog.text
        assert marker not in caplog.text
        assert "INSERT INTO" not in caplog.text
    finally:
        await store.close()


async def test_domain_rejection_does_not_poison_storage_or_log_payload(tmp_path, caplog):
    store = await SQLiteStore.open(tmp_path / "domain-rejection.sqlite")

    async def reject(_connection):
        raise ServiceError("invalid_input")

    try:
        with caplog.at_level(logging.ERROR, logger="krim_agent.storage.sqlite"):
            with pytest.raises(ServiceError, match="invalid_input"):
                await store._write(reject)
        assert store.healthy is True
        assert "Storage write failed" not in caplog.text
    finally:
        await store.close()


async def test_unconfirmed_write_logs_sanitized_cause_and_remains_fail_closed(tmp_path, caplog):
    store = await SQLiteStore.open(tmp_path / "unconfirmed.sqlite")
    marker = "private-confirmation-value"

    async def fail(_connection):
        try:
            raise ValueError(marker)
        except ValueError:
            raise ServiceError("storage_unavailable") from None

    async def unconfirmed():
        return None

    try:
        with caplog.at_level(logging.ERROR, logger="krim_agent.storage.sqlite"):
            with pytest.raises(ServiceError, match="storage_unavailable"):
                await store._eval_write(fail, unconfirmed)
        assert store.healthy is False
        assert "ServiceError" in caplog.text and "ValueError" in caplog.text
        assert marker not in caplog.text
    finally:
        await store.close()

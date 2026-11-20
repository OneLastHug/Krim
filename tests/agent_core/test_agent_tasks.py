"""Historical evidence preservation and explicit workspace-target retirement."""

import json
from pathlib import Path

import pytest

from krim_agent.composition import open_application
from krim_agent.contracts import ServiceError
from krim_agent.security import LOCAL
from krim_agent.sessions.types import SessionCreate


@pytest.fixture(scope="module", autouse=True)
def preserve_final_failure_databases() -> None:
    """Only inspect byte-for-byte copies of archived failures; never recover originals."""
    import hashlib
    import os
    import sqlite3
    import stat
    from contextlib import closing

    # Ordinary retirement checks do not consume an external D4 archive. When
    # archival qualification is explicitly configured, retain its original checks.
    if "KRIM_D4_ROOT" not in os.environ:
        return
    root = Path(os.environ["KRIM_D4_ROOT"])
    index = root / "evidence/d4-final-failure-original-copies/index.json"
    if not index.exists():
        return
    records = json.loads(index.read_bytes())
    out = Path(os.environ["KRIM_D4_EVIDENCE"]) / "prior-final-failures"
    out.mkdir()
    completed = []
    for position, record in enumerate(records):
        source = Path(record["shadow_database"])
        assert stat.S_ISREG(source.lstat().st_mode)
        backup = out / (str(position) + ".sqlite")
        with closing(sqlite3.connect(source.as_uri() + "?mode=ro", uri=True)) as reader:
            with closing(sqlite3.connect(backup)) as writer, writer:
                reader.backup(writer)
        for file in record["files"]:
            original = Path(file["source"])
            assert stat.S_ISREG(original.lstat().st_mode)
            assert hashlib.sha256(original.read_bytes()).hexdigest() == file["sha256"]
        completed.append(
            record
            | {
                "readonly_backup": str(backup),
                "recover_calls": 0,
                "backup_sha256": hashlib.sha256(backup.read_bytes()).hexdigest(),
            }
        )
    (out / "index.json").write_text(json.dumps(completed, ensure_ascii=False, indent=2) + "\n")


@pytest.mark.asyncio
async def test_workspace_task_target_is_retired_without_creating_a_run(tmp_path: Path) -> None:
    async with await open_application(tmp_path / "retired-workspace.sqlite") as app:
        with pytest.raises(ServiceError, match="target_retired"):
            await app.sessions.create(
                LOCAL,
                "local",
                SessionCreate(request_key="retired-workspace", target_key="target-workspace"),
            )
        assert "target-workspace" not in app.service.bindings
        assert not app.service.tasks

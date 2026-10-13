"""真实隔离/Registry生成后首次封存落盘，供SIGKILL后的新进程只读恢复。"""

import asyncio
import json
import os
import sys
from pathlib import Path

import krim_agent
from krim_agent.config import Settings
from krim_agent.persistence import Database, KrimRepository
from krim_agent.persistence.completed_tree_seals import CompletedTreeSealStore
from krim_agent.providers import FakeProvider
from krim_agent.runtime import ChatRuntime
from krim_agent.tools.builtins import builtin_registry

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from test_completed_tree_origins import complete  # noqa: E402


async def main():
    root = Path(sys.argv[1])
    database = Database(f"sqlite+aiosqlite:///{root / 'state.sqlite'}")
    await database.initialize()
    runtime = ChatRuntime(
        KrimRepository(database), FakeProvider(delay=0), builtin_registry(), Settings()
    )
    service, agent, scope, trees, plan, binding, snapshot, outputs = await complete(
        runtime, root / "files"
    )
    try:
        sealed, created = await CompletedTreeSealStore(trees).seal(
            scope, binding, plan.plan_id, snapshot.fingerprint, snapshot
        )
        assert created
        print(
            json.dumps(
                {
                    "pid": os.getpid(),
                    "package": str(krim_agent.__file__),
                    "scope": scope,
                    "plan_id": plan.plan_id,
                    "resource_fingerprint": binding.fingerprint,
                    "fingerprint": snapshot.fingerprint,
                    "proof_hash": sealed.origin.view.proof_hash,
                    "inode": snapshot.entries[-1].file.inode,
                    "operation_ids": sealed.origin.operation_ids,
                }
            ),
            flush=True,
        )
        assert (await asyncio.to_thread(sys.stdin.readline)).strip() == "release"
    finally:
        await service.close()
        await runtime.close()
        await database.close()


asyncio.run(main())

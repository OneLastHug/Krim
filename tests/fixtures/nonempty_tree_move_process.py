"""只供可信合成测试的原材料落盘/实际rename后强杀/另进程零重放核实。"""

import json
import os
import pickle
import sys
from pathlib import Path

import krim_agent
import krim_agent.workspace_tree_writer as module
from krim_agent.workspace import Workspace
from krim_agent.workspace_tree_writer import WorkspaceTreeWriter


def main():
    phase, folder = sys.argv[1:3]
    root = Path(folder)
    state = root / "trusted-synthetic-move.pkl"
    with Workspace(root / "files", writable=True) as workspace:
        writer = WorkspaceTreeWriter(workspace)
        calls = 0
        if phase == "after-move":
            snapshot = writer.reader.capture("tree")
            change = writer.prepare("tree", "dest/tree", expected_fingerprint=snapshot.fingerprint)
            original = module.rename_at

            def intent(value):
                # 仅自己生成的合成对象；pickle不是产品协议，也不读取外来数据。
                state.write_bytes(pickle.dumps(value))
                state.chmod(0o600)

            def hold(*args, **kwargs):
                nonlocal calls
                calls += 1
                original(*args, **kwargs)
                assert (root / "files/dest/tree/sub/data.bin").read_bytes() == b"\0\xff"
                assert not (root / "files/tree").exists()
                print(
                    json.dumps(
                        {
                            "package": str(krim_agent.__file__),
                            "pid": os.getpid(),
                            "token": change.token,
                            "before": change.before.fingerprint,
                            "original_renames": calls,
                            "original_effect_observed": True,
                            "test_only_local_trusted_material": True,
                        }
                    ),
                    flush=True,
                )
                assert sys.stdin.readline().strip() == "release"

            module.rename_at = hold
            result = writer.apply(change, record_intent=intent)
            assert result.receipt.status == "done"
            return
        assert phase == "recover"
        # 该路径在测试新建私有目录中，由上面的唯一测试生产者写入。
        change = pickle.loads(state.read_bytes())

        def forbidden(*_args, **_kwargs):
            nonlocal calls
            calls += 1
            raise AssertionError("另进程核实禁止再次rename")

        module.rename_at = forbidden
        result = writer.reconcile(change)
        assert result.receipt.status == "done" and result.receipt.operation_token == change.token
        assert calls == 0 and writer._moved(change, result.observed_after)
        assert (root / "files/dest/tree/sub/data.bin").read_bytes() == b"\0\xff"
        assert not (root / "files/tree").exists()
        print(
            json.dumps(
                {
                    "package": str(krim_agent.__file__),
                    "pid": os.getpid(),
                    "token": result.receipt.operation_token,
                    "before": result.receipt.before_fingerprint,
                    "after": result.receipt.after_fingerprint,
                    "status": result.receipt.status,
                    "original_nodes": len(result.observed_after.entries),
                    "replay_renames": calls,
                    "test_only_local_trusted_material": True,
                    "product_persistent_store": False,
                }
            ),
            flush=True,
        )


main()

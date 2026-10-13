"""新物理交换真实effect后暂停；严格已有Codec测试材料供另一进程只读核实。"""

import base64
import json
import os
import sys
from pathlib import Path

import krim_agent
import krim_agent.workspace_tree_exchange as module
from krim_agent.persistence.tree_move_codec import decode_move, encode_move
from krim_agent.workspace import Workspace
from krim_agent.workspace_tree_exchange import PreparedTreeExchange, WorkspaceTreeExchangeWriter

phase, folder = sys.argv[1:3]
root = Path(folder)
with Workspace(root / "files", writable=True) as workspace:
    writer = WorkspaceTreeExchangeWriter(workspace)
    if phase == "produce":
        left = writer.trees.reader.capture("old")
        right = writer.trees.reader.capture("new")
        change = writer.prepare(
            "old", "new", expected_left=left.fingerprint, expected_right=right.fingerprint
        )
        records = []
        for move in writer._moves(change):
            metadata, blob = encode_move(move)
            records.append({"metadata": metadata, "blob": base64.b64encode(blob).decode()})
        # 此文件只属于合成测试，不宣称新的产品持久协议已实现。
        (root / "material.json").write_text(json.dumps({"version": 1, "records": records}))
        effect = writer.apply(change)
        assert effect.receipt.status == "done"
        print(
            json.dumps(
                {
                    "pid": os.getpid(),
                    "package": str(krim_agent.__file__),
                    "token": change.token,
                    "effect_before_ledger": True,
                    "left_inode": effect.observed_left.entries[0].directory.inode,
                    "right_inode": effect.observed_right.entries[0].directory.inode,
                }
            ),
            flush=True,
        )
        assert sys.stdin.readline().strip() == "release"
    else:
        assert phase == "recover"
        value = json.loads((root / "material.json").read_text())
        assert value["version"] == 1 and len(value["records"]) == 2
        moves = [
            decode_move(record["metadata"], base64.b64decode(record["blob"], validate=True))
            for record in value["records"]
        ]
        assert moves[0].token == moves[1].token
        assert (
            moves[0].target_path == moves[1].before.root_path
            and moves[1].target_path == moves[0].before.root_path
        )
        change = PreparedTreeExchange(moves[0].before, moves[1].before, moves[0].token)
        replays = 0

        def forbidden(*_args, **_kwargs):
            global replays
            replays += 1
            raise AssertionError("强杀恢复不得再次exchange/apply")

        module.rename_at = forbidden
        WorkspaceTreeExchangeWriter.apply = forbidden
        result = writer.reconcile(change)
        assert result.receipt.status == "done" and result.receipt.operation_token == change.token
        assert (root / "files/old/data.bin").read_bytes() == b"new\0\xff"
        assert (root / "files/new/data.bin").read_bytes() == b"old\xff\0"
        assert replays == 0
        print(
            json.dumps(
                {
                    "pid": os.getpid(),
                    "package": str(krim_agent.__file__),
                    "token": change.token,
                    "status": result.receipt.status,
                    "left_inode": result.observed_left.entries[0].directory.inode,
                    "right_inode": result.observed_right.entries[0].directory.inode,
                    "replay_calls": replays,
                }
            ),
            flush=True,
        )

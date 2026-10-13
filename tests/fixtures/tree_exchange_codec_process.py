"""严格产品Codec强杀材料：namespace后或完整after落盘后暂停，另进程只读核实。"""

import json
import os
import sys
from pathlib import Path

import krim_agent
import krim_agent.workspace_tree_exchange as physical
from krim_agent.persistence.tree_exchange_codec import (
    decode_exchange,
    decode_exchange_effect,
    encode_exchange,
    encode_exchange_effect,
)
from krim_agent.workspace import Workspace
from krim_agent.workspace_tree_exchange import WorkspaceTreeExchangeWriter

phase, folder, pause = sys.argv[1:4]
root = Path(folder)


def persist(stem, metadata, payload):
    for suffix, data in (("json", json.dumps(metadata).encode()), ("blob", payload)):
        with (root / f"{stem}.{suffix}").open("wb") as output:
            output.write(data)
            output.flush()
            os.fsync(output.fileno())


def announce(change):
    print(
        json.dumps(
            {
                "pid": os.getpid(),
                "package": str(krim_agent.__file__),
                "token": change.token,
                "pause": pause,
                "left_inode": (root / "files/old").stat().st_ino,
                "right_inode": (root / "files/new").stat().st_ino,
            }
        ),
        flush=True,
    )
    assert sys.stdin.readline().strip() == "release"


with Workspace(root / "files", writable=True) as workspace:
    writer = WorkspaceTreeExchangeWriter(workspace)
    if phase == "produce":
        left = writer.trees.reader.capture("old")
        right = writer.trees.reader.capture("new")
        change = writer.prepare(
            "old", "new", expected_left=left.fingerprint, expected_right=right.fingerprint
        )
        persist("original", *encode_exchange(change))
        if pause == "namespace":
            actual = physical.rename_at

            def after_namespace(*args, **kwargs):
                actual(*args, **kwargs)
                announce(change)

            physical.rename_at = after_namespace
        effect = writer.apply(change)
        assert effect.receipt.status == "done"
        persist("actual", *encode_exchange_effect(change, effect))
        if pause == "after":
            announce(change)
    else:
        assert phase == "recover"
        change = decode_exchange(
            json.loads((root / "original.json").read_text()), (root / "original.blob").read_bytes()
        )
        replays = 0

        def forbidden(*_args, **_kwargs):
            global replays
            replays += 1
            raise AssertionError("原交换核实和after冷读取不得重放exchange/apply")

        physical.rename_at = forbidden
        WorkspaceTreeExchangeWriter.apply = forbidden
        result = writer.reconcile(change)
        assert result.receipt.status == "done"
        if pause == "after":
            recorded = decode_exchange_effect(
                change,
                json.loads((root / "actual.json").read_text()),
                (root / "actual.blob").read_bytes(),
            )
            assert recorded == result
        else:
            assert not (root / "actual.json").exists()
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
                    "recorded_after": pause == "after",
                }
            ),
            flush=True,
        )

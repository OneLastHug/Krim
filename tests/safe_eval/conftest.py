"""仅本轮根的合成消费者与失败DB索引；收集阶段不要求产品已安装。"""

import hashlib
import json
import os
import sqlite3
import stat
import sys
from contextlib import closing
from pathlib import Path

import pytest

SOURCE = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(SOURCE / "src"))


@pytest.fixture(autouse=True)
def node_databases(request, tmp_path):
    yield
    root = Path(os.environ["KRIM_SAFE_EVAL_ROOT"])
    assert tmp_path.is_relative_to(root)
    destination = root / "evidence/current/node-databases"
    destination.mkdir(parents=True, exist_ok=True)
    tag = hashlib.sha256((request.node.nodeid + str(tmp_path)).encode()).hexdigest()
    records = []
    # 只遍历pytest自有普通fixture目录，不碰外部路径或负例目录。
    for path in tmp_path.glob("*.sqlite"):
        st = path.lstat()
        if not stat.S_ISREG(st.st_mode):
            records.append({"path": str(path), "type": stat.S_IFMT(st.st_mode)})
            continue
        backup = destination / f"{tag}-{path.name}"
        backup_fd_before = _backup_fds()
        with closing(sqlite3.connect(path.as_uri() + "?mode=ro", uri=True)) as source, source:
            with closing(sqlite3.connect(backup)) as target, target:
                source.backup(target)
        backup_fd_after = _backup_fds()
        records.append(
            {
                "path": str(path),
                "backup": str(backup),
                "sha256": hashlib.sha256(backup.read_bytes()).hexdigest(),
                "backup_fd_before": backup_fd_before,
                "backup_fd_after": backup_fd_after,
                "backup_fd_equal": backup_fd_after == backup_fd_before,
            }
        )
        assert backup_fd_after == backup_fd_before
    (destination / f"{tag}.json").write_text(
        json.dumps(
            {
                "node": request.node.nodeid,
                "tmp_path": str(tmp_path),
                "databases": records,
            },
            indent=2,
        )
    )


class ConsumerEvidence:
    """记录最终组合中每个实际node的fixture路径，不凭UUID猜数据库归属。"""

    def __init__(self, config):
        self.output = Path(str(config.option.basetemp)).parent / "consumers"
        self.output.mkdir(exist_ok=False)
        self.active = None
        self.records = {}
        self.pool_events = []
        from sqlalchemy import event
        from sqlalchemy.pool import Pool

        for kind in ["connect", "checkout", "reset", "checkin", "close"]:

            def record(*args, kind=kind):
                self.pool_events.append(
                    {
                        "node": self.active,
                        "event": kind,
                        "connection": id(args[0]),
                    }
                )

            event.listen(Pool, kind, record)

    @pytest.hookimpl(tryfirst=True)
    def pytest_runtest_setup(self, item):
        self.active = item.nodeid
        self.records[item.nodeid] = {
            "node": item.nodeid,
            "fd_before": len(os.listdir("/proc/self/fd")),
            "phases": [],
            "databases": [],
        }

    @pytest.hookimpl(hookwrapper=True)
    def pytest_runtest_makereport(self, item, call):
        outcome = yield
        report = outcome.get_result()
        record = self.records[item.nodeid]
        record["phases"].append({"when": report.when, "outcome": report.outcome})
        path = item.funcargs.get("tmp_path")
        if path is not None:
            record["tmp_path"] = str(path)
        if report.when == "teardown":
            record["fd_after"] = len(os.listdir("/proc/self/fd"))
            root = record.get("tmp_path")
            if root is not None:
                for database in Path(root).iterdir():
                    st = database.lstat()
                    if database.suffix != ".sqlite":
                        continue
                    if not stat.S_ISREG(st.st_mode) or st.st_mode & 0o444 == 0:
                        record["databases"].append(
                            {
                                "path": str(database),
                                "type": stat.S_IFMT(st.st_mode),
                            }
                        )
                        continue
                    tag = hashlib.sha256((item.nodeid + str(database)).encode()).hexdigest()
                    backup = self.output / f"{tag}.sqlite"
                    backup_fd_before = _backup_fds()
                    with (
                        closing(
                            sqlite3.connect(database.as_uri() + "?mode=ro", uri=True)
                        ) as reader,
                        reader,
                    ):
                        with closing(sqlite3.connect(backup)) as writer, writer:
                            reader.backup(writer)
                    backup_fd_after = _backup_fds()
                    record["databases"].append(
                        {
                            "path": str(database),
                            "backup": str(backup),
                            "sha256": hashlib.sha256(backup.read_bytes()).hexdigest(),
                            "backup_fd_before": backup_fd_before,
                            "backup_fd_after": backup_fd_after,
                            "backup_fd_equal": backup_fd_after == backup_fd_before,
                        }
                    )
                    assert backup_fd_after == backup_fd_before

    def pytest_sessionfinish(self, session, exitstatus):
        (self.output / "index.json").write_text(
            json.dumps(
                {
                    "nodes": self.records,
                    "pool_events": self.pool_events,
                    "exitstatus": exitstatus,
                },
                indent=2,
            )
        )


def pytest_configure(config):
    config.pluginmanager.register(ConsumerEvidence(config), "safe-eval-consumers")


def _backup_fds():
    """只观察本次备份前后的完整FD映射；listdir临时目录FD已在返回前关闭。"""
    result = {}
    for number in sorted(os.listdir("/proc/self/fd")):
        try:
            result[number] = os.readlink("/proc/self/fd/" + number)
        except FileNotFoundError:
            pass
    return result

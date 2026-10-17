"""仓库外-I安装验收：真实SDK/CLI/TCP所有权，不注入源码。"""

import asyncio
import hashlib
import json
import os
import signal
import socket
import sqlite3
import sys
from pathlib import Path

import httpx

import krim_agent
from krim_agent.composition import open_application
from krim_agent.evaluation.types import EvalRequest, FreezeRequest, rule
from krim_agent.sdk import EvaluationSDK, RunSDK

ROOT = Path(sys.argv[1])
ROOT.mkdir(parents=True, exist_ok=True)
ACCEPT = ROOT.parent.parent
SOURCE = ACCEPT / "source"
KEY = "SYNTHETIC_M2_INSTALLED_KEY"
assert sys.flags.isolated and "site-packages" in Path(krim_agent.__file__).parts
assert not any(
    str(p).startswith(str(SOURCE)) or str(p).startswith("/workspace/Krim-agent-core")
    for p in sys.path
)


def save(name, value):
    (ROOT / name).write_text(json.dumps(value, ensure_ascii=False, indent=2) + "\n")


def rows(path, sql, parameters=()):
    connection = sqlite3.connect(path.as_uri() + "?mode=ro", uri=True)
    try:
        return connection.execute(sql, parameters).fetchall()
    finally:
        connection.close()


def prepare_tcp():
    root = ROOT / "tcp"
    root.mkdir()
    return root


def events_at(root):
    path = root / "entries.jsonl"
    return [json.loads(line) for line in path.read_text().splitlines()] if path.exists() else []


def ref(expected="5"):
    return {
        "expected": expected,
        "expected_status": "completed",
        "expected_code": None,
        "expected_tools": None,
        "trajectory": None,
        "expected_schema": None,
    }


def case(case_id, text, expected="5"):
    return {
        "case_id": case_id,
        "source_kind": "manual",
        "input": text,
        "reference": ref(expected),
        "reason": "人工核算的安装验收参考",
    }


def req(revision, key, target="target-v2", repetitions=1):
    return EvalRequest.model_validate(
        {
            "request_key": key,
            "dataset": {k: getattr(revision, k) for k in ["name", "version", "fingerprint"]},
            "target_key": target,
            "rules": [rule(n).model_dump(mode="json") for n in ["status_code", "exact_match"]],
            "execution": {
                "repetitions": repetitions,
                "concurrency": 1,
                "item_timeout_seconds": 12,
                "scorer_timeout_seconds": 1,
                "batch_timeout_seconds": 600,
            },
        }
    )


async def cli(arguments, label, extra=None):
    env = dict(os.environ)
    env.pop("PYTHONPATH", None)
    env.pop("KRIM_GUIDE_SOURCE", None)
    if extra:
        env.update(extra)
    argv = [sys.executable, "-I", "-B", "-m", "krim_agent", *arguments]
    process = await asyncio.create_subprocess_exec(
        *argv, cwd=ROOT, env=env, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE
    )
    out, err = await process.communicate()
    (ROOT / (label + ".stdout")).write_bytes(out)
    (ROOT / (label + ".stderr")).write_bytes(err)
    save(
        label + "-command.json",
        {"argv": argv, "cwd": str(ROOT), "pid": process.pid, "exit": process.returncode},
    )
    return process.returncode, json.loads(out)


async def local_checks():
    database = ROOT / "sdk.sqlite"
    async with await open_application(database) as app:
        assert app.evaluations is not None
        sdk = EvaluationSDK(app.evaluations)
        runs = RunSDK(app.service)
        source = await runs.start("/add 2 3", "installed-source")
        await runs.wait(source.id)
        candidate = await sdk.candidate(source.id, True)
        chosen = case("integer", "/add 2 3")
        chosen.pop("input")
        chosen.update(
            source_kind="run",
            source_run_id=source.id,
            candidate_fingerprint=candidate.candidate_fingerprint,
            selected_input="/add 2 3",
        )
        revision = await sdk.freeze(
            FreezeRequest.model_validate(
                {
                    "name": "installed",
                    "version": "1",
                    "cases": [
                        chosen,
                        case("fraction_a", "/add 2.5 0.5", "3"),
                        case("fraction_b", "/add 1.5 1.5", "3"),
                    ],
                }
            )
        )
        reports = {}
        counts = {}
        for target in ["target-v1", "target-v2", "target-bad"]:
            request = req(revision, "installed-" + target, target, 2)
            before = app.service.bindings[target].loop.provider.calls
            view = await sdk.start(request)
            await sdk.wait(view.id)
            assert (await sdk.start(request)).id == view.id
            report = await sdk.report(view.id)
            reports[target] = report
            assert len({i["run_id"] for i in report["items"]}) == 6
            assert all(
                i["run_id"] != i["source_run_id"]
                and i["output"]["model_steps"] == 2
                and i["output"]["tools"][0]["executor_started"] is True
                for i in report["items"]
            )
            counts[target] = app.service.bindings[target].loop.provider.calls - before
            assert counts[target] == 12
            assert report["summary"]["token_usage"] is None
        gates = {
            "v2": await sdk.compare(
                reports["target-v1"]["eval_id"], reports["target-v2"]["eval_id"]
            ),
            "bad": await sdk.compare(
                reports["target-v1"]["eval_id"], reports["target-bad"]["eval_id"]
            ),
        }
        assert gates["v2"]["status"] == "pass" and gates["bad"]["status"] == "fail"
        slow = await sdk.freeze(
            FreezeRequest.model_validate(
                {"name": "slow", "version": "1", "cases": [case("one", "/slow")]}
            )
        )
        provider = app.service.bindings["target-v2"].loop.provider
        provider.entered.clear()
        view = await sdk.start(req(slow, "sdk-cancel"))
        await provider.entered.wait()
        assert (await sdk.cancel(view.id)).status == "aborted" and provider.active == 0
        save(
            "sdk.json",
            {
                "module": krim_agent.__file__,
                "executable": sys.executable,
                "cwd": str(Path.cwd()),
                "counts": counts,
                "reports": reports,
                "gates": gates,
                "cancelled": view.id,
            },
        )
    assert (
        app.store.closed
        and all(t.done() for t in app.service.tasks.values())
        and all(t.done() for t in app.evaluations.tasks.values())
    )
    assert all(
        b.loop.provider.closed and b.loop.provider.calls == b.loop.provider.streams_closed
        for b in app.service.bindings.values()
    )
    before = hashlib.sha256(database.read_bytes()).hexdigest()
    code, report = await cli(
        ["report", "--database", str(database), "--id", reports["target-v2"]["eval_id"]],
        "cold-report",
    )
    assert code == 0 and report["eval_id"] == reports["target-v2"]["eval_id"]
    code, gate = await cli(
        [
            "compare",
            "--database",
            str(database),
            "--baseline",
            reports["target-v1"]["eval_id"],
            "--candidate",
            reports["target-v2"]["eval_id"],
        ],
        "cold-gate",
    )
    assert (
        code == 0
        and gate["status"] == "pass"
        and hashlib.sha256(database.read_bytes()).hexdigest() == before
    )
    local = ROOT / "local-cli.sqlite"
    async with await open_application(local) as owner:
        revision = await owner.evaluations.freeze(
            FreezeRequest.model_validate(
                {"name": "local", "version": "1", "cases": [case("sum", "/add 2 3")]}
            )
        )
        slow_revision = await owner.evaluations.freeze(
            FreezeRequest.model_validate(
                {"name": "local-slow", "version": "1", "cases": [case("slow", "/slow")]}
            )
        )
    code, result = await cli(
        [
            "eval",
            "start",
            "--database",
            str(local),
            "--name",
            "local",
            "--revision",
            "1",
            "--fingerprint",
            revision.fingerprint,
            "--target",
            "target-v2",
            "--key",
            "local-cli",
        ],
        "local-start",
    )
    assert code == 0 and result["status"] == "completed" and result["complete"] is True
    before = hashlib.sha256(local.read_bytes()).hexdigest()
    for operation in ["cancel", "wait"]:
        code, error = await cli(
            ["eval", operation, "--database", str(local), "--id", result["eval_id"]],
            "local-" + operation,
        )
        assert code == 1 and error["error"]["code"] == "readonly"
    assert hashlib.sha256(local.read_bytes()).hexdigest() == before
    # 真正本机CLI进程持有writer；等model_start持久化再发可捕获中断。
    argv = [
        sys.executable,
        "-I",
        "-B",
        "-m",
        "krim_agent",
        "eval",
        "start",
        "--database",
        str(local),
        "--name",
        "local-slow",
        "--revision",
        "1",
        "--fingerprint",
        slow_revision.fingerprint,
        "--target",
        "target-v2",
        "--key",
        "local-interrupt",
    ]
    process = await asyncio.create_subprocess_exec(
        *argv, cwd=ROOT, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE
    )
    try:
        # 只观察本次预留child，已有正常批次不能误触发屏障。
        async with asyncio.timeout(5):
            while not rows(
                local,
                "SELECT r.id FROM runs r JOIN eval_items i ON i.run_id=r.id "
                "JOIN evaluations e ON e.id=i.eval_id JOIN run_events v ON v.run_id=r.id "
                "WHERE e.request_key='local-interrupt' AND v.kind='model_start'",
            ):
                assert process.returncode is None
                await asyncio.sleep(0.02)
        process.send_signal(signal.SIGINT)
        out, err = await asyncio.wait_for(process.communicate(), 8)
        (ROOT / "local-interrupt.stdout").write_bytes(out)
        (ROOT / "local-interrupt.stderr").write_bytes(err)
        assert process.returncode == 130
        assert rows(
            local, "SELECT status FROM evaluations WHERE request_key='local-interrupt'"
        ) == [("aborted",)]
        assert rows(
            local,
            "SELECT r.status FROM runs r JOIN eval_items i ON i.run_id=r.id "
            "JOIN evaluations e ON e.id=i.eval_id WHERE e.request_key='local-interrupt'",
        ) == [("aborted",)]
        # 新writer确实可准入，不能只凭进程退出推测锁已释放。
        async with await open_application(local) as reopened:
            assert await reopened.evaluations.by_key("local-interrupt")
        save(
            "local-interrupt-command.json",
            {
                "argv": argv,
                "cwd": str(ROOT),
                "pid": process.pid,
                "exit": process.returncode,
                "writer_reopened": True,
            },
        )
    finally:
        if process.returncode is None:
            process.kill()
            await process.wait()
    return {"sdk": True, "local_cli_completed": result["eval_id"], "cold_reads_zero_writes": True}


async def tcp_checks():
    root = prepare_tcp()
    env = dict(os.environ, KRIM_AGENT_API_KEY=KEY)
    env.pop("KRIM_GUIDE_SOURCE", None)
    env.pop("PYTHONPATH", None)
    argv = [
        sys.executable,
        "-I",
        "-B",
        str(SOURCE / "tests/core/fixtures/eval_ui_server.py"),
        "--database",
        str(root / "browser.sqlite"),
        "--port",
        "0",
        "--evidence",
        str(root),
    ]
    log = (root / "server.log").open("w")
    process = await asyncio.create_subprocess_exec(*argv, cwd=root, env=env, stdout=log, stderr=log)
    row = {"argv": argv, "cwd": str(root), "pid": process.pid}
    waiter = None
    try:
        async with httpx.AsyncClient(trust_env=False, timeout=3) as client:
            async with asyncio.timeout(12):
                while not (root / "server-starts.jsonl").exists():
                    assert process.returncode is None
                    await asyncio.sleep(0.02)
                start = json.loads((root / "server-starts.jsonl").read_text().splitlines()[-1])
                row.update(port=start["port"], module=start["module"])
                assert "installed-env" in start["module"] and "site-packages" in start["module"]
                base = "http://127.0.0.1:" + str(start["port"])
                while True:
                    try:
                        if (await client.get(base + "/healthz")).status_code == 200:
                            break
                    except httpx.HTTPError:
                        pass
                    await asyncio.sleep(0.02)
            headers = {"Authorization": "Bearer " + KEY}
            body = {"name": "remote", "version": "1", "cases": [case("slow", "/slow")]}
            before_revisions = rows(
                root / "browser.sqlite", "SELECT count(*) FROM dataset_revisions"
            )[0][0]
            denied = await client.post(
                base + "/v1/datasets", json=body, headers={"Authorization": "Bearer WRONG_KEY"}
            )
            assert denied.status_code == 401 and denied.json()["error"]["code"] == "unauthorized"
            assert before_revisions == 0 and rows(
                root / "browser.sqlite", "SELECT count(*) FROM dataset_revisions"
            ) == [(0,)]
            response = await client.post(base + "/v1/datasets", json=body, headers=headers)
            assert response.status_code == 200
            assert rows(root / "browser.sqlite", "SELECT count(*) FROM dataset_revisions") == [(1,)]
            save(
                "remote-freeze-auth.json",
                {
                    "denied": denied.status_code,
                    "before": 0,
                    "after_denied": 0,
                    "after_explicit_retry": 1,
                },
            )
            revision = response.json()
            fp = revision["fingerprint"]
            code, view = await cli(
                [
                    "eval",
                    "start",
                    "--url",
                    base,
                    "--name",
                    "remote",
                    "--revision",
                    "1",
                    "--fingerprint",
                    fp,
                    "--target",
                    "target-v2",
                    "--key",
                    "remote-cli",
                ],
                "remote-start",
                {"KRIM_AGENT_API_KEY": KEY},
            )
            assert code == 0
            async with asyncio.timeout(4):
                while True:
                    current = (
                        await client.get(base + "/v1/evals/" + view["id"], headers=headers)
                    ).json()
                    if current["status"] == "running" and current["revision"] >= 3:
                        break
                    await asyncio.sleep(0.02)
            # 关联提交与模型启动可能交错；观察真实流进入后才进入无写入等待窗口。
            async with asyncio.timeout(3):
                while True:
                    if any(
                        event["kind"] == "provider_entry" and event.get("target") == "target-v2"
                        for event in events_at(root)
                    ):
                        break
                    await asyncio.sleep(0.02)
            before = hashlib.sha256((root / "browser.sqlite").read_bytes()).hexdigest()
            code, error = await cli(
                ["eval", "cancel", "--database", str(root / "browser.sqlite"), "--id", view["id"]],
                "remote-writer-local-refusal",
            )
            assert code == 1 and error["error"]["code"] == "readonly"
            assert hashlib.sha256((root / "browser.sqlite").read_bytes()).hexdigest() == before
            waiter_argv = [
                sys.executable,
                "-I",
                "-B",
                "-m",
                "krim_agent",
                "eval",
                "wait",
                "--url",
                base,
                "--id",
                view["id"],
            ]

            def get_count():
                return sum(
                    e["kind"] == "http_request"
                    and e.get("method") == "GET"
                    and e.get("path") == "/v1/evals/" + view["id"]
                    for e in events_at(root)
                )

            baseline_gets = get_count()
            waiter = await asyncio.create_subprocess_exec(
                *waiter_argv,
                cwd=ROOT,
                env=env,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
            )
            # 实际GET记录作为读等待进入屏障，不凭固定sleep假设命令已经开始。
            async with asyncio.timeout(5):
                while True:
                    if waiter.returncode is not None:
                        raise AssertionError("wait command exited early")
                    if get_count() > baseline_gets:
                        break
                    await asyncio.sleep(0.02)
            waiter.send_signal(signal.SIGINT)
            out, err = await waiter.communicate()
            (ROOT / "remote-wait.stdout").write_bytes(out)
            (ROOT / "remote-wait.stderr").write_bytes(err)
            save(
                "remote-wait-command.json",
                {
                    "argv": waiter_argv,
                    "cwd": str(ROOT),
                    "pid": waiter.pid,
                    "exit": waiter.returncode,
                },
            )
            assert waiter.returncode == 130
            current = (await client.get(base + "/v1/evals/" + view["id"], headers=headers)).json()
            assert current["status"] == "running"
            code, cancelled = await cli(
                ["eval", "cancel", "--url", base, "--id", view["id"]],
                "remote-cancel",
                {"KRIM_AGENT_API_KEY": KEY},
            )
            assert code == 0 and cancelled["status"] == "aborted"
            row.update(
                eval_id=view["id"], remote_exit_kept_producer=True, wait_exit=waiter.returncode
            )
    finally:
        if waiter is not None and waiter.returncode is None:
            waiter.kill()
            await waiter.wait()
        if process.returncode is None:
            process.terminate()
        try:
            row["exit"] = await asyncio.wait_for(process.wait(), 10)
        except TimeoutError:
            process.kill()
            row["exit"] = await process.wait()
        log.close()
        if "port" in row:
            with socket.socket() as probe:
                probe.settimeout(0.5)
                row["port_closed"] = probe.connect_ex(("127.0.0.1", row["port"])) != 0
        stops = root / "server-stops.jsonl"
        if stops.exists():
            row["cleanup"] = json.loads(stops.read_text().splitlines()[-1])
        save("tcp-process.json", row)
        assert row["exit"] == 0 and row.get("port_closed")
        c = row["cleanup"]
        assert (
            c["tasks_done"] and c["eval_tasks_done"] and c["scorers_done"] and c["database_closed"]
        )
        assert all(
            p["active"] == 0 and p["closed"] and p["calls"] == p["streams_closed"]
            for p in c["providers"].values()
        )
    return row


async def main():
    local = await local_checks()
    tcp = await tcp_checks()
    save(
        "installed-eval.json",
        {
            "module": krim_agent.__file__,
            "python": sys.executable,
            "cwd": str(Path.cwd()),
            "local": local,
            "tcp": tcp,
        },
    )
    print(
        json.dumps(
            {
                "module": krim_agent.__file__,
                "local": local,
                "tcp_exit": tcp["exit"],
                "port_closed": tcp["port_closed"],
            }
        )
    )


if __name__ == "__main__":
    asyncio.run(main())

"""账号CLI契约：在线请求形状、离线初始化、密钥一次输出与预检卫生。"""

import argparse
import io
import json
import os
import sys
from types import ModuleType

import pytest

from krim_agent.account_cli import _membership, add_account_parser, execute_account
from krim_agent.contracts import ServiceError

POLICY = {
    "credentials": [
        {
            "principal_id": "local",
            "token": "old-global-secret",
            "generation": 2,
            "memberships": [{"workspace_id": "local", "role": "admin", "approve": True}],
            "legacy": True,
        },
        {
            "principal_id": "alice",
            "token": "alice-token",
            "memberships": [
                {"workspace_id": "ws-a", "role": "member", "approve": True},
                {"workspace_id": "shared", "role": "viewer", "approve": False},
            ],
        },
    ]
}


def parse(*argv: str) -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    sub = parser.add_subparsers(dest="command", required=True)
    add_account_parser(sub)
    return parser.parse_args(["account", *argv])


@pytest.fixture
def fake_client(monkeypatch):
    import krim_agent.account_cli as module

    calls: list[tuple[str, str, dict | None]] = []
    holder: dict[str, str] = {}

    class FakeClient:
        def __init__(self, base_url: str, api_key: str = "") -> None:
            holder["url"], holder["key"] = base_url, api_key

        async def request(self, method: str, path: str, body: dict | None = None) -> dict:
            calls.append((method, path, body))
            return {"done": method}

        async def close(self) -> None:
            holder["closed"] = "yes"

    monkeypatch.setattr(module, "AsyncRunClient", FakeClient)
    return calls, holder


@pytest.fixture
def stub_service(monkeypatch):
    calls: list[tuple] = []
    stores: list = []

    class StubIdentityService:
        def __init__(self, store: object) -> None:
            self.store = store

        @classmethod
        async def open(cls, store: object) -> "StubIdentityService":
            stores.append(store)
            return cls(store)

        async def close(self) -> None:
            calls.append(("close",))

        async def bootstrap(
            self,
            display_name: str,
            *,
            request_key: str,
            principal_id: str | None = None,
            claim_local: bool = False,
        ) -> dict:
            calls.append(("bootstrap", display_name, request_key, principal_id, claim_local))
            return {
                "account": {
                    "principal_id": principal_id or "p-new",
                    "display_name": display_name,
                    "status": "active",
                    "platform_role": "platform_admin",
                },
                "token": "issued-one-time-token",
                "replayed": False,
            }

        async def import_policy(
            self, policy: object, admin_id: str, *, request_key: str, display_names=None
        ) -> dict:
            calls.append((
                "import",
                admin_id,
                request_key,
                display_names,
                [
                    (
                        item.principal_id,
                        item.generation,
                        item.legacy,
                        [(m.workspace_id, m.role, m.approve) for m in item.memberships],
                        item.token.get_secret_value(),
                    )
                    for item in policy.credentials
                ],
            ))
            return {"imported": 2, "platform_admin": admin_id}

        async def recover(
            self, principal_id: str, *, request_key: str, actor: str = "host"
        ) -> dict:
            calls.append(("recover", principal_id, request_key, actor))
            return {
                "principal_id": principal_id,
                "token": "recovery-one-time-token",
                "replayed": False,
            }

    package = ModuleType("krim_agent.accounts")
    service = ModuleType("krim_agent.accounts.service")
    service.IdentityService = StubIdentityService
    package.service = service
    monkeypatch.setitem(sys.modules, "krim_agent.accounts", package)
    monkeypatch.setitem(sys.modules, "krim_agent.accounts.service", service)
    return calls, stores


def write_policy(tmp_path) -> str:
    path = tmp_path / "policy.json"
    path.write_text(json.dumps(POLICY))
    os.chmod(path, 0o600)
    return str(path)


def run_main(monkeypatch, capsys, *argv: str) -> tuple[int, str, str]:
    from krim_agent.cli import main

    monkeypatch.setattr(sys, "argv", ["krim_agent", *argv])
    code = 0
    try:
        main()
    except SystemExit as exit_status:
        code = exit_status.code
    captured = capsys.readouterr()
    return code, captured.out, captured.err


async def test_online_list_uses_api_and_env_key(fake_client, monkeypatch) -> None:
    calls, holder = fake_client
    monkeypatch.setenv("KRIM_AGENT_API_KEY", "env-key")
    result = await execute_account(
        parse(
            "list",
            "--url",
            "http://127.0.0.1:9",
            "--offset",
            "40",
            "--limit",
            "5",
            "--status",
            "active",
            "--query",
            "ali",
        )
    )
    assert result == {"done": "GET"}
    assert calls == [("GET", "/v2/admin/accounts?offset=40&limit=5&status=active&query=ali", None)]
    assert holder["url"] == "http://127.0.0.1:9"
    assert holder["key"] == "env-key" and holder["closed"] == "yes"


async def test_online_list_without_filters_drops_empty_query(fake_client, monkeypatch) -> None:
    calls, holder = fake_client
    monkeypatch.delenv("KRIM_AGENT_API_KEY", raising=False)
    await execute_account(parse("list", "--url", "http://x"))
    assert calls == [("GET", "/v2/admin/accounts?offset=0&limit=20", None)]
    assert holder["key"] == ""


async def test_online_reads(fake_client) -> None:
    calls, _ = fake_client
    await execute_account(parse("profile", "--url", "http://x"))
    await execute_account(parse("get", "--url", "http://x", "--id", "a b"))
    await execute_account(parse("events", "--url", "http://x", "--id", "alice", "--limit", "3"))
    await execute_account(parse("events", "--url", "http://x"))
    await execute_account(
        parse("events", "--url", "http://x", "--request-key", "unknown-1", "--limit", "3")
    )
    assert calls[0] == ("GET", "/v2/me/profile", None)
    assert calls[1] == ("GET", "/v2/admin/accounts/a%20b", None)
    assert calls[2] == (
        "GET",
        "/v2/admin/account-events?offset=0&limit=3&principal_id=alice",
        None,
    )
    assert calls[3] == ("GET", "/v2/admin/account-events?offset=0&limit=20", None)
    assert calls[4] == (
        "GET",
        "/v2/admin/account-events?offset=0&limit=3&request_key=unknown-1",
        None,
    )


async def test_online_create_sends_request_key_and_memberships(fake_client) -> None:
    calls, _ = fake_client
    result = await execute_account(
        parse(
            "create",
            "--url",
            "http://x",
            "--display-name",
            " 张三 ",
            "--membership",
            "ws-a:member",
            "--membership",
            "shared:viewer:no-approve",
            "--request-key",
            "req-1",
        )
    )
    assert result == {"done": "POST"}
    assert calls == [
        (
            "POST",
            "/v2/admin/accounts",
            {
                "display_name": "张三",
                "platform_role": "member",
                "memberships": [
                    {"workspace_id": "ws-a", "role": "member", "approve": True},
                    {"workspace_id": "shared", "role": "viewer", "approve": False},
                ],
                "request_key": "req-1",
            },
        )
    ]


async def test_online_update_sends_only_given_fields(fake_client) -> None:
    calls, _ = fake_client
    await execute_account(
        parse(
            "update",
            "--url",
            "http://x",
            "--id",
            "alice",
            "--display-name",
            "新名",
            "--revision",
            "3",
            "--request-key",
            "upd-1",
        )
    )
    assert calls == [
        (
            "PATCH",
            "/v2/admin/accounts/alice",
            {"request_key": "upd-1", "display_name": "新名", "expected_revision": 3},
        )
    ]


def test_update_requires_revision_flag() -> None:
    with pytest.raises(SystemExit):
        parse("update", "--url", "http://x", "--id", "alice", "--request-key", "upd-2")


def test_memberships_requires_generation_flag() -> None:
    with pytest.raises(SystemExit):
        parse(
            "memberships",
            "--url",
            "http://x",
            "--id",
            "alice",
            "--membership",
            "ws:member",
            "--request-key",
            "mem-1",
        )


async def test_online_key_operations_paths(fake_client) -> None:
    calls, _ = fake_client
    await execute_account(
        parse("issue", "--url", "http://x", "--id", "alice", "--request-key", "k1")
    )
    await execute_account(parse("rotate", "--url", "http://x", "--request-key", "k2"))
    await execute_account(
        parse("revoke", "--url", "http://x", "--id", "alice", "--request-key", "k3")
    )
    await execute_account(
        parse(
            "memberships",
            "--url",
            "http://x",
            "--id",
            "alice",
            "--membership",
            "ws:admin",
            "--generation",
            "4",
            "--request-key",
            "k4",
        )
    )
    assert calls == [
        ("POST", "/v2/admin/accounts/alice/key/issue", {"request_key": "k1"}),
        ("POST", "/v2/me/key/rotate", {"request_key": "k2"}),
        ("POST", "/v2/admin/accounts/alice/key/revoke", {"request_key": "k3"}),
        (
            "PUT",
            "/v2/admin/accounts/alice/memberships",
            {
                "memberships": [{"workspace_id": "ws", "role": "admin", "approve": True}],
                "request_key": "k4",
                "expected_generation": 4,
            },
        ),
    ]


async def test_online_rejects_bad_request_key(fake_client) -> None:
    with pytest.raises(ServiceError):
        await execute_account(
            parse("create", "--url", "http://x", "--display-name", "a", "--request-key", "bad key")
        )


async def test_online_rejects_control_chars_in_display_name(fake_client) -> None:
    with pytest.raises(ServiceError):
        await execute_account(
            parse(
                "create",
                "--url",
                "http://x",
                "--display-name",
                "a\nb",
                "--request-key",
                "req-2",
            )
        )


def test_online_operations_reject_database_flag() -> None:
    with pytest.raises(SystemExit):
        parse("list", "--url", "http://x", "--database", "db.sqlite")


def test_offline_operations_reject_url_flag() -> None:
    with pytest.raises(SystemExit):
        parse("bootstrap", "--database", "d", "--request-key", "k", "--url", "http://x")


def test_membership_parsing() -> None:
    assert _membership("ws:member") == {"workspace_id": "ws", "role": "member", "approve": True}
    assert _membership("ws:admin:approve")["approve"] is True
    assert _membership("ws:viewer:no-approve")["approve"] is False
    for raw in (":admin", "ws:root", "ws:admin:maybe", "ws"):
        with pytest.raises(ServiceError):
            _membership(raw)


async def test_offline_bootstrap_contract(tmp_path, stub_service) -> None:
    calls, stores = stub_service
    result = await execute_account(
        parse(
            "bootstrap",
            "--database",
            str(tmp_path / "db.sqlite"),
            "--display-name",
            "管理员",
            "--principal-id",
            "local",
            "--claim-local",
            "--request-key",
            "boot-1",
        )
    )
    assert result["token"] == "issued-one-time-token" and result["replayed"] is False
    assert calls == [("bootstrap", "管理员", "boot-1", "local", True), ("close",)]
    assert stores and stores[0].closed is True


async def test_offline_import_policy_contract(tmp_path, stub_service) -> None:
    calls, stores = stub_service
    result = await execute_account(
        parse(
            "import",
            "--database",
            str(tmp_path / "db.sqlite"),
            "--policy",
            write_policy(tmp_path),
            "--admin-id",
            "local",
            "--display-name",
            "alice=Alice 陈",
            "--request-key",
            "imp-1",
        )
    )
    assert result == {"imported": 2, "platform_admin": "local"}
    operation, admin, key, names, credentials = calls[0]
    assert (operation, admin, key, names) == ("import", "local", "imp-1", {"alice": "Alice 陈"})
    assert credentials == [
        ("local", 2, True, [("local", "admin", True)], "old-global-secret"),
        ("alice", 1, False, [("ws-a", "member", True), ("shared", "viewer", False)], "alice-token"),
    ]
    assert stores[0].closed is True


async def test_offline_import_admin_must_be_in_policy(tmp_path, stub_service) -> None:
    calls, _ = stub_service
    with pytest.raises(ServiceError):
        await execute_account(
            parse(
                "import",
                "--database",
                str(tmp_path / "db.sqlite"),
                "--policy",
                write_policy(tmp_path),
                "--admin-id",
                "ghost",
                "--request-key",
                "imp-2",
            )
        )
    assert calls == []


async def test_offline_import_global_key_env(tmp_path, stub_service, monkeypatch) -> None:
    calls, _ = stub_service
    monkeypatch.setenv("KRIM_AGENT_API_KEY", "legacy-global-secret")
    await execute_account(
        parse(
            "import",
            "--database",
            str(tmp_path / "db.sqlite"),
            "--global-key-env",
            "--admin-id",
            "local",
            "--request-key",
            "imp-3",
        )
    )
    assert calls[0][4] == [("local", 1, True, [("local", "admin", True)], "legacy-global-secret")]


async def test_offline_import_global_key_stdin(tmp_path, stub_service, monkeypatch) -> None:
    calls, _ = stub_service
    monkeypatch.setattr(sys, "stdin", io.StringIO("piped-legacy-secret\n"))
    await execute_account(
        parse(
            "import",
            "--database",
            str(tmp_path / "db.sqlite"),
            "--global-key-stdin",
            "--admin-id",
            "local",
            "--request-key",
            "imp-4",
        )
    )
    assert calls[0][4] == [("local", 1, True, [("local", "admin", True)], "piped-legacy-secret")]


async def test_offline_recover_contract(tmp_path, stub_service) -> None:
    calls, stores = stub_service
    result = await execute_account(
        parse(
            "recover",
            "--database",
            str(tmp_path / "db.sqlite"),
            "--id",
            "local",
            "--request-key",
            "rec-1",
        )
    )
    assert result["token"] == "recovery-one-time-token"
    assert calls == [("recover", "local", "rec-1", "host"), ("close",)]
    assert stores[0].closed is True


async def test_offline_dry_runs_never_touch_database_or_service(tmp_path, stub_service) -> None:
    calls, stores = stub_service
    absent = tmp_path / "absent.sqlite"
    bootstrap = await execute_account(
        parse(
            "bootstrap",
            "--database",
            str(absent),
            "--display-name",
            "管理员",
            "--claim-local",
            "--request-key",
            "k",
            "--dry-run",
        )
    )
    assert bootstrap == {
        "operation": "bootstrap",
        "dry_run": True,
        "database": {"exists": False, "would_initialize": True},
        "display_name": "管理员",
        "principal_id": None,
        "claim_local": True,
    }
    recover = await execute_account(
        parse(
            "recover",
            "--database",
            str(absent),
            "--id",
            "local",
            "--request-key",
            "k",
            "--dry-run",
        )
    )
    assert recover == {
        "operation": "recover",
        "dry_run": True,
        "database": {"exists": False, "would_initialize": True},
        "principal_id": "local",
    }
    assert not absent.exists()
    assert calls == [] and stores == []


async def test_offline_import_preflight_reports_metadata_without_secrets(
    tmp_path, stub_service
) -> None:
    calls, _ = stub_service
    result = await execute_account(
        parse(
            "import",
            "--database",
            str(tmp_path / "absent.sqlite"),
            "--policy",
            write_policy(tmp_path),
            "--admin-id",
            "local",
            "--request-key",
            "k",
            "--dry-run",
        )
    )
    assert result["operation"] == "import" and result["dry_run"] is True
    assert result["database"] == {"exists": False, "would_initialize": True}
    assert result["source"] == "policy_file"
    assert result["principal_count"] == 2 and result["platform_admin"] == "local"
    assert [item["principal_id"] for item in result["principals"]] == ["local", "alice"]
    assert result["principals"][0]["generation"] == 2 and result["principals"][0]["legacy"] is True
    assert result["principals"][1]["workspaces"] == [
        {"workspace_id": "ws-a", "role": "member", "approve": True},
        {"workspace_id": "shared", "role": "viewer", "approve": False},
    ]
    rendered = json.dumps(result, ensure_ascii=False)
    assert "old-global-secret" not in rendered and "alice-token" not in rendered
    assert calls == []


async def test_offline_dry_run_still_validates_request_key(tmp_path, stub_service) -> None:
    with pytest.raises(ServiceError):
        await execute_account(
            parse(
                "recover",
                "--database",
                str(tmp_path / "x.sqlite"),
                "--id",
                "local",
                "--request-key",
                "bad key",
                "--dry-run",
            )
        )
    assert stub_service[0] == []


def test_main_bootstrap_prints_token_once(tmp_path, stub_service, monkeypatch, capsys) -> None:
    code, out, _ = run_main(
        monkeypatch,
        capsys,
        "account",
        "bootstrap",
        "--database",
        str(tmp_path / "db.sqlite"),
        "--display-name",
        "管理员",
        "--request-key",
        "boot-main",
    )
    assert code == 0
    assert out.count("\n") == 1
    value = json.loads(out)
    assert value["token"] == "issued-one-time-token"
    assert out.count("issued-one-time-token") == 1


def test_main_recover_prints_token_once_and_records_host(
    tmp_path, stub_service, monkeypatch, capsys
) -> None:
    code, out, _ = run_main(
        monkeypatch,
        capsys,
        "account",
        "recover",
        "--database",
        str(tmp_path / "db.sqlite"),
        "--id",
        "local",
        "--request-key",
        "rec-main",
    )
    assert code == 0
    value = json.loads(out)
    assert value["token"] == "recovery-one-time-token"
    assert out.count("recovery-one-time-token") == 1
    assert stub_service[0] == [("recover", "local", "rec-main", "host"), ("close",)]


def test_main_bootstrap_dry_run_prints_no_token_and_no_database(
    tmp_path, stub_service, monkeypatch, capsys
) -> None:
    database = tmp_path / "absent.sqlite"
    code, out, _ = run_main(
        monkeypatch,
        capsys,
        "account",
        "bootstrap",
        "--database",
        str(database),
        "--display-name",
        "管理员",
        "--request-key",
        "k",
        "--dry-run",
    )
    assert code == 0
    assert json.loads(out)["dry_run"] is True
    assert "token" not in out
    assert not database.exists()
    assert stub_service == ([], [])


def test_main_import_stdin_secret_never_printed(
    tmp_path, stub_service, monkeypatch, capsys
) -> None:
    monkeypatch.setattr(sys, "stdin", io.StringIO("piped-legacy-secret\n"))
    code, out, _ = run_main(
        monkeypatch,
        capsys,
        "account",
        "import",
        "--database",
        str(tmp_path / "db.sqlite"),
        "--global-key-stdin",
        "--admin-id",
        "local",
        "--request-key",
        "imp-main",
    )
    assert code == 0
    assert json.loads(out) == {"imported": 2, "platform_admin": "local"}
    assert "piped-legacy-secret" not in out


def test_main_busy_hints_stopped_service(
    tmp_path, stub_service, monkeypatch, capsys
) -> None:
    import krim_agent.storage.sqlite as sqlite_module

    async def busy(cls, *args, **kwargs):
        raise ServiceError("database_busy")

    monkeypatch.setattr(sqlite_module.SQLiteStore, "open", classmethod(busy))
    code, out, err = run_main(
        monkeypatch,
        capsys,
        "account",
        "bootstrap",
        "--database",
        str(tmp_path / "x.sqlite"),
        "--display-name",
        "管理员",
        "--request-key",
        "k",
    )
    assert code == 1
    assert json.loads(out) == {"error": {"code": "database_busy"}}
    assert "停止服务" in err


def test_main_service_error_becomes_error_json(tmp_path, stub_service, monkeypatch, capsys) -> None:
    # 父包属性仍指向conftest期导入的真实子模块；必须从sys.modules取stub。
    stub_module = sys.modules["krim_agent.accounts.service"]

    async def refusing(
        self,
        display_name: str,
        *,
        request_key: str,
        principal_id: str | None = None,
        claim_local: bool = False,
    ) -> None:
        raise ServiceError("accounts_not_initialized", 409)

    monkeypatch.setattr(stub_module.IdentityService, "bootstrap", refusing)
    code, out, _ = run_main(
        monkeypatch,
        capsys,
        "account",
        "bootstrap",
        "--database",
        str(tmp_path / "db.sqlite"),
        "--display-name",
        "管理员",
        "--request-key",
        "k",
    )
    assert code == 1
    assert json.loads(out) == {"error": {"code": "accounts_not_initialized"}}


async def test_preflight_reports_legacy_database(tmp_path) -> None:
    from krim_agent.storage.models import sessions as storage_sessions
    from krim_agent.storage.sqlite import SQLiteStore

    database = tmp_path / "legacy.sqlite"
    store = await SQLiteStore.open(database)
    try:
        async with store.engine.begin() as conn:
            for index, (workspace, owner) in enumerate(
                (("ws-a", "local"), ("shared-ws", "local"), ("shared-ws", "alice"))
            ):
                await conn.execute(
                    storage_sessions.insert().values(
                        id=f"s-{index}",
                        workspace_id=workspace,
                        owner_id=owner,
                        create_key=f"k-{index}",
                        request_hash="hash",
                        title="预检",
                        target_key="demo",
                        target={},
                        context_policy={},
                        revision=0,
                        status="active",
                        dispatch_paused=False,
                        created_at=1.0,
                        closed_at=None,
                    )
                )
    finally:
        await store.close()
    report = await execute_account(
        parse(
            "bootstrap",
            "--database",
            str(database),
            "--display-name",
            "管理员",
            "--claim-local",
            "--request-key",
            "pre-dry",
            "--dry-run",
        )
    )
    assert report["operation"] == "bootstrap" and report["dry_run"] is True
    assert report["display_name"] == "管理员" and report["claim_local"] is True
    db_report = report["database"]
    assert db_report["exists"] is True and db_report["managed"] is False
    assert db_report["sessions_total"] == 3
    assert db_report["session_owners"] == [
        {"principal_id": "alice", "sessions": 1},
        {"principal_id": "local", "sessions": 2},
    ]
    assert db_report["shared_session_workspaces"] == [
        {"workspace_id": "shared-ws", "owners": ["alice", "local"]}
    ]
    assert db_report["accounts"] == [] and db_report["account_total"] == 0
    assert db_report["legacy_owners"] == []
    assert db_report["shared_membership_workspaces"] == []
    assert db_report["personal_workspaces"] == 0


async def test_preflight_reports_managed_database(tmp_path) -> None:
    database = tmp_path / "managed.sqlite"
    await execute_account(
        parse(
            "bootstrap",
            "--database",
            str(database),
            "--display-name",
            "管理员",
            "--request-key",
            "pre-boot",
        )
    )
    report = await execute_account(
        parse(
            "bootstrap",
            "--database",
            str(database),
            "--display-name",
            "管理员",
            "--request-key",
            "pre-dry",
            "--dry-run",
        )
    )
    db_report = report["database"]
    assert db_report["exists"] is True and db_report["managed"] is True
    assert db_report["account_total"] == 1
    assert db_report["accounts"][0]["platform_role"] == "platform_admin"
    assert db_report["legacy_owners"] == []
    assert db_report["personal_workspaces"] == 1
    assert db_report["shared_membership_workspaces"] == []


async def test_bootstrap_replay_without_principal_id_is_idempotent(tmp_path) -> None:
    database = tmp_path / "replay.sqlite"
    argv = (
        "bootstrap",
        "--database",
        str(database),
        "--display-name",
        "管理员",
        "--request-key",
        "replay-1",
    )
    first = await execute_account(parse(*argv))
    assert first["replayed"] is False and isinstance(first["token"], str)
    replay = await execute_account(parse(*argv))
    assert replay["replayed"] is True
    assert replay["token"] is None
    assert replay["account"]["principal_id"] == first["account"]["principal_id"]


async def test_cli_online_round_trip_over_real_http(tmp_path, monkeypatch) -> None:
    import httpx

    import krim_agent.account_cli as module
    from krim_agent.api.server import create_app
    from krim_agent.client import AsyncRunClient as RealClient
    from krim_agent.composition import open_application
    from krim_agent.config import Settings

    database = tmp_path / "http.sqlite"
    boot = await execute_account(
        parse(
            "bootstrap",
            "--database",
            str(database),
            "--display-name",
            "管理员",
            "--request-key",
            "boot-http",
        )
    )
    admin_token = boot["token"]
    admin_id = boot["account"]["principal_id"]
    assert isinstance(admin_token, str) and boot["replayed"] is False

    async with await open_application(
        database, settings=Settings(database=database)
    ) as application:
        api = create_app(application=application)
        async with api.router.lifespan_context(api):

            def asgi_client(base_url: str, api_key: str = "") -> RealClient:
                client = RealClient(base_url, api_key)
                client.http = httpx.AsyncClient(
                    transport=httpx.ASGITransport(app=api),
                    base_url=base_url,
                    headers={"Authorization": "Bearer " + api_key} if api_key else {},
                    follow_redirects=False,
                    timeout=10,
                )
                return client

            monkeypatch.setattr(module, "AsyncRunClient", asgi_client)

            def with_key(token: str) -> None:
                monkeypatch.setenv("KRIM_AGENT_API_KEY", token)

            with_key(admin_token)
            profile = await execute_account(parse("profile", "--url", "http://127.0.0.1"))
            assert profile["display_name"] == "管理员"
            assert profile["platform_role"] == "platform_admin"

            created = await execute_account(
                parse(
                    "create",
                    "--url",
                    "http://127.0.0.1",
                    "--display-name",
                    "用户甲",
                    "--request-key",
                    "create-http",
                )
            )
            member = created.get("account", created)
            member_id = member["principal_id"]

            listed = await execute_account(
                parse("list", "--url", "http://127.0.0.1", "--limit", "5")
            )
            assert listed["total"] == 2
            assert member_id in {item["principal_id"] for item in listed["items"]}

            issue_argv = (
                "issue",
                "--url",
                "http://127.0.0.1",
                "--id",
                member_id,
                "--request-key",
                "issue-http",
            )
            issued = await execute_account(parse(*issue_argv))
            member_token = issued["token"]
            assert isinstance(member_token, str)
            issue_replay = await execute_account(parse(*issue_argv))
            assert issue_replay["token"] is None and issue_replay["replayed"] is True

            with_key(member_token)
            own = await execute_account(parse("profile", "--url", "http://127.0.0.1"))
            assert own["principal_id"] == member_id
            rotated_member = await execute_account(
                parse("rotate", "--url", "http://127.0.0.1", "--request-key", "rotate-member")
            )
            new_member = rotated_member["token"]
            assert isinstance(new_member, str)
            with pytest.raises(ServiceError) as stale_member:
                await execute_account(parse("profile", "--url", "http://127.0.0.1"))
            assert stale_member.value.code == "unauthorized"
            with_key(new_member)
            assert (
                await execute_account(parse("profile", "--url", "http://127.0.0.1"))
            )["principal_id"] == member_id

            with_key(admin_token)
            current = await execute_account(
                parse("get", "--url", "http://127.0.0.1", "--id", member_id)
            )
            updated = await execute_account(
                parse(
                    "update",
                    "--url",
                    "http://127.0.0.1",
                    "--id",
                    member_id,
                    "--status",
                    "disabled",
                    "--revision",
                    str(current.get("account", current)["revision"]),
                    "--request-key",
                    "disable-http",
                )
            )
            assert updated.get("account", updated)["status"] == "disabled"
            with_key(new_member)
            with pytest.raises(ServiceError) as disabled:
                await execute_account(parse("profile", "--url", "http://127.0.0.1"))
            assert disabled.value.code == "unauthorized"

            with_key(admin_token)
            revoked = await execute_account(
                parse(
                    "revoke",
                    "--url",
                    "http://127.0.0.1",
                    "--id",
                    member_id,
                    "--request-key",
                    "revoke-http",
                )
            )
            assert revoked["key"]["status"] == "revoked"

            events = await execute_account(
                parse(
                    "events",
                    "--url",
                    "http://127.0.0.1",
                    "--request-key",
                    "issue-http",
                    "--limit",
                    "10",
                )
            )
            assert events["total"] >= 1 and events["items"]
            rendered = json.dumps(events, ensure_ascii=False)
            assert member_token not in rendered and new_member not in rendered

            rotated_admin = await execute_account(
                parse("rotate", "--url", "http://127.0.0.1", "--request-key", "rotate-admin")
            )
            new_admin = rotated_admin["token"]
            assert isinstance(new_admin, str)
            with_key(new_admin)
            fresh = await execute_account(parse("profile", "--url", "http://127.0.0.1"))
            assert fresh["principal_id"] == admin_id
            with_key(admin_token)
            with pytest.raises(ServiceError) as stale_admin:
                await execute_account(parse("profile", "--url", "http://127.0.0.1"))
            assert stale_admin.value.code == "unauthorized"

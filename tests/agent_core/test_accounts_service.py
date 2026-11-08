"""IdentityService与账号存储：托管初始化、幂等、防锁死、schema7升级。"""

import json
import sqlite3

import pytest
from pydantic import SecretStr

from krim_agent.accounts import IdentityService
from krim_agent.contracts import RunRequest, ServiceError
from krim_agent.security import AccessPolicy, Credential, Membership, PrincipalContext
from krim_agent.storage.sandbox_models import SANDBOX_TABLES
from krim_agent.storage.sqlite import SCHEMA_VERSION, SQLiteStore

ACCOUNT_TABLES = [
    "accounts",
    "access_keys",
    "account_workspaces",
    "workspace_profiles",
    "account_events",
    "account_meta",
]


def raw(path, names):
    conn = sqlite3.connect(path)
    try:
        return {name: conn.execute(f'SELECT * FROM "{name}"').fetchall() for name in names}
    finally:
        conn.close()


def drop_accounts(path):
    conn = sqlite3.connect(path)
    try:
        for table in reversed(SANDBOX_TABLES):
            conn.execute('DROP TABLE "' + table.name + '"')
        for name in ACCOUNT_TABLES:
            conn.execute(f'DROP TABLE "{name}"')
        conn.execute("PRAGMA user_version=6")
        conn.commit()
    finally:
        conn.close()


def table_names(path):
    conn = sqlite3.connect(path)
    try:
        return {row[0] for row in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")}
    finally:
        conn.close()


async def service_for(tmp_path, name="identity.sqlite"):
    store = await SQLiteStore.open(tmp_path / name)
    service = await IdentityService.open(store)
    return store, service


async def admin_of(service, key="boot:1", display="管理员"):
    first = await service.bootstrap(display, request_key=key)
    return service.authenticate("Bearer " + first.token), first


async def member_view_of(service, admin, key="create:2"):
    return await service.create_account(admin, {"display_name": "李四"}, request_key=key)


async def key_of(service, admin, pid, key="issue:1"):
    return await service.issue_key(admin, pid, {}, request_key=key)


def insert_legacy_local_session(path):
    conn = sqlite3.connect(path)
    try:
        conn.execute(
            "INSERT INTO sessions (id, workspace_id, owner_id, create_key, request_hash,"
            " title, target_key, target, context_policy, revision, status, dispatch_paused,"
            " created_at) VALUES ('s-local', 'local', 'local', 'k', 'h', 't', 'tk', '{}',"
            " '{}', 1, 'closed', 0, 1.0)"
        )
        conn.commit()
    finally:
        conn.close()


async def test_fresh_store_is_unmanaged_and_fails_closed(tmp_path):
    store, service = await service_for(tmp_path)
    try:
        assert service.managed is False
        assert service.principals == ()
        assert service.source is None
        with pytest.raises(ServiceError, match="unauthorized"):
            service.authenticate("Bearer anything")
        with pytest.raises(ServiceError, match="unauthorized"):
            service.resolve("local")
        assert (tmp_path / "identity.sqlite").exists()
        assert store.schema_version == SCHEMA_VERSION
        assert set(ACCOUNT_TABLES) <= table_names(tmp_path / "identity.sqlite")
    finally:
        await service.close()
        await store.close()


async def test_bootstrap_claim_local_keeps_old_owner(tmp_path):
    store, service = await service_for(tmp_path)
    try:
        result = await service.bootstrap(
            "管理员", request_key="boot:1", principal_id="local", claim_local=True
        )
        assert service.managed is True
        assert result.account.principal_id == "local"
        assert result.account.platform_role == "platform_admin"
        assert result.account.legacy is True
        assert isinstance(result.token, str)
        ctx = service.authenticate("Bearer " + result.token)
        assert ctx.principal_id == "local" and ctx.generation == 1 and ctx.legacy is True
        assert ctx.member("local", write=True, approve=True).role == "admin"
        assert service.resolve("local") == ctx
        profile = await service.profile(ctx)
        assert profile.workspaces[0].workspace_id == "local"
        assert profile.workspaces[0].personal is True
        assert profile.workspaces[0].display_name == "个人工作区"
        assert raw(tmp_path / "identity.sqlite", ["workspace_profiles"])["workspace_profiles"] == [
            ("local", "个人工作区", "local")
        ]
    finally:
        await service.close()
        await store.close()


async def test_bootstrap_rejects_local_without_explicit_claim(tmp_path):
    store, service = await service_for(tmp_path)
    try:
        with pytest.raises(ServiceError, match="invalid_input"):
            await service.bootstrap(
                "管理员", request_key="boot:1", principal_id="local", claim_local=False
            )
        with pytest.raises(ServiceError, match="invalid_input"):
            await service.bootstrap("管理员", request_key="boot:1", claim_local=True)
        result = await service.bootstrap("管理员", request_key="boot:1")
        profile = await service.profile(service.authenticate("Bearer " + result.token))
        assert all(ref.workspace_id != "local" for ref in profile.workspaces)
        assert result.account.principal_id != "local"
    finally:
        await service.close()
        await store.close()


async def test_bootstrap_requires_explicit_claim_for_legacy_local_data(tmp_path):
    database = tmp_path / "legacy.sqlite"
    store = await SQLiteStore.open(database)
    try:
        service = await IdentityService.open(store)
        try:
            insert_legacy_local_session(database)
            with pytest.raises(ServiceError, match="revision_conflict"):
                await service.bootstrap("管理员", request_key="boot:1")
            result = await service.bootstrap(
                "管理员", request_key="boot:2", principal_id="local", claim_local=True
            )
            assert result.account.principal_id == "local"
            ctx = service.authenticate("Bearer " + result.token)
            assert ctx.member("local").role == "admin"
        finally:
            await service.close()
    finally:
        await store.close()


async def test_bootstrap_replay_and_conflict(tmp_path):
    store, service = await service_for(tmp_path)
    try:
        first = await service.bootstrap("管理员", request_key="boot:1")
        replay = await service.bootstrap("管理员", request_key="boot:1")
        assert replay.replayed is True and replay.token is None
        assert replay.account.principal_id == first.account.principal_id
        assert first.account.revision == 1
        with pytest.raises(ServiceError, match="key_conflict"):
            await service.bootstrap("别的名字", request_key="boot:1")
        with pytest.raises(ServiceError, match="revision_conflict"):
            await service.bootstrap(
                "又一个", request_key="boot:2", principal_id="local", claim_local=True
            )
    finally:
        await service.close()
        await store.close()


async def test_create_account_bundle_and_idempotency(tmp_path):
    store, service = await service_for(tmp_path)
    try:
        admin, _ = await admin_of(service)
        body = {
            "display_name": "  张三  ",
            "memberships": [{"workspace_id": "shared", "role": "member", "approve": True}],
        }
        created = await service.create_account(admin, body, request_key="create:1")
        account = created.account
        assert account.display_name == "张三"
        assert account.status == "active"
        assert account.platform_role == "member"
        assert account.generation == 1
        assert account.revision == 1
        assert account.has_active_key is False and account.key is None
        dumped = json.loads(created.model_dump_json())
        assert dumped["account"]["principal_id"] == account.principal_id
        replay = await service.create_account(admin, body, request_key="create:1")
        assert replay.replayed is True and replay.account.principal_id == account.principal_id
        with pytest.raises(ServiceError, match="key_conflict"):
            await service.create_account(admin, {"display_name": "别人"}, request_key="create:1")
        page = await service.list_accounts(admin)
        assert page.total == 2
        assert account.principal_id in {item.principal_id for item in page.items}
        assert await service.account(admin, account.principal_id) == account
        with pytest.raises(ServiceError, match="not_found"):
            await service.account(admin, "missing-1")
        events = await service.events(admin, request_key="create:1")
        assert events.total == 1 and events.items[0].action == "create"
        assert events.items[0].target == account.principal_id
    finally:
        await service.close()
        await store.close()


async def test_member_boundaries_and_self_profile(tmp_path):
    store, service = await service_for(tmp_path)
    try:
        admin, _ = await admin_of(service)
        member_view = await member_view_of(service, admin)
        with pytest.raises(ServiceError, match="not_found"):
            await service.issue_key(admin, "ghost-1", {}, request_key="issue:0")
        issued = await key_of(service, admin, member_view.account.principal_id)
        member = service.authenticate("Bearer " + issued.token)
        assert member.principal_id == member_view.account.principal_id
        assert member.memberships[0].workspace_id != "local"
        with pytest.raises(ServiceError, match="permission_denied"):
            await service.list_accounts(member)
        with pytest.raises(ServiceError, match="permission_denied"):
            await service.create_account(member, {"display_name": "x"}, request_key="c:3")
        with pytest.raises(ServiceError, match="permission_denied"):
            await service.events(member)
        with pytest.raises(ServiceError, match="permission_denied"):
            await service.set_memberships(
                member, admin.principal_id, {"memberships": []}, request_key="m:0"
            )
        with pytest.raises(ServiceError, match="permission_denied"):
            await service.update_account(
                member, admin.principal_id, {"display_name": "劫持"}, request_key="u:0"
            )
        with pytest.raises(ServiceError, match="permission_denied"):
            await service.update_account(
                member,
                member.principal_id,
                {"display_name": "李四5", "status": "active"},
                request_key="u:1",
            )
        with pytest.raises(ServiceError, match="permission_denied"):
            await service.issue_key(member, member.principal_id, {}, request_key="i:9")
        renamed = await service.update_account(
            member, member.principal_id, {"display_name": "李四5"}, request_key="u:2"
        )
        assert renamed.account.display_name == "李四5" and renamed.account.generation == 1
        profile = await service.profile(member)
        assert profile.display_name == "李四5"
        assert profile.workspaces[0].display_name == "个人工作区"
        assert profile.key is not None and profile.key.weak is False
        assert await service.account(admin, member.principal_id) == renamed.account
    finally:
        await service.close()
        await store.close()


async def test_actor_scoped_request_keys(tmp_path):
    store, service = await service_for(tmp_path)
    try:
        admin, _ = await admin_of(service)
        member_view = await member_view_of(service, admin)
        issued = await key_of(service, admin, member_view.account.principal_id)
        member = service.authenticate("Bearer " + issued.token)
        # request_key按(actor,key)唯一：不同用户可各自使用同名键，互不冲突。
        renamed = await service.update_account(
            member, member.principal_id, {"display_name": "撞键"}, request_key="shared-key"
        )
        assert renamed.replayed is False
        admin_note = await service.update_account(
            admin, admin.principal_id, {"display_name": "管理员甲"}, request_key="shared-key"
        )
        assert admin_note.replayed is False
        replay = await service.update_account(
            member, member.principal_id, {"display_name": "撞键"}, request_key="shared-key"
        )
        assert replay.replayed is True and replay.account.display_name == "撞键"
        events = await service.events(admin, request_key="shared-key")
        assert events.total == 2
        assert {event.actor for event in events.items} == {
            admin.principal_id,
            member.principal_id,
        }
        mine = await service.events(
            admin, principal_id=admin.principal_id, request_key="shared-key"
        )
        assert [event.actor for event in mine.items] == [admin.principal_id]
        theirs = await service.events(
            admin, principal_id=member.principal_id, request_key="shared-key"
        )
        assert [event.actor for event in theirs.items] == [member.principal_id]
    finally:
        await service.close()
        await store.close()


async def test_reload_failure_fails_closed_until_refresh(tmp_path, monkeypatch):
    store, service = await service_for(tmp_path)
    try:
        admin, first = await admin_of(service)
        original = IdentityService._load

        async def broken(self, conn):
            raise RuntimeError("reload broken")

        monkeypatch.setattr(IdentityService, "_load", broken)
        with pytest.raises(ServiceError, match="storage_unavailable"):
            await service.update_account(
                admin, admin.principal_id, {"display_name": "新名"}, request_key="u:1"
            )
        with pytest.raises(ServiceError, match="storage_unavailable"):
            service.authenticate("Bearer " + first.token)
        with pytest.raises(ServiceError, match="storage_unavailable"):
            service.resolve(admin.principal_id)
        with pytest.raises(ServiceError, match="storage_unavailable"):
            service.validate(admin)
        assert service.principals == ()
        monkeypatch.setattr(IdentityService, "_load", original)
        await service.refresh()
        assert service.authenticate("Bearer " + first.token).principal_id == admin.principal_id
        replay = await service.update_account(
            admin, admin.principal_id, {"display_name": "新名"}, request_key="u:1"
        )
        assert replay.replayed is True and replay.account.display_name == "新名"
    finally:
        await service.close()
        await store.close()


async def test_lock_window_permission_recheck(tmp_path):
    store, service = await service_for(tmp_path)
    try:
        admin, _ = await admin_of(service)
        member_view = await member_view_of(service, admin)
        issued = await key_of(service, admin, member_view.account.principal_id)
        stale_ctx = service.authenticate("Bearer " + issued.token)
        current = await service.set_memberships(
            admin, member_view.account.principal_id, {"memberships": []}, request_key="members:1"
        )
        assert current.generation == 2
        # 旧generation上下文即使identity/密钥有效也在锁内被拒。
        with pytest.raises(ServiceError, match="unauthorized"):
            await service.rotate_key(stale_ctx, {}, request_key="rotate:stale")
        fresh = service.authenticate("Bearer " + issued.token)
        rotated = await service.rotate_key(fresh, {}, request_key="rotate:fresh")
        assert rotated.token is not None
    finally:
        await service.close()
        await store.close()


async def test_rotate_keeps_generation_and_replay_hides_token(tmp_path):
    store, service = await service_for(tmp_path)
    try:
        first = await service.bootstrap("管理员", request_key="boot:1")
        ctx = service.authenticate("Bearer " + first.token)
        rotated = await service.rotate_key(ctx, {}, request_key="rotate:1")
        assert rotated.revoked_key_id == first.account.key.key_id
        with pytest.raises(ServiceError, match="unauthorized"):
            service.authenticate("Bearer " + first.token)
        new_ctx = service.authenticate("Bearer " + rotated.token)
        assert new_ctx.principal_id == ctx.principal_id
        assert new_ctx.generation == ctx.generation == 1
        detail = await service.account(new_ctx, ctx.principal_id)
        assert detail.key.credential_version == 2 and detail.key.status == "active"
        replay = await service.rotate_key(new_ctx, {}, request_key="rotate:1")
        assert replay.replayed is True and replay.token is None
        assert replay.key.key_id == rotated.key.key_id
        assert replay.key.credential_version == 2
        dumped = json.loads(rotated.model_dump_json())
        assert dumped["token"] == rotated.token
        assert "**********" in repr(rotated)
        keys = raw(tmp_path / "identity.sqlite", ["access_keys"])["access_keys"]
        assert sorted((row[6], row[3]) for row in keys) == [("active", 2), ("revoked", 1)]
    finally:
        await service.close()
        await store.close()


async def test_issue_re_sign_bumps_generation(tmp_path):
    store, service = await service_for(tmp_path)
    try:
        admin, _ = await admin_of(service)
        member_view = await member_view_of(service, admin)
        first = await key_of(service, admin, member_view.account.principal_id)
        assert first.revoked_key_id is None
        member_ctx = service.authenticate("Bearer " + first.token)
        assert member_ctx.generation == 1
        second = await key_of(service, admin, member_view.account.principal_id, key="issue:2")
        assert second.revoked_key_id == first.key.key_id
        with pytest.raises(ServiceError, match="unauthorized"):
            service.authenticate("Bearer " + first.token)
        re_ctx = service.authenticate("Bearer " + second.token)
        assert re_ctx.generation == 2
        detail = await service.account(admin, member_view.account.principal_id)
        assert detail.key.credential_version == 2
        replay = await key_of(service, admin, member_view.account.principal_id, key="issue:2")
        assert replay.replayed is True and replay.token is None
        replay2 = await key_of(service, admin, member_view.account.principal_id)
        assert replay2.replayed is True and replay2.key.status == "revoked"
    finally:
        await service.close()
        await store.close()


async def test_last_loginable_admin_protection(tmp_path):
    store, service = await service_for(tmp_path)
    try:
        first = await service.bootstrap("管理员", request_key="boot:1")
        admin = service.authenticate("Bearer " + first.token)
        with pytest.raises(ServiceError, match="permission_denied"):
            await service.revoke_key(admin, admin.principal_id, {}, request_key="revoke:1")
        with pytest.raises(ServiceError, match="permission_denied"):
            await service.update_account(
                admin, admin.principal_id, {"status": "disabled"}, request_key="disable:1"
            )
        with pytest.raises(ServiceError, match="permission_denied"):
            await service.update_account(
                admin, admin.principal_id, {"platform_role": "member"}, request_key="demote:1"
            )
        rotated = await service.rotate_key(admin, {}, request_key="rotate:1")
        assert rotated.token is not None
        admin = service.authenticate("Bearer " + rotated.token)
        peer = await service.create_account(
            admin,
            {"display_name": "副手", "platform_role": "platform_admin"},
            request_key="create:2",
        )
        peer_key = await key_of(service, admin, peer.account.principal_id)
        peer_ctx = service.authenticate("Bearer " + peer_key.token)
        revoked = await service.revoke_key(admin, admin.principal_id, {}, request_key="revoke:2")
        assert revoked.generation == 2
        with pytest.raises(ServiceError, match="unauthorized"):
            service.authenticate("Bearer " + rotated.token)
        assert [p.principal_id for p in service.principals] == [peer.account.principal_id]
        remaining = await service.account(peer_ctx, admin.principal_id)
        assert remaining.has_active_key is False and remaining.generation == 2
        recovered = await service.recover(admin.principal_id, request_key="recover:1")
        admin2 = service.authenticate("Bearer " + recovered.token)
        assert admin2.generation == 3
        with pytest.raises(ServiceError, match="not_found"):
            await service.recover("ghost-1", request_key="recover:2")
    finally:
        await service.close()
        await store.close()


async def test_disable_enable_generation_and_snapshot(tmp_path):
    store, service = await service_for(tmp_path)
    try:
        admin, _ = await admin_of(service)
        member_view = await member_view_of(service, admin)
        key = await key_of(service, admin, member_view.account.principal_id)
        ctx = service.authenticate("Bearer " + key.token)
        disabled = await service.update_account(
            admin, member_view.account.principal_id, {"status": "disabled"}, request_key="disable:1"
        )
        assert disabled.account.status == "disabled" and disabled.account.generation == 2
        with pytest.raises(ServiceError, match="unauthorized"):
            service.authenticate("Bearer " + key.token)
        with pytest.raises(ServiceError, match="unauthorized"):
            service.resolve(member_view.account.principal_id)
        with pytest.raises(ServiceError, match="unauthorized"):
            service.validate(ctx)
        with pytest.raises(ServiceError, match="unauthorized"):
            await service.update_account(
                ctx, member_view.account.principal_id, {"display_name": "x"}, request_key="u:9"
            )
        replay = await service.update_account(
            admin, member_view.account.principal_id, {"status": "disabled"}, request_key="disable:1"
        )
        assert replay.replayed is True
        enabled = await service.update_account(
            admin, member_view.account.principal_id, {"status": "active"}, request_key="enable:1"
        )
        assert enabled.account.generation == 3
        assert service.authenticate("Bearer " + key.token).generation == 3
        stale = PrincipalContext(ctx.principal_id, 1, ctx.memberships, False)
        with pytest.raises(ServiceError, match="unauthorized"):
            service.validate(stale)
    finally:
        await service.close()
        await store.close()


async def test_display_rename_conflict_and_stability(tmp_path):
    store, service = await service_for(tmp_path)
    try:
        first = await service.bootstrap("管理员", request_key="boot:1")
        admin = service.authenticate("Bearer " + first.token)
        before = await service.account(admin, admin.principal_id)
        with pytest.raises(ServiceError, match="revision_conflict"):
            await service.update_account(
                admin,
                admin.principal_id,
                {"display_name": "新名", "expected_revision": before.revision + 1},
                request_key="u:1",
            )
        renamed = await service.update_account(
            admin,
            admin.principal_id,
            {"display_name": "  新名  ", "expected_revision": before.revision},
            request_key="u:2",
        )
        assert renamed.account.display_name == "新名"
        assert renamed.account.principal_id == before.principal_id
        assert renamed.account.generation == before.generation
        assert renamed.account.revision == before.revision + 1
        current = service.resolve(before.principal_id)
        assert current.generation == before.generation
        assert service.validate(current).memberships == current.memberships
        with pytest.raises(ServiceError, match="unauthorized"):
            service.validate(PrincipalContext(before.principal_id, 99, (), False))
    finally:
        await service.close()
        await store.close()


async def test_set_memberships_json_and_generation(tmp_path):
    store, service = await service_for(tmp_path)
    try:
        admin, _ = await admin_of(service)
        member_view = await member_view_of(service, admin)
        key = await key_of(service, admin, member_view.account.principal_id)
        ctx = service.authenticate("Bearer " + key.token)
        pid = member_view.account.principal_id
        current = await service.account(admin, pid)
        updated = await service.set_memberships(
            admin,
            pid,
            {
                "memberships": [
                    {"workspace_id": "proj", "role": "member", "approve": True},
                    {"workspace_id": "archive", "role": "viewer", "approve": False},
                ],
                "expected_generation": current.generation,
            },
            request_key="members:1",
        )
        owner_rows = [m for m in updated.memberships if m.role == "admin"]
        assert len(owner_rows) == 1
        assert {m.workspace_id for m in updated.memberships} == {
            "archive",
            "proj",
            owner_rows[0].workspace_id,
        }
        assert owner_rows[0].workspace_id not in {"archive", "proj"}
        assert updated.generation == current.generation + 1
        with pytest.raises(ServiceError, match="unauthorized"):
            service.validate(ctx)
        fresh = service.resolve(pid)
        assert fresh.generation == updated.generation
        assert service.validate(fresh).generation == updated.generation
        fresh.member("proj", write=True)
        with pytest.raises(ServiceError, match="revision_conflict"):
            await service.set_memberships(
                admin,
                pid,
                {
                    "memberships": [{"workspace_id": "proj", "role": "viewer", "approve": True}],
                    "expected_generation": current.generation,
                },
                request_key="members:2",
            )
        with pytest.raises(ServiceError, match="invalid_input"):
            await service.set_memberships(
                admin,
                pid,
                {
                    "memberships": [
                        {"workspace_id": "proj", "role": "member", "approve": True},
                        {"workspace_id": "proj", "role": "viewer", "approve": True},
                    ]
                },
                request_key="members:4",
            )
        replay = await service.set_memberships(
            admin,
            pid,
            {
                "memberships": [
                    {"workspace_id": "proj", "role": "member", "approve": True},
                    {"workspace_id": "archive", "role": "viewer", "approve": False},
                ],
                "expected_generation": current.generation,
            },
            request_key="members:1",
        )
        assert replay.replayed is True
        assert {m.workspace_id for m in replay.memberships} == {
            "archive",
            "proj",
            owner_rows[0].workspace_id,
        }
        empty = await service.set_memberships(
            admin, pid, {"memberships": []}, request_key="members:3"
        )
        assert [m.workspace_id for m in empty.memberships] == [owner_rows[0].workspace_id]
    finally:
        await service.close()
        await store.close()


async def test_on_change_actions_and_fail_closed(tmp_path):
    store, service = await service_for(tmp_path)
    calls = []

    async def hook(principal, invalidate, action):
        calls.append((principal, invalidate, action))

    service.on_change = hook
    try:
        admin, _ = await admin_of(service)
        member_view = await member_view_of(service, admin)
        pid = member_view.account.principal_id
        key = await service.issue_key(admin, pid, {}, request_key="issue:1")
        ctx = service.authenticate("Bearer " + key.token)
        await service.update_account(admin, pid, {"display_name": "李四2"}, request_key="u:1")
        await service.rotate_key(ctx, {}, request_key="rotate:1")
        await service.update_account(admin, pid, {"status": "disabled"}, request_key="disable:1")
        await service.update_account(admin, pid, {"status": "active"}, request_key="enable:1")
        await service.set_memberships(admin, pid, {"memberships": []}, request_key="members:1")
        actions = [(invalidate, action) for _, invalidate, action in calls]
        assert (False, "bootstrap") in actions
        assert (False, "create") in actions
        assert (False, "issue") in actions
        assert (False, "update") in actions
        assert (False, "rotate") in actions
        assert (True, "disable") in actions
        assert (True, "enable") in actions
        assert (True, "memberships") in actions

        async def failing(principal, invalidate, action):
            raise RuntimeError("hook failed")

        service.on_change = failing
        with pytest.raises(ServiceError, match="storage_unavailable"):
            await service.update_account(admin, pid, {"display_name": "李四3"}, request_key="u:2")
        service.on_change = None
        with pytest.raises(ServiceError, match="storage_unavailable"):
            service.authenticate("Bearer " + key.token)
        await service.refresh()
        replay = await service.update_account(
            admin, pid, {"display_name": "李四3"}, request_key="u:2"
        )
        assert replay.replayed is True
    finally:
        await service.close()
        await store.close()


async def test_commit_unknown_confirmed_not_reissued(tmp_path):
    store, service = await service_for(tmp_path)
    try:
        first = await service.bootstrap("管理员", request_key="boot:1")
        ctx = service.authenticate("Bearer " + first.token)

        async def failing(principal, invalidate, action):
            raise RuntimeError("hook failed")

        service.on_change = failing
        with pytest.raises(ServiceError, match="storage_unavailable"):
            await service.rotate_key(ctx, {}, request_key="rotate:1")
        service.on_change = None
        with pytest.raises(ServiceError, match="storage_unavailable"):
            service.resolve(ctx.principal_id)
        await service.refresh()
        counts = raw(tmp_path / "identity.sqlite", ["access_keys", "account_events"])
        assert len(counts["access_keys"]) == 2 and len(counts["account_events"]) == 2
        replay = await service.rotate_key(ctx, {}, request_key="rotate:1")
        assert replay.replayed is True and replay.token is None
        counts = raw(tmp_path / "identity.sqlite", ["access_keys", "account_events"])
        assert len(counts["access_keys"]) == 2 and len(counts["account_events"]) == 2
        detail = await service.account(service.resolve(ctx.principal_id), ctx.principal_id)
        assert detail.key.credential_version == 2
        assert service.validate(service.resolve(ctx.principal_id)) is not None
    finally:
        await service.close()
        await store.close()


async def test_no_secret_material_persisted(tmp_path):
    store, service = await service_for(tmp_path)
    try:
        first = await service.bootstrap("管理员", request_key="boot:1")
        admin = service.authenticate("Bearer " + first.token)
        member_view = await member_view_of(service, admin)
        key = await key_of(service, admin, member_view.account.principal_id)
        await service.rotate_key(
            service.authenticate("Bearer " + key.token), {}, request_key="rotate:1"
        )
        events = await service.events(admin)
        blob = json.dumps([event.model_dump(mode="json") for event in events.items])
        assert first.token not in blob and key.token not in blob
        database = (tmp_path / "identity.sqlite").read_bytes()
        assert first.token.encode() not in database
        assert key.token.encode() not in database
        keys = raw(tmp_path / "identity.sqlite", ["access_keys"])["access_keys"]
        assert all(len(row[2]) == 64 for row in keys)
        assert all("krim1." in row[4] for row in keys)
    finally:
        await service.close()
        await store.close()


async def test_events_require_admin_and_query_by_key(tmp_path):
    store, service = await service_for(tmp_path)
    try:
        admin, _ = await admin_of(service)
        member_view = await member_view_of(service, admin)
        pid = member_view.account.principal_id
        page = await service.events(admin, principal_id=pid)
        assert page.total == 1 and page.items[0].target == pid
        single = await service.events(admin, request_key="create:2")
        assert single.total == 1 and single.items[0].action == "create"
        assert (await service.events(admin, request_key="absent:1")).total == 0
    finally:
        await service.close()
        await store.close()


def legacy_policy():
    return AccessPolicy(
        credentials=(
            Credential(
                principal_id="legacy-admin",
                token=SecretStr("short"),
                generation=3,
                memberships=(Membership(workspace_id="proj", role="admin"),),
                legacy=True,
            ),
            Credential(
                principal_id="worker",
                token=SecretStr("w" * 40),
                generation=1,
                memberships=(Membership(workspace_id="proj", role="member"),),
            ),
        )
    )


async def test_import_policy_preserves_and_is_idempotent(tmp_path):
    store, service = await service_for(tmp_path)
    try:
        policy = legacy_policy()
        result = await service.import_policy(
            policy, "legacy-admin", request_key="import:1", display_names={"worker": " 工人 "}
        )
        assert service.managed is True
        assert result.replayed is False
        assert {account.principal_id for account in result.accounts} == {"legacy-admin", "worker"}
        by_id = {account.principal_id: account for account in result.accounts}
        assert by_id["legacy-admin"].platform_role == "platform_admin"
        assert by_id["legacy-admin"].legacy is True
        assert by_id["legacy-admin"].generation == 3
        assert by_id["worker"].display_name == "工人"
        assert by_id["worker"].platform_role == "member"
        assert by_id["worker"].key.weak is False and by_id["legacy-admin"].key.weak is True
        legacy_ctx = service.authenticate("Bearer short")
        assert legacy_ctx.principal_id == "legacy-admin" and legacy_ctx.legacy is True
        assert legacy_ctx.generation == 3
        worker_ctx = service.authenticate("Bearer " + "w" * 40)
        worker_ctx.member("proj")
        again = await service.import_policy(policy, "legacy-admin", request_key="import:2")
        assert again.replayed is True
        assert {account.principal_id for account in again.accounts} == {"legacy-admin", "worker"}
        changed = AccessPolicy(
            credentials=(
                Credential(
                    principal_id="legacy-admin",
                    token=SecretStr("changed"),
                    generation=3,
                    memberships=(Membership(workspace_id="proj", role="admin"),),
                    legacy=True,
                ),
            )
        )
        with pytest.raises(ServiceError, match="key_conflict"):
            await service.import_policy(changed, "legacy-admin", request_key="import:1")
        with pytest.raises(ServiceError, match="invalid_input"):
            await service.import_policy(policy, "worker-not-in-policy", request_key="import:3")
        assert service.source == result.source
        keys = raw(tmp_path / "identity.sqlite", ["access_keys"])["access_keys"]
        assert "short" not in " ".join(row[4] for row in keys)
        assert len(keys) == 2
    finally:
        await service.close()
        await store.close()


async def test_import_does_not_map_unknown_owners_or_local(tmp_path):
    database = tmp_path / "import.sqlite"
    store = await SQLiteStore.open(database)
    try:
        service = await IdentityService.open(store)
        try:
            insert_legacy_local_session(database)
            result = await service.import_policy(
                legacy_policy(), "legacy-admin", request_key="import:1"
            )
            principals = {account.principal_id for account in result.accounts}
            assert principals == {"legacy-admin", "worker"}
            assert "ghost-owner" not in principals and "local" not in principals
            rows = raw(database, ["accounts"])["accounts"]
            assert {row[0] for row in rows} == {"legacy-admin", "worker", "local"}
            reserved = next(row for row in rows if row[0] == "local")
            assert reserved[2] == "disabled" and reserved[3] == "member"
            with pytest.raises(ServiceError, match="unauthorized"):
                service.resolve("local")
            account_rows = raw(database, ["account_workspaces"])["account_workspaces"]
            assert all(row[0] != "local" for row in account_rows)
            roles = {row[0]: row[3] for row in rows}
            assert roles["legacy-admin"] == "platform_admin"
            assert roles["worker"] == "member"
            sessions_rows = raw(database, ["sessions"])["sessions"]
            assert sessions_rows[0][2] == "local"
        finally:
            await service.close()
    finally:
        await store.close()


async def test_last_used_bounded_merge(tmp_path):
    store, service = await service_for(tmp_path)
    try:
        first = await service.bootstrap("管理员", request_key="boot:1")
        service.authenticate("Bearer " + first.token)
        service.authenticate("Bearer " + first.token)
        keys = raw(tmp_path / "identity.sqlite", ["access_keys"])["access_keys"]
        assert keys[0][9] is None
        await service.flush_usage()
        keys = raw(tmp_path / "identity.sqlite", ["access_keys"])["access_keys"]
        after = keys[0][9]
        assert after is not None
        service.authenticate("Bearer " + first.token)
        await service.close()
        keys = raw(tmp_path / "identity.sqlite", ["access_keys"])["access_keys"]
        assert keys[0][9] >= after
    finally:
        await store.close()


async def test_schema7_upgrade_preserves_old_rows(tmp_path):
    database = tmp_path / "upgrade.sqlite"
    store = await SQLiteStore.open(database)
    view, created = await store.begin(RunRequest(text="保留", request_key="keep:1"), {"v": 2})
    await store.close()
    assert created is True
    drop_accounts(database)
    before = raw(database, ["runs", "run_events"])
    assert not (set(ACCOUNT_TABLES) & table_names(database))
    conn = sqlite3.connect(database)
    try:
        assert conn.execute("PRAGMA user_version").fetchone()[0] == 6
    finally:
        conn.close()
    writer = await SQLiteStore.open(database)
    try:
        assert writer.schema_version == SCHEMA_VERSION
        assert raw(database, ["runs", "run_events"]) == before
        assert set(ACCOUNT_TABLES) <= table_names(database)
        service = await IdentityService.open(writer)
        try:
            assert service.managed is False
            result = await service.bootstrap(
                "管理员", request_key="boot:1", principal_id="local", claim_local=True
            )
            assert result.token is not None
            assert (await writer.snapshot(view.id)).request.text == "保留"
        finally:
            await service.close()
    finally:
        await writer.close()


async def test_readonly_v6_keeps_original_schema(tmp_path):
    database = tmp_path / "readonly.sqlite"
    store = await SQLiteStore.open(database)
    await store.close()
    drop_accounts(database)
    before = database.read_bytes()
    reader = await SQLiteStore.open(database, readonly=True)
    try:
        assert reader.schema_version == 6
        service = await IdentityService.open(reader)
        try:
            assert service.managed is False
            assert service.principals == ()
            with pytest.raises(ServiceError, match="unauthorized"):
                service.authenticate("Bearer x")
        finally:
            await service.close()
    finally:
        await reader.close()
    assert database.read_bytes() == before


async def test_upgrade_failure_rolls_back_before_unlock(tmp_path, monkeypatch):
    database = tmp_path / "rollback.sqlite"
    store = await SQLiteStore.open(database)
    await store.close()
    drop_accounts(database)
    before = database.read_bytes()
    original = SQLiteStore._upgrade

    async def injected(conn):
        await original(conn)
        raise RuntimeError("injected migration failure")

    monkeypatch.setattr(SQLiteStore, "_upgrade", staticmethod(injected))
    with pytest.raises(ServiceError, match="database_invalid"):
        await SQLiteStore.open(database)
    assert database.read_bytes() == before
    monkeypatch.setattr(SQLiteStore, "_upgrade", staticmethod(original))
    probe = await SQLiteStore.open(database)
    try:
        assert probe.schema_version == SCHEMA_VERSION
    finally:
        await probe.close()


async def test_managed_mode_survives_restart_and_never_silently_degrades(tmp_path):
    database = tmp_path / "managed.sqlite"
    store, service = await service_for(tmp_path, name="managed.sqlite")
    try:
        await admin_of(service)
        assert service.managed is True
    finally:
        await service.close()
        await store.close()
    conn = sqlite3.connect(database)
    try:
        assert (
            json.loads(
                conn.execute("SELECT body FROM account_meta WHERE id='identity'").fetchone()[0]
            )["mode"]
            == "managed"
        )
        conn.execute("DELETE FROM account_meta WHERE id='identity'")
        conn.commit()
    finally:
        conn.close()
    probe = await SQLiteStore.open(database)
    try:
        with pytest.raises(ServiceError, match="facts_corrupt"):
            await IdentityService.open(probe)
    finally:
        await probe.close()
    conn = sqlite3.connect(database)
    try:
        conn.execute(
            "INSERT INTO account_meta (id, body) VALUES ('identity', '{\"mode\": \"legacy\"}')"
        )
        conn.commit()
    finally:
        conn.close()
    probe = await SQLiteStore.open(database)
    try:
        with pytest.raises(ServiceError, match="facts_corrupt"):
            await IdentityService.open(probe)
    finally:
        await probe.close()


async def test_v5_v6_table_sets_derived_from_metadata_exclusion(tmp_path):
    database = tmp_path / "sets.sqlite"
    store = await SQLiteStore.open(database)
    await store.close()
    drop_accounts(database)
    reader = await SQLiteStore.open(database, readonly=True)
    try:
        assert reader.schema_version == 6
    finally:
        await reader.close()
    conn = sqlite3.connect(database)
    try:
        conn.execute('DROP TABLE "park_checkpoints"')
        conn.execute("PRAGMA user_version=5")
        conn.commit()
    finally:
        conn.close()
    reader = await SQLiteStore.open(database, readonly=True)
    try:
        assert reader.schema_version == 5
    finally:
        await reader.close()
    assert "accounts" not in table_names(database)

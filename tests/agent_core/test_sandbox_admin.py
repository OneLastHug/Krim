"""沙箱部署dry-run的合成测试：预检纯函数、备份/恢复计划与模板静态检查。

不触碰真实部署、root服务或真实备份；文件系统行为在本机临时目录验证。
"""

from __future__ import annotations

import argparse
import importlib.util
import json
import os
import shutil
import sqlite3
import stat
import types
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parents[2]
SCRIPT_PATH = REPO / "scripts" / "sandbox_admin.py"
TEMPLATE_DIR = REPO / "deployment" / "sandbox"

MASTER_SECRET = "master-secret-value"


def _load_script():
    spec = importlib.util.spec_from_file_location("sandbox_admin", SCRIPT_PATH)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


sa = _load_script()


@pytest.fixture
def fixture_roots(tmp_path):
    data = tmp_path / "data"
    state = tmp_path / "state"
    secrets = tmp_path / "secrets"
    backup = tmp_path / "backup"
    for root in (data, state, secrets):
        root.mkdir()
    project = data / "projects" / "proj-1" / "content"
    project.mkdir(parents=True)
    (project / "README.md").write_text("hello", encoding="utf-8")
    db = state / "control.sqlite3"
    connection = sqlite3.connect(db)
    connection.execute("CREATE TABLE t (v TEXT)")
    connection.commit()
    connection.close()
    (secrets / "credentials.json").write_text(
        json.dumps({"connection": "a", "token": "tok-abc123"}), encoding="utf-8"
    )
    (secrets / "master.key").write_text(MASTER_SECRET, encoding="utf-8")
    return {"data": data, "state": state, "secrets": secrets, "backup": backup}


def _fixture_plan(roots):
    return sa.create_backup_plan(
        data_root=roots["data"],
        state_root=roots["state"],
        secrets_root=roots["secrets"],
        backup_root=roots["backup"],
        fixture=True,
    )


def test_parse_lxss_registry_pairs_and_skips_unnamed() -> None:
    sample = (
        "HKEY_CURRENT_USER\\Software\\Microsoft\\Windows\\CurrentVersion\\Lxss\\{1111}\n"
        "    DistributionName    REG_SZ    Krim-Ubuntu-24.04\n"
        "    BasePath    REG_SZ    F:\\WSL\\Krim-Ubuntu-24.04\n"
        "\n"
        "HKEY_CURRENT_USER\\Software\\Microsoft\\Windows\\CurrentVersion\\Lxss\\{2222}\n"
        "    BasePath    REG_SZ    C:\\wsl\\docker\n"
    )
    assert sa.parse_lxss_registry(sample) == {"Krim-Ubuntu-24.04": "F:\\WSL\\Krim-Ubuntu-24.04"}


def test_validate_host_evidence_requires_f_drive() -> None:
    good = {
        "distro_name": "Krim-Ubuntu-24.04",
        "base_path_windows": "F:\\WSL\\dev",
        "drive_letter": "F",
        "source": "windows-registry",
    }
    assert sa.validate_host_evidence(good) == []
    other = {**good, "drive_letter": "C", "base_path_windows": "C:\\wsl"}
    assert any("不是F" in problem for problem in sa.validate_host_evidence(other))
    assert len(sa.validate_host_evidence({"source": "windows-registry"})) >= 1
    assert sa.validate_host_evidence(["not-an-object"]) == ["evidence必须是JSON对象"]


def test_classify_mount_native_prjquota_and_windows_fs() -> None:
    xfs = sa.classify_mount(
        {"fstype": "xfs", "options": ["rw", "prjquota", "nosuid"], "source": "/dev/sdc"}
    )
    assert xfs["native_linux_fs"] and xfs["prjquota"] and not xfs["windows_or_network_fs"]
    windows = sa.classify_mount({"fstype": "9p", "options": ["rw"], "source": "/mnt/f"})
    assert windows["windows_or_network_fs"] and not windows["native_linux_fs"]
    ext4 = sa.classify_mount({"fstype": "ext4", "options": ["rw"], "source": "/dev/sdc"})
    assert ext4["native_linux_fs"] and not ext4["prjquota"]


def test_writable_by_owner_group_and_world() -> None:
    def entry(mode, uid, gid):
        return types.SimpleNamespace(st_mode=stat.S_IFREG | mode, st_uid=uid, st_gid=gid)

    business = frozenset({1500})
    assert sa.writable_by(entry(0o002, 0, 0), 1500, business)
    assert sa.writable_by(entry(0o700, 1500, 1500), 1500, business)
    assert sa.writable_by(entry(0o770, 0, 1500), 1500, business)
    assert not sa.writable_by(entry(0o750, 0, 1500), 1500, business)
    assert not sa.writable_by(entry(0o700, 0, 0), 1500, business)


@pytest.mark.skipif(os.name != "posix", reason="需要posix的uid/权限语义")
def test_audit_path_flags_symlink_world_writable_and_owner(tmp_path) -> None:
    business_uid = os.getuid()
    open_root = tmp_path / "worldwritable"
    open_root.mkdir()
    open_root.chmod(0o777)
    target = open_root / "code"
    target.mkdir()
    problems = sa.audit_path(
        target, owner_uid=business_uid, private_mode=False,
        business_uid=business_uid, business_gids=frozenset(),
    )
    assert any("业务用户" in problem for problem in problems)

    link = tmp_path / "link"
    link.symlink_to(tmp_path)
    problems = sa.audit_path(
        link / "x", owner_uid=0, private_mode=False,
        business_uid=business_uid, business_gids=frozenset(),
    )
    assert any("symlink" in problem for problem in problems)

    private = tmp_path / "private"
    private.mkdir()
    private.chmod(0o755)
    problems = sa.audit_path(
        private, owner_uid=0, private_mode=True,
        business_uid=None, business_gids=frozenset(),
    )
    assert any("过宽" in problem for problem in problems)


@pytest.mark.skipif(os.name != "posix", reason="配置加载要求posix")
def test_configuration_check_passes_private_file_and_rejects_open_mode(tmp_path) -> None:
    config = tmp_path / "sandbox.json"
    config.write_text(json.dumps({"schema_version": 1}), encoding="utf-8")
    config.chmod(0o600)
    assert sa.check_sandbox_configuration(config)["status"] == "pass"
    config.chmod(0o644)
    assert sa.check_sandbox_configuration(config)["status"] == "fail"


def test_admin_image_pattern_matches_catalog_digest_rules() -> None:
    assert sa.IMAGE_PATTERN.fullmatch("sha256:" + "a" * 64)
    assert sa.IMAGE_PATTERN.fullmatch("docker.io/library/node@sha256:" + "b" * 64)
    assert not sa.IMAGE_PATTERN.fullmatch("docker.io/library/python:3.12@sha256:" + "b" * 64)
    assert not sa.IMAGE_PATTERN.fullmatch("registry.example.com/python@sha256:" + "b" * 64)


def test_preflight_reports_unprovisioned_roots_as_unknown(tmp_path) -> None:
        args = argparse.Namespace(
            config=None,
            install_root=tmp_path / "absent-install",
            state_root=tmp_path / "absent-state",
            storage_root=None,
            broker_socket=tmp_path / "absent-dir" / "broker.sock",
            docker_root=tmp_path / "absent-docker",
            containerd_root=tmp_path / "absent-containerd",
            control_uid=None,
            control_gid=None,
            broker_uid=None,
            command_image=None,
            evidence=None,
            min_free_bytes=1 << 30,
            template_dir=None,
        )
        result = sa.run_preflight(args)
        by_name = {entry["check"]: entry for entry in result["checks"]}
        assert by_name["admin_owned_code"]["status"] == "unknown"
        assert by_name["admin_owned_state"]["status"] == "unknown"
        assert by_name["broker_socket_parent"]["status"] == "unknown"
        assert by_name["broker_socket_absent"]["status"] == "pass"
        assert by_name["f_drive_hosting"]["status"] == "unknown"
        assert by_name["deployment_templates"]["status"] == "pass"
        # passed只表示无fail；存在unknown时confirmed必须为false，未检查项不得假通过。
        assert result["passed"] is True
        assert result["confirmed"] is False


def test_preflight_unprovided_broker_uid_and_image_surface_as_unknown(tmp_path) -> None:
    args = argparse.Namespace(
        config=None,
        install_root=None,
        state_root=None,
        storage_root=None,
        broker_socket=None,
        docker_root=None,
        containerd_root=None,
        control_uid=1500,
        control_gid=None,
        broker_uid=None,
        command_image=None,
        evidence=None,
        min_free_bytes=1 << 30,
        template_dir=None,
    )
    result = sa.run_preflight(args)
    by_name = {entry["check"]: entry for entry in result["checks"]}
    assert by_name["broker_runs_as_root"]["status"] == "unknown"
    assert by_name["command_image_fixed"]["status"] == "unknown"
    assert by_name["control_plane_user"]["status"] == "pass"
    assert result["passed"] is True and result["confirmed"] is False


@pytest.mark.skipif(os.name != "posix" or (hasattr(os, "geteuid") and os.geteuid() == 0),
                    reason="需要非root的posix权限语义")
def test_preflight_unreadable_path_reports_unknown_instead_of_crash(tmp_path) -> None:
    sealed = tmp_path / "sealed"
    sealed.mkdir()
    sealed.chmod(0o000)
    try:
        args = argparse.Namespace(
            config=None,
            install_root=None,
            state_root=sealed / "state",
            storage_root=None,
            broker_socket=None,
            docker_root=None,
            containerd_root=None,
            control_uid=None,
            control_gid=None,
            broker_uid=None,
            command_image=None,
            evidence=None,
            min_free_bytes=1 << 30,
            template_dir=None,
        )
        result = sa.run_preflight(args)
    finally:
        sealed.chmod(0o700)
    by_name = {entry["check"]: entry for entry in result["checks"]}
    assert by_name["admin_owned_state"]["status"] == "unknown"
    assert "无法检查" in by_name["admin_owned_state"]["detail"]


def test_check_hosting_unknown_without_resolvable_roots_and_pass_counts_them() -> None:
    evidence = {
        "distro_name": "Krim-Ubuntu-24.04",
        "base_path_windows": "F:\\WSL\\dev",
        "drive_letter": "F",
        "source": "windows-registry",
    }
    assert sa.check_hosting({}, evidence, [])["status"] == "unknown"
    native = {
        "fstype": "xfs",
        "options": ["rw", "prjquota"],
        "source": "/dev/loop0",
        "native_linux_fs": True,
        "windows_or_network_fs": False,
    }
    verdict_pass = sa.check_hosting({"storage_root": native, "docker_root": None}, evidence, [])
    assert verdict_pass["status"] == "pass"
    assert "1" in verdict_pass["detail"]


def test_backup_plan_omits_secret_contents_and_names(fixture_roots) -> None:
    plan = _fixture_plan(fixture_roots)
    rendered = json.dumps(plan, ensure_ascii=False)
    assert MASTER_SECRET not in rendered
    assert "tok-abc123" not in rendered
    assert "credentials.json" not in rendered
    secrets_target = next(t for t in plan["targets"] if t["kind"] == "secrets")
    assert secrets_target["files"] == 1
    assert secrets_target["master_key_files_excluded"] == 1
    assert secrets_target["names_listed"] is False
    assert plan["quiesce"]["required"]


def test_mask_secrets_masks_string_values_under_secret_keys() -> None:
    source = {"api_key": "k1", "nested": {"token": "t", "count": 1}, "note": "keep"}
    expected = {
        "api_key": "<redacted>",
        "nested": {"token": "<redacted>", "count": 1},
        "note": "keep",
    }
    assert sa.mask_secrets(source) == expected


def test_sqlite_backup_captures_active_wal_where_plain_cp_stales(tmp_path) -> None:
    db = tmp_path / "control.sqlite3"
    writer = sqlite3.connect(db)
    writer.execute("CREATE TABLE t (v TEXT)")
    writer.commit()
    writer.execute("PRAGMA journal_mode=WAL")
    writer.execute("INSERT INTO t VALUES ('live')")
    writer.commit()

    stale = tmp_path / "plain-copy.sqlite3"
    shutil.copyfile(db, stale)
    plain = sqlite3.connect(stale)
    try:
        assert plain.execute("SELECT count(*) FROM t").fetchone()[0] == 0
    finally:
        plain.close()

    destination = tmp_path / "backup.sqlite3"
    sa.sqlite_backup(db, destination)
    restored = sqlite3.connect(destination)
    try:
        assert restored.execute("SELECT v FROM t").fetchone()[0] == "live"
    finally:
        restored.close()
    writer.close()


def test_execute_synthetic_backup_manifest_sealed_secrets_and_exclusions(fixture_roots) -> None:
    plan = _fixture_plan(fixture_roots)
    sa.execute_synthetic_backup(plan, backup_root=fixture_roots["backup"])
    manifest_path = fixture_roots["backup"] / "manifest.json"
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    assert "projects/projects/proj-1/content/README.md" in manifest["entries"]
    assert "sqlite/control.sqlite3" in manifest["entries"]
    assert manifest["plan_digest"] == sa.plan_digest(plan)
    assert manifest["entries"]["sqlite/control.sqlite3"]["sha256"] == sa.sha256_file(
        fixture_roots["backup"] / "sqlite" / "control.sqlite3"
    )
    assert manifest["secrets"]["master_key_excluded"] == 1
    assert not (fixture_roots["backup"] / "secrets" / "master.key").exists()
    sealed = fixture_roots["backup"] / "secrets" / "credentials.json"
    assert stat.S_IMODE(sealed.stat().st_mode) == 0o600
    assert stat.S_IMODE(manifest_path.stat().st_mode) == 0o600


def test_execute_adapter_refuses_real_plans_and_real_prefixes(fixture_roots, tmp_path) -> None:
    real_plan = _fixture_plan(fixture_roots) | {"fixture": False}
    with pytest.raises(ValueError, match="合成fixture"):
        sa.execute_synthetic_backup(real_plan, backup_root=tmp_path / "out")
    planted = {
        "fixture": True,
        "kind": "backup",
        "targets": [{"kind": "projects", "source": "/srv/krim-sandbox"}],
        "destination": {"root": str(tmp_path / "out")},
    }
    with pytest.raises(ValueError, match="真实数据根"):
        sa.execute_synthetic_backup(planted, backup_root=tmp_path / "out")


def test_restore_preview_warns_resurrection_newer_data_and_no_bypass(fixture_roots) -> None:
    plan = _fixture_plan(fixture_roots)
    preview = sa.restore_preview(plan, known_schema_versions=frozenset({10}))
    assert preview["authorization"]["accepted"] is False
    assert preview["authorization"]["cli_bypass"] is None
    assert any("复活" in warning for warning in preview["warnings"])
    assert any("stopped proof" in warning for warning in preview["warnings"])

    newer = {**plan, "data_versions": {"control_schema": 11}}
    newer_preview = sa.restore_preview(newer, known_schema_versions=frozenset({10}))
    assert any("不在已知范围" in warning for warning in newer_preview["warnings"])
    assert any("未记录schema" in warning for warning in sa.restore_preview(plan)["warnings"])

    mismatch = sa.restore_preview(plan, expected_digest="0" * 64)
    assert any("digest" in problem for problem in mismatch["problems"])
    assert sa.restore_preview(plan, expected_digest=preview["plan_digest"])["problems"] == []


def test_cli_parser_has_no_restore_bypass_flag() -> None:
    parser = sa.build_parser()
    args = parser.parse_args(["restore-preview", "--plan", "plan.json"])
    assert args.plan == Path("plan.json")
    help_text = parser.format_help()
    assert "--yes" not in help_text
    assert "execute" not in help_text
    with pytest.raises(SystemExit):
        parser.parse_args(["restore-preview", "--plan", "plan.json", "--yes"])


def test_render_substitutes_and_reports_missing_values() -> None:
    values = {
        "@KRIM_STORAGE_DISK@": "/var/lib/krim-sandbox/disks/accounts.xfs",
        "@KRIM_STORAGE_ROOT@": "/srv/krim-sandbox",
    }
    rendered = sa.render_units(values, ["krim-sandbox-data.mount"])
    content = rendered["krim-sandbox-data.mount"]
    assert "@KRIM_" not in content
    assert "What=/var/lib/krim-sandbox/disks/accounts.xfs" in content
    assert "prjquota" in content
    with pytest.raises(ValueError, match="缺少替换值"):
        sa.render_units({}, ["krim-sandbox-data.mount"])


def test_execute_synthetic_backup_refuses_existing_manifest(fixture_roots) -> None:
    plan = _fixture_plan(fixture_roots)
    sa.execute_synthetic_backup(plan, backup_root=fixture_roots["backup"])
    manifest_path = fixture_roots["backup"] / "manifest.json"
    manifest_before = manifest_path.read_text(encoding="utf-8")
    with pytest.raises(ValueError, match="拒绝覆盖"):
        sa.execute_synthetic_backup(plan, backup_root=fixture_roots["backup"])
    assert manifest_path.read_text(encoding="utf-8") == manifest_before


def test_restore_preview_surfaces_root_chain_problems_and_malformed_targets(fixture_roots) -> None:
    plan = _fixture_plan(fixture_roots)
    chained = {
        **plan,
        "root_chain_problems": {"data": ["x: symlink（必须拒绝，不跟随）"], "state": []},
    }
    preview = sa.restore_preview(chained)
    assert any("根链审计问题" in problem for problem in preview["problems"])

    malformed = {**plan, "targets": "secrets"}
    preview = sa.restore_preview(malformed)
    assert any("targets" in problem for problem in preview["problems"])

    bad_versions = {**plan, "data_versions": "unknown"}
    bad_preview = sa.restore_preview(bad_versions)
    assert bad_preview["problems"] == []
    assert any("未记录schema" in warning for warning in bad_preview["warnings"])


def test_main_restore_preview_exit_code_reflects_problems(fixture_roots, tmp_path, capsys) -> None:
    plan = _fixture_plan(fixture_roots)
    plan_path = tmp_path / "plan.json"
    plan_path.write_text(json.dumps(plan), encoding="utf-8")
    good = sa.main(
        ["restore-preview", "--plan", str(plan_path), "--expect-digest", sa.plan_digest(plan)]
    )
    capsys.readouterr()
    mismatch = sa.main(
        ["restore-preview", "--plan", str(plan_path), "--expect-digest", "0" * 64]
    )
    capsys.readouterr()
    assert good == 0 and mismatch == 2


def test_restore_preview_accepts_backup_plan_envelope(fixture_roots, tmp_path, capsys) -> None:
    plan = _fixture_plan(fixture_roots)
    envelope = {
        "command": "backup-plan",
        "plan": plan,
        "plan_digest": sa.plan_digest(plan),
    }
    plan_path = tmp_path / "envelope.json"
    plan_path.write_text(json.dumps(envelope), encoding="utf-8")
    # 信封原样可预览：内层plan一致时无问题，退出码0。
    good = sa.main(["restore-preview", "--plan", str(plan_path)])
    output = json.loads(capsys.readouterr().out)
    assert good == 0
    assert output["preview"]["plan_digest"] == envelope["plan_digest"]
    assert output["preview"]["problems"] == []
    # 内层plan被改而信封digest未更新：必须报不一致。
    tampered = {**envelope, "plan": {**plan, "plan_version": 1, "warnings": ["x"]}}
    plan_path.write_text(json.dumps(tampered), encoding="utf-8")
    tampered_result = sa.main(["restore-preview", "--plan", str(plan_path)])
    output = json.loads(capsys.readouterr().out)
    assert tampered_result == 2
    assert any("信封" in problem for problem in output["preview"]["problems"])


def test_render_output_dir_refuses_overwrite_and_honors_template_dir(tmp_path) -> None:
    values = {
        "@KRIM_STORAGE_DISK@": "/var/lib/krim-sandbox/disks/accounts.xfs",
        "@KRIM_STORAGE_ROOT@": "/srv/krim-sandbox",
    }
    args = argparse.Namespace(
        set=[f"{key}={value}" for key, value in values.items()],
        unit=["krim-sandbox-data.mount"],
        output_dir=tmp_path / "fresh",
        template_dir=None,
    )
    (tmp_path / "fresh").mkdir()
    fresh = sa.run_render(args)
    assert fresh["written_to"] == str(tmp_path / "fresh")
    written = (tmp_path / "fresh" / "krim-sandbox-data.mount").read_text(encoding="utf-8")
    assert "prjquota" in written

    args.output_dir = tmp_path / "occupied"
    (tmp_path / "occupied").mkdir()
    (tmp_path / "occupied" / "krim-sandbox-data.mount").write_text("existing", encoding="utf-8")
    refused = sa.run_render(args)
    assert "拒绝覆盖" in refused["error"]
    occupied_file = tmp_path / "occupied" / "krim-sandbox-data.mount"
    assert occupied_file.read_text(encoding="utf-8") == "existing"

    args.output_dir = tmp_path / "absent-out"
    missing = sa.run_render(args)
    assert "输出目录不存在" in missing["error"]


def test_render_accepts_explicit_template_dir(tmp_path) -> None:
    tdir = tmp_path / "templates"
    tdir.mkdir()
    shutil.copy2(TEMPLATE_DIR / "krim-sandbox-data.mount", tdir / "krim-sandbox-data.mount")
    shutil.copy2(TEMPLATE_DIR / "README.md", tdir / "README.md")
    values = {
        "@KRIM_STORAGE_DISK@": "/var/lib/krim-sandbox/disks/accounts.xfs",
        "@KRIM_STORAGE_ROOT@": "/srv/krim-sandbox",
    }
    rendered = sa.render_units(values, ["krim-sandbox-data.mount"], template_dir=tdir)
    assert "@KRIM_" not in rendered["krim-sandbox-data.mount"]
    with pytest.raises(ValueError, match="模板目录不存在"):
        sa.render_units(values, ["krim-sandbox-data.mount"], template_dir=tmp_path / "absent")


ALL_PLACEHOLDERS = {
    "@KRIM_INSTALL_ROOT@": "/opt/krim",
    "@KRIM_STORAGE_ROOT@": "/srv/krim-sandbox",
    "@KRIM_STORAGE_DISK@": "/var/lib/krim-sandbox/disks/accounts.xfs",
    "@KRIM_STATE_ROOT@": "/var/lib/krim-sandbox/state",
    "@KRIM_BROKER_SOCKET@": "/run/krim-sandbox/broker.sock",
    "@KRIM_SOCKET_DIR@": "/run/krim-sandbox",
    "@KRIM_CONTROL_UID@": "1500",
    "@KRIM_CONTROL_USER@": "svc-krim",
    "@KRIM_CONTROL_GROUP@": "svc-krim",
    "@KRIM_SOCKET_GID@": "1600",
    "@KRIM_COMMAND_IMAGE@": "sha256:" + "a" * 64,
    "@KRIM_RUNTIME_MANIFEST@": "/opt/krim/share/krim/runtime-manifest.json",
    "@KRIM_DB_PATH@": "/var/lib/krim/control.sqlite3",
    "@KRIM_DB_DIR@": "/var/lib/krim",
    "@KRIM_API_PORT@": "8765",
    "@KRIM_WORKSPACE_ROOT@": "/var/lib/krim/workspace",
}


def test_all_units_render_without_leftover_placeholders() -> None:
    rendered = sa.render_units(dict(ALL_PLACEHOLDERS))
    assert set(rendered) == {
        "krim-sandbox-api.service",
        "krim-sandbox-broker.service",
        "krim-sandbox-data.mount",
    }
    for name, content in rendered.items():
        assert "@KRIM_" not in content, name


def test_templates_reference_installed_module_without_machine_paths() -> None:
    broker = (TEMPLATE_DIR / "krim-sandbox-broker.service").read_text(encoding="utf-8")
    api = (TEMPLATE_DIR / "krim-sandbox-api.service").read_text(encoding="utf-8")
    mount = (TEMPLATE_DIR / "krim-sandbox-data.mount").read_text(encoding="utf-8")
    for name, text in (("broker", broker), ("api", api), ("mount", mount)):
        assert "@KRIM_INSTALL_ROOT@" in text or name == "mount", name
        for forbidden in ("/home/", "/mnt/f", "F:\\", "worktree", "krim-venv"):
            assert forbidden not in text, f"{name}: {forbidden}"

    broker_exec = next(line for line in broker.splitlines() if line.startswith("ExecStart="))
    assert "-m krim_agent.sandbox.broker" in broker_exec
    assert "--runtime-manifest @KRIM_RUNTIME_MANIFEST@" in broker_exec
    assert "--allow-dev-runtimes" not in broker_exec
    assert "User=root" in broker
    assert "CAP_SYS_ADMIN" in broker
    assert "ReadWritePaths=@KRIM_STORAGE_ROOT@ @KRIM_STATE_ROOT@ @KRIM_SOCKET_DIR@" in broker

    api_exec = next(line for line in api.splitlines() if line.startswith("ExecStart="))
    assert "/bin/krim serve" in api_exec
    assert "User=@KRIM_CONTROL_USER@" in api
    assert "CapabilityBoundingSet=" in api
    assert not (TEMPLATE_DIR / "krim-sandbox-broker.socket").exists()
    assert "prjquota" in mount and "Type=xfs" in mount

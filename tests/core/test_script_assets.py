"""按需观测bundle仍以构建白名单提供，拒绝任意目录与篡改。"""
import hashlib
import json

import pytest

from krim_agent.script_assets import load_script_assets


def manifest(root, path="assets/observability.js", body=b"export default 1", **updates):
    target = root / path
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_bytes(body)
    row = {"path": path, "size_bytes": len(body), "sha256": hashlib.sha256(body).hexdigest()}
    row.update(updates)
    (root / "script-manifest.json").write_text(json.dumps({"version": 1, "scripts": [row]}))
    return target


def test_legacy_no_manifest_never_discovers_unapproved_js(tmp_path):
    (tmp_path / "private.js").write_text("private")
    assert load_script_assets(tmp_path) == ()


def test_script_assets_validate_declared_bytes(tmp_path):
    target = manifest(tmp_path)
    assets = load_script_assets(tmp_path)
    assert [a.path for a in assets] == ["assets/observability.js"]
    target.write_bytes(b"tampered")
    with pytest.raises(ValueError):
        load_script_assets(tmp_path)


@pytest.mark.parametrize("update", [
    {"path": "../private.js"}, {"path": "assets/../private.js"},
    {"path": "assets/private.js.map"}, {"path": "https://outside/script.js"},
    {"size_bytes": True}, {"sha256": "not-a-hash"},
])
def test_invalid_script_entries_rejected(tmp_path, update):
    manifest(tmp_path)
    content = json.loads((tmp_path / "script-manifest.json").read_text())
    content["scripts"][0].update(update)
    (tmp_path / "script-manifest.json").write_text(json.dumps(content))
    with pytest.raises(ValueError):
        load_script_assets(tmp_path)


def test_duplicate_manifest_keys_rejected(tmp_path):
    (tmp_path / "script-manifest.json").write_text(
        '{"version":1,"version":1,"scripts":[]}', encoding="utf-8")
    with pytest.raises(ValueError):
        load_script_assets(tmp_path)


def test_oversized_script_manifest_rejected_before_parse(tmp_path):
    (tmp_path / "script-manifest.json").write_bytes(b" " * 65537)
    with pytest.raises(ValueError):
        load_script_assets(tmp_path)


def test_script_casefold_duplicates_rejected(tmp_path):
    manifest(tmp_path)
    content = json.loads((tmp_path / "script-manifest.json").read_text())
    content["scripts"].append(content["scripts"][0])
    (tmp_path / "script-manifest.json").write_text(json.dumps(content))
    with pytest.raises(ValueError):
        load_script_assets(tmp_path)


def test_only_manifest_scripts_mounted_with_same_origin_headers(tmp_path):
    from fastapi import FastAPI
    from fastapi.testclient import TestClient

    from krim_agent.api.web import mount_browser_ui

    manifest(tmp_path)
    (tmp_path / "assets" / "private.js").write_text("private")
    app = FastAPI()
    mount_browser_ui(app, root=tmp_path)
    with TestClient(app) as client:
        response = client.get("/ui/assets/observability.js")
        assert response.status_code == 200
        assert "javascript" in response.headers["content-type"]
        assert response.headers["x-content-type-options"] == "nosniff"
        assert "script-src 'self'" in response.headers["content-security-policy"]
        assert client.get("/ui/assets/private.js").status_code == 404
    assert "/ui/assets/observability.js" in app.state.public_ui_paths
    assert "/ui/assets/private.js" not in app.state.public_ui_paths


def test_observation_modules_registered_for_distribution():
    import tomllib
    from pathlib import Path

    root = Path(__file__).resolve().parents[2]
    project = tomllib.loads((root / "pyproject.toml").read_text(encoding="utf-8"))
    shipped = set(project["tool"]["hatch"]["build"]["targets"]["wheel"]["only-include"])
    required = {"src/krim_agent/" + name for name in (
        "context/manifest.py", "observation_views.py", "api/observations.py", "script_assets.py",
    )}
    assert required <= shipped
    assert "src/krim_agent/web/assets" in shipped
    assert "src/krim_agent/web/script-manifest.json" in shipped


def test_script_assets_symlinks_rejected(tmp_path):
    target = manifest(tmp_path)
    real = tmp_path / "real.js"
    target.rename(real)
    try:
        target.symlink_to(real)
    except OSError:
        pytest.skip("symlink not available")
    with pytest.raises(ValueError):
        load_script_assets(tmp_path)

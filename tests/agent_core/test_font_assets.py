"""字体清单校验与同源静态路由回归测试。"""

from __future__ import annotations

import hashlib
import json
from pathlib import Path

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from krim_agent.api import boundary, web
from krim_agent.api.boundary import LocalBoundary
from krim_agent.config import Settings
from krim_agent.font_assets import (
    FontAsset,
    FontManifestError,
    load_font_assets,
    parse_font_manifest,
)
from scripts.verify_distribution import (
    FONT_DIRECTORY_SOURCE,
    FONT_MANIFEST_SOURCE,
    _expand_wheel_files,
)

FONT_PATH = "assets/fonts/geist-latin-normal-abc12345.woff2"
FONT_BYTES = b"wOF2synthetic-font-bytes"


def _record(path: str = FONT_PATH, data: bytes = FONT_BYTES) -> dict[str, object]:
    return {
        "path": path,
        "size_bytes": len(data),
        "sha256": hashlib.sha256(data).hexdigest(),
    }


def _write_manifest(web_root: Path, records: list[dict[str, object]]) -> None:
    web_root.mkdir(parents=True, exist_ok=True)
    (web_root / "font-manifest.json").write_text(
        json.dumps({"version": 1, "fonts": records}), encoding="utf-8"
    )


def _write_font(web_root: Path, path: str = FONT_PATH, data: bytes = FONT_BYTES) -> None:
    target = web_root / path
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_bytes(data)


def test_distribution_expands_only_the_validated_font_inventory(tmp_path: Path) -> None:
    source_file = "src/krim_agent/example.py"
    (tmp_path / source_file).parent.mkdir(parents=True)
    (tmp_path / source_file).write_text("# source\\n", encoding="utf-8")
    (tmp_path / FONT_MANIFEST_SOURCE).parent.mkdir(parents=True)
    (tmp_path / FONT_MANIFEST_SOURCE).write_text("{}\\n", encoding="utf-8")
    _write_font(tmp_path / "src/krim_agent/web")
    extra = tmp_path / "src/krim_agent/web/assets/fonts/unlisted-abc12345.woff2"
    extra.write_bytes(b"wOF2not-listed")
    asset = FontAsset(
        path=FONT_PATH,
        size_bytes=len(FONT_BYTES),
        sha256=hashlib.sha256(FONT_BYTES).hexdigest(),
    )

    expanded = _expand_wheel_files(
        tmp_path,
        [source_file, FONT_MANIFEST_SOURCE, FONT_DIRECTORY_SOURCE],
        (asset,),
    )

    assert expanded == sorted(
        [
            source_file,
            FONT_MANIFEST_SOURCE,
            f"src/krim_agent/web/{FONT_PATH}",
        ]
    )


def test_manifest_parses_finite_woff2_inventory() -> None:
    assets = parse_font_manifest(
        json.dumps({"version": 1, "fonts": [_record()]}).encode("utf-8")
    )

    assert assets == (
        FontAsset(
            path=FONT_PATH,
            size_bytes=len(FONT_BYTES),
            sha256=hashlib.sha256(FONT_BYTES).hexdigest(),
        ),
    )


@pytest.mark.parametrize(
    "path",
    [
        "../outside-abc12345.woff2",
        "/absolute/font-abc12345.woff2",
        "assets/fonts/../font-abc12345.woff2",
        "assets//fonts/font-abc12345.woff2",
        "assets/fonts/font-abc12345.ttf",
        "assets/fonts/font.woff2",
        r"assets\fonts\font-abc12345.woff2",
    ],
)
def test_manifest_rejects_traversal_absolute_duplicate_separator_and_non_woff2(
    path: str,
) -> None:
    with pytest.raises(FontManifestError):
        parse_font_manifest(json.dumps({"version": 1, "fonts": [_record(path)]}).encode())


def test_manifest_rejects_duplicate_json_keys_and_duplicate_paths() -> None:
    with pytest.raises(FontManifestError, match="重复JSON键"):
        parse_font_manifest(b'{"version":1,"version":1,"fonts":[]}')
    with pytest.raises(FontManifestError, match="重复路径"):
        parse_font_manifest(
            json.dumps({"version": 1, "fonts": [_record(), _record()]}).encode()
        )


def test_manifest_requires_exact_inventory_and_matching_woff2_hash(tmp_path: Path) -> None:
    web_root = tmp_path / "web"
    _write_font(web_root)
    _write_manifest(web_root, [_record()])

    assert load_font_assets(web_root)[0].path == FONT_PATH

    extra = web_root / "assets/fonts/extra-abc12345.woff2"
    extra.write_bytes(b"wOF2extra")
    with pytest.raises(FontManifestError, match="额外"):
        load_font_assets(web_root)
    extra.unlink()

    font_file = web_root / FONT_PATH
    font_file.write_bytes(b"wOF2tampered")
    with pytest.raises(FontManifestError, match="大小"):
        load_font_assets(web_root)
    font_file.write_bytes(b"not-a-woff2")
    broken = _record(data=b"not-a-woff2")
    _write_manifest(web_root, [broken])
    with pytest.raises(FontManifestError, match="不是WOFF2"):
        load_font_assets(web_root)


def test_manifest_rejects_missing_font_and_hash_mismatch(tmp_path: Path) -> None:
    web_root = tmp_path / "web"
    _write_manifest(web_root, [_record()])
    with pytest.raises(FontManifestError, match="不存在"):
        load_font_assets(web_root)

    _write_font(web_root)
    wrong_hash = _record() | {"sha256": "0" * 64}
    _write_manifest(web_root, [wrong_hash])
    with pytest.raises(FontManifestError, match="SHA256"):
        load_font_assets(web_root)


def test_legacy_tree_is_supported_only_without_unlisted_fonts(tmp_path: Path) -> None:
    web_root = tmp_path / "web"
    web_root.mkdir()
    assert load_font_assets(web_root) == ()

    (web_root / "assets/fonts").mkdir(parents=True)
    assert load_font_assets(web_root) == ()
    _write_font(web_root)
    with pytest.raises(FontManifestError, match="必须为空"):
        load_font_assets(web_root)


def test_font_symlink_is_rejected(tmp_path: Path) -> None:
    web_root = tmp_path / "web"
    _write_font(web_root)
    font_file = web_root / FONT_PATH
    target = web_root / "real-font.woff2"
    target.write_bytes(FONT_BYTES)
    font_file.unlink()
    try:
        font_file.symlink_to(target)
    except OSError as error:
        pytest.skip(f"当前文件系统不支持创建测试符号链接：{error}")
    _write_manifest(web_root, [_record()])

    with pytest.raises(FontManifestError, match="符号链接"):
        load_font_assets(web_root)


def _font_app(root: Path, font_path: str = FONT_PATH, data: bytes = FONT_BYTES) -> FastAPI:
    _write_font(root, font_path, data)
    _write_manifest(root, [_record(font_path, data)])
    app = FastAPI()
    web.mount_browser_ui(app, root=root)
    app.add_middleware(
        LocalBoundary,
        settings=Settings(database=root / "unused.sqlite", api_key="route-secret"),
    )
    return app


def test_mount_registers_only_manifest_paths_for_get_and_head(tmp_path: Path) -> None:
    app = _font_app(tmp_path / "web")
    route_path = "/ui/" + FONT_PATH
    route = next(item for item in app.routes if getattr(item, "path", None) == route_path)
    assert route.methods == {"GET", "HEAD"}
    assert route_path in app.state.public_ui_paths
    assert route_path not in web.PUBLIC_UI_PATHS
    assert boundary.PUBLIC_UI_PATHS is web.PUBLIC_UI_PATHS
    with TestClient(app, base_url="http://127.0.0.1") as client:
        response = client.get(route_path)
        assert response.status_code == 200
        assert response.content == FONT_BYTES
        assert response.headers["content-type"] == "font/woff2"
        assert "default-src 'self'" in response.headers["content-security-policy"]
        assert client.head(route_path).status_code == 200
        assert client.get("/ui/assets/fonts/unlisted-abc12345.woff2").status_code == 401
        assert client.get("/v2/private").status_code == 401
        assert client.post(route_path).status_code == 401
        assert client.post(
            route_path, headers={"Authorization": "Bearer route-secret"}
        ).status_code == 405


def test_font_public_paths_are_isolated_between_app_instances(tmp_path: Path) -> None:
    other_path = "assets/fonts/geist-mono-normal-def67890.woff2"
    other_bytes = b"wOF2another-font"
    app_a = _font_app(tmp_path / "first")
    with TestClient(app_a, base_url="http://127.0.0.1") as first:
        assert first.get("/ui/" + FONT_PATH).content == FONT_BYTES
        app_b = _font_app(tmp_path / "second", other_path, other_bytes)
        with TestClient(app_b, base_url="http://127.0.0.1") as second:
            assert first.get("/ui/" + FONT_PATH).status_code == 200
            assert second.get("/ui/" + other_path).content == other_bytes
            assert first.get("/ui/" + other_path).status_code == 401
            assert second.get("/ui/" + FONT_PATH).status_code == 401
            assert first.get("/v2/private").status_code == 401
            assert second.get("/v2/private").status_code == 401
        assert first.get("/ui/" + FONT_PATH).status_code == 200

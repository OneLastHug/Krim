"""Downloader verifies fixed bytes in isolated folders; test sources are in memory."""

import hashlib
import io
import json
import sys
from pathlib import Path

import pytest

DEPLOY = Path(__file__).parents[2] / "deployment/development-browser"
sys.path.insert(0, str(DEPLOY))
import prepare_wheels  # noqa: E402


@pytest.fixture
def fake_downloads(tmp_path, monkeypatch):
    sources = {"first.whl": b"first wheel fixture", "second.deb": b"second fixture"}
    specs = [
        {
            "kind": "wheel" if name.endswith("whl") else "deb",
            "filename": name,
            "url": "https://files.pythonhosted.org/" + name,
            "bytes": len(data),
            "sha256": hashlib.sha256(data).hexdigest(),
        }
        for name, data in sources.items()
    ]
    manifest = tmp_path / "worker-downloads.json"
    manifest.write_text(json.dumps(specs))
    monkeypatch.setattr(prepare_wheels, "__file__", str(tmp_path / "prepare_wheels.py"))

    class Opener:
        def open(self, url, timeout):
            assert timeout == 60
            return io.BytesIO(sources[url.rsplit("/", 1)[1]])

    monkeypatch.setattr(prepare_wheels.urllib.request, "build_opener", lambda *args: Opener())
    return sources, specs


def test_downloads_are_per_file_and_hash_checked(tmp_path, fake_downloads):
    sources, specs = fake_downloads
    root = tmp_path / "new-downloads"
    prepare_wheels.fetch_all(root)
    for item in specs:
        kind = {"wheel": "wheelhouse", "deb": "debs"}[item["kind"]]
        directory = root / kind / item["sha256"]
        assert list(directory.iterdir()) == [directory / item["filename"]]
        assert (directory / item["filename"]).read_bytes() == sources[item["filename"]]
    with pytest.raises(ValueError, match="not_empty"):
        prepare_wheels.fetch_all(root)


def test_modified_download_is_never_published(tmp_path, fake_downloads):
    sources, _ = fake_downloads
    sources["first.whl"] = b"wrong same size dat"
    root = tmp_path / "new-downloads"
    with pytest.raises(ValueError, match="download_(size|integrity)_invalid"):
        prepare_wheels.fetch_all(root)
    assert not list(root.rglob("*.whl"))
    assert not list(root.rglob(".download-*"))


def test_downloader_refuses_redirects():
    with pytest.raises(ValueError, match="download_redirect_denied"):
        prepare_wheels.FixedRedirects().redirect_request(
            None, None, 302, "redirect", {}, "https://other"
        )

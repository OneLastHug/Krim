"""Approval-gated download of fixed data only, never installation or execution."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import tempfile
import urllib.request
from pathlib import Path

from build_context import native_path


class FixedRedirects(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        raise ValueError("download_redirect_denied")


def fetch_all(root: Path) -> None:
    native_path(root)
    specs = json.loads(Path(__file__).with_name("worker-downloads.json").read_text())
    if root.is_symlink() or root.exists() and (not root.is_dir() or any(root.iterdir())):
        raise ValueError("download_directory_not_empty")
    root.mkdir(mode=0o700, parents=True, exist_ok=True)
    opener = urllib.request.build_opener(FixedRedirects)
    for item in specs:
        directory = (
            root
            / {"wheel": "wheelhouse", "deb": "debs", "profile": "profile"}[item["kind"]]
            / item["sha256"]
        )
        directory.mkdir(mode=0o700, parents=True, exist_ok=False)
        fd, temporary = tempfile.mkstemp(prefix=".download-", dir=directory)
        h, size = hashlib.sha256(), 0
        try:
            with os.fdopen(fd, "wb") as output, opener.open(item["url"], timeout=60) as response:
                while chunk := response.read(65536):
                    size += len(chunk)
                    if size > item["bytes"]:
                        raise ValueError("download_size_invalid")
                    h.update(chunk)
                    output.write(chunk)
            if size != item["bytes"] or h.hexdigest() != item["sha256"]:
                raise ValueError("download_integrity_invalid")
            os.rename(temporary, directory / item["filename"])
        finally:
            if os.path.exists(temporary):
                os.unlink(temporary)
        print(f"verified {item['filename']} bytes={size}")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--fetch", action="store_true")
    parser.add_argument("--directory", type=Path, required=True)
    args = parser.parse_args()
    if not args.fetch:
        parser.error("network access requires explicit --fetch after approval")
    fetch_all(args.directory)


if __name__ == "__main__":
    main()

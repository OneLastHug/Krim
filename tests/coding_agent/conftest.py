"""Native test paths independent of long pytest case names."""

import tempfile
from pathlib import Path

import pytest


@pytest.fixture
def credential_cache_root(tmp_path_factory):
    # Keep Unix socket paths short while remaining inside pytest's safe root.
    return tmp_path_factory.mktemp("gc")


@pytest.fixture
def xfs_mount_root():
    # This suite creates directories only; it never invokes XFS or stores a DB.
    # The production XFS command path deliberately excludes dot characters.
    with tempfile.TemporaryDirectory(prefix="krim-xfs-", dir="/tmp") as directory:
        yield Path(directory)

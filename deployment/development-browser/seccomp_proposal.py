"""Render a review-only one-rule proposal; never install or select the profile."""

from __future__ import annotations

import argparse
import difflib
import hashlib
import json
from pathlib import Path

EXPECTED = "cc3e61cabda6bbc1e53e54d27ba4d55a9d3be829b6dd1a596f4a7b31b1cc7849"


def proposal(original: bytes) -> bytes:
    if hashlib.sha256(original).hexdigest() != EXPECTED:
        raise ValueError("original_profile_mismatch")
    value = json.loads(original)
    matches = [rule for rule in value["syscalls"] if rule.get("names") == ["chroot"]]
    if len(matches) != 1 or matches[0].get("includes") != {"caps": ["CAP_SYS_CHROOT"]}:
        raise ValueError("original_chroot_rule_mismatch")
    # Only remove the capability condition on the existing chroot rule. This
    # lets Chromium's already-allowed child user namespace use its own chroot;
    # no capability, network, mount, AppArmor or host setting is changed here.
    matches[0]["includes"] = {}
    return (json.dumps(value, indent="\t") + "\n").encode()


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--original", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    if args.output.exists() or args.output.is_symlink():
        raise ValueError("proposal_output_must_be_new")
    original = args.original.read_bytes()
    result = proposal(original)
    args.output.mkdir(mode=0o700, parents=True)
    (args.output / "proposed-seccomp.json").write_bytes(result)
    diff = "".join(
        difflib.unified_diff(
            original.decode().splitlines(keepends=True),
            result.decode().splitlines(keepends=True),
            fromfile="approved-seccomp.json",
            tofile="proposed-seccomp.json",
        )
    )
    (args.output / "proposal.diff").write_text(diff)
    metadata = {
        "state": "review_only_not_applied",
        "original_sha256": EXPECTED,
        "proposed_sha256": hashlib.sha256(result).hexdigest(),
        "only_semantic_change": "Remove includes.caps condition from existing chroot ALLOW rule",
        "unchanged": [
            "cap-drop=ALL",
            "no-new-privileges",
            "network=none",
            "read-only",
            "nonroot UID/GID 1000",
            "Chromium sandbox enabled",
            "all other syscall rules",
        ],
    }
    (args.output / "proposal.json").write_text(json.dumps(metadata, indent=2) + "\n")
    print(json.dumps(metadata, indent=2))


if __name__ == "__main__":
    main()

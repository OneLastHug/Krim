"""Render a single fixed worker container command; never execute Docker here."""

from __future__ import annotations

import hashlib
import re
import stat
from dataclasses import dataclass
from pathlib import Path

from .policy import canonical_origin, trusted_control_origin

# Approved 2026-11-15 and verified with Chromium sandbox: chroot only.
SECCOMP_SHA256 = "f922c7c9bfc1ece0b72b244e56dc85756ded002c837fb46054cfd8b275c74b6b"
DOCKER = ("/usr/bin/docker", "--host=unix:///var/run/docker.sock")
RUN_ROOT = Path("/run/krim-browser")


@dataclass(frozen=True)
class WorkerSpec:
    """All inputs are supplied by trusted I composition, never a model tool."""

    image_id: str
    name: str
    origin: str
    control_origin: str
    socket_path: Path
    ca_path: Path
    ca_sha256: str
    seccomp_path: Path

    def verify(self) -> None:
        if not re.fullmatch(r"sha256:[a-f0-9]{64}", self.image_id):
            raise ValueError("worker_image_invalid")
        if not re.fullmatch(r"krim-browser-[a-f0-9]{32}", self.name):
            raise ValueError("worker_name_invalid")
        canonical_origin(self.origin)
        trusted_control_origin(self.control_origin)
        if not re.fullmatch(r"[a-f0-9]{64}", self.ca_sha256):
            raise ValueError("worker_ca_invalid")
        for path in (self.socket_path, self.ca_path, self.seccomp_path):
            if (
                not path.is_absolute()
                or path.is_symlink()
                or any(part in {"..", "."} for part in path.parts)
                or any(c in str(path) for c in {",", "\n", "\r", "\0"})
            ):
                raise ValueError("worker_path_invalid")
            # Refuse symlink ancestors as well as the final node.
            if any(parent.is_symlink() for parent in path.parents):
                raise ValueError("worker_path_invalid")
        if not self.socket_path.is_relative_to(RUN_ROOT):
            raise ValueError("worker_socket_invalid")
        if not stat.S_ISSOCK(self.socket_path.lstat().st_mode):
            raise ValueError("worker_socket_invalid")
        ca_stat = self.ca_path.stat()
        if not self.ca_path.is_file() or not 0 < ca_stat.st_size <= 65536:
            raise ValueError("worker_ca_invalid")
        # This is a public CA certificate, not a private key. Its single-file
        # read-only bind must be readable by container UID/GID 1000.
        if not (
            (ca_stat.st_uid == 1000 and ca_stat.st_mode & stat.S_IRUSR)
            or (ca_stat.st_gid == 1000 and ca_stat.st_mode & stat.S_IRGRP)
            or ca_stat.st_mode & stat.S_IROTH
        ):
            raise ValueError("worker_ca_unreadable")
        if hashlib.sha256(self.ca_path.read_bytes()).hexdigest() != self.ca_sha256:
            raise ValueError("worker_ca_invalid")
        if hashlib.sha256(self.seccomp_path.read_bytes()).hexdigest() != SECCOMP_SHA256:
            raise ValueError("worker_seccomp_invalid")

    def create_args(self) -> tuple[str, ...]:
        self.verify()
        return (
            "create",
            "--name=" + self.name,
            "--pull=never",
            "--runtime=runc",
            "--user=1000:1000",
            "--cap-drop=ALL",
            "--security-opt=no-new-privileges",
            "--security-opt=seccomp=" + str(self.seccomp_path),
            "--network=none",
            "--read-only",
            "--init",
            "--restart=no",
            "--interactive",
            "--log-driver=none",
            "--stop-timeout=1",
            "--no-healthcheck",
            "--memory=1073741824",
            "--memory-swap=1073741824",
            "--cpus=2",
            "--pids-limit=128",
            "--shm-size=134217728",
            "--ipc=private",
            "--ulimit=nofile=256:256",
            "--tmpfs=/tmp:rw,nosuid,nodev,size=134217728,mode=1777",
            "--tmpfs=/tmp/krim-home:rw,nosuid,nodev,size=33554432,uid=1000,gid=1000,mode=700",
            "--mount=type=bind,source="
            + str(self.socket_path)
            + ",target=/run/krim-preview/bridge.sock,readonly,bind-propagation=rprivate",
            "--mount=type=bind,source="
            + str(self.ca_path)
            + ",target=/run/krim-ca/preview-ca.pem,readonly,bind-propagation=rprivate",
            "--env=KRIM_PREVIEW_ORIGIN=" + self.origin,
            "--env=KRIM_CONTROL_ORIGIN=" + self.control_origin,
            "--env=KRIM_CA_SHA256=" + self.ca_sha256,
            "--env=HOME=/tmp/krim-home",
            self.image_id,
        )

    def remove_args(self, container_id: str) -> tuple[str, ...]:
        # Remove only the exact instance returned by create; a reused name is
        # not sufficient evidence of the worker the caller owns.
        if not re.fullmatch(r"[a-f0-9]{64}", container_id):
            raise ValueError("worker_instance_invalid")
        return ("rm", "--force", container_id)

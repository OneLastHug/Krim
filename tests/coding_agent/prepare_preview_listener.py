"""Prepare an isolated TLS/API smoke instance; starts no server or container."""

from __future__ import annotations

import argparse
import asyncio
import ipaddress
import json
import os
from datetime import UTC, datetime, timedelta
from pathlib import Path

from cryptography import x509
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.x509.oid import NameOID

from krim_agent.accounts.service import IdentityService
from krim_agent.development.configuration import (
    DevelopmentConfiguration,
    DevelopmentListenerConfiguration,
    DevelopmentPreviewConfiguration,
)
from krim_agent.sandbox.configuration import SandboxConfiguration
from krim_agent.storage.sqlite import SQLiteStore


def write_private(path: Path, data: bytes) -> None:
    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    with os.fdopen(fd, "wb") as output:
        output.write(data)


def certificates(root: Path) -> None:
    key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    name = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, "Krim isolated preview test")])
    now = datetime.now(UTC)
    cert = (x509.CertificateBuilder().subject_name(name).issuer_name(name)
            .public_key(key.public_key()).serial_number(x509.random_serial_number())
            .not_valid_before(now - timedelta(minutes=5)).not_valid_after(now + timedelta(days=1))
            .add_extension(x509.SubjectAlternativeName([
                x509.IPAddress(ipaddress.ip_address("127.0.0.1")),
                x509.DNSName("control.example"), x509.DNSName("*.preview.example"),
            ]), critical=False).sign(key, hashes.SHA256()))
    write_private(root / "tls.key", key.private_bytes(
        serialization.Encoding.PEM, serialization.PrivateFormat.PKCS8,
        serialization.NoEncryption(),
    ))
    write_private(root / "tls.crt", cert.public_bytes(serialization.Encoding.PEM))


async def prepare(root: Path, image: str, control_port: int, preview_port: int) -> None:
    await asyncio.to_thread(root.mkdir, mode=0o700, parents=False, exist_ok=False)
    await asyncio.to_thread(certificates, root)
    database = root / "application.sqlite"
    store = await SQLiteStore.open(database)
    identities = None
    try:
        identities = await IdentityService.open(store)
        issued = await identities.bootstrap("Preview listener test", request_key="bootstrap")
        write_private(root / "test-token", str(issued.token).encode())
    finally:
        if identities is not None:
            await identities.close()
        await store.close()
    sandbox = root / "sandbox.json"
    write_private(sandbox, SandboxConfiguration(
        broker_socket=root / "unused-broker.sock",
    ).model_dump_json().encode())
    await asyncio.to_thread((root / "environments").mkdir, mode=0o700)
    listener = DevelopmentListenerConfiguration(
        control_port=control_port, preview_port=preview_port,
        control_certificate=root / "tls.crt", control_private_key=root / "tls.key",
        preview_certificate=root / "tls.crt", preview_private_key=root / "tls.key",
    )
    development = DevelopmentConfiguration(
        image_id=image, broker_environments_root=root / "environments",
        private_state_root=root / "private", preview=DevelopmentPreviewConfiguration(
            control_origin="https://control.example", control_cookie_domain="control.example",
            preview_domain="preview.example", listener=listener,
        ),
    )
    write_private(root / "development.json", development.model_dump_json().encode())
    print(json.dumps({"prepared": True, "control_port": control_port,
                      "preview_port": preview_port, "containers_started": 0}))


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", required=True, type=Path)
    parser.add_argument("--image", required=True)
    parser.add_argument("--control-port", default=5194, type=int)
    parser.add_argument("--preview-port", default=5195, type=int)
    args = parser.parse_args()
    asyncio.run(prepare(args.root, args.image, args.control_port, args.preview_port))

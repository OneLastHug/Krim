"""Ephemeral sandbox-only CA. The host never trusts this certificate."""
from __future__ import annotations

from datetime import UTC, datetime, timedelta

from krim_agent.project_access.egress import ProjectEgressPolicy


def certificate(policy: ProjectEgressPolicy, seconds: float) -> tuple[str, str]:
    # cryptography is already an installed project extra. Absence fails closed;
    # this module never installs a package or changes either host trust store.
    from cryptography import x509
    from cryptography.hazmat.primitives import hashes, serialization
    from cryptography.hazmat.primitives.asymmetric import rsa
    from cryptography.x509.oid import NameOID

    now = datetime.now(UTC)
    key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    name = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, "Krim download session")])
    hosts = sorted({policy.validate_target(f"https://{h}:{policy.grant.ports[0]}/").host
                    for h in policy.grant.hosts})
    cert = (x509.CertificateBuilder().subject_name(name).issuer_name(name)
            .public_key(key.public_key()).serial_number(x509.random_serial_number())
            .not_valid_before(now - timedelta(seconds=30))
            .not_valid_after(now + timedelta(seconds=seconds + 30))
            .add_extension(x509.BasicConstraints(ca=True, path_length=0), critical=True)
            .add_extension(x509.SubjectAlternativeName([x509.DNSName(h) for h in hosts]),
                           critical=False)
            .sign(key, hashes.SHA256()))
    pem = cert.public_bytes(serialization.Encoding.PEM).decode("ascii")
    private = key.private_bytes(serialization.Encoding.PEM,
                                serialization.PrivateFormat.PKCS8,
                                serialization.NoEncryption()).decode("ascii")
    return pem, private

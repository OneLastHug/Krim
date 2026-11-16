"""Fixed browser-component fixture for the managed real-preview runner."""

from __future__ import annotations

import asyncio
import base64
import hashlib
import ipaddress
import json
import struct
import time
import zlib
from datetime import UTC, datetime, timedelta
from pathlib import Path

import httpx
from cryptography import x509
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.x509.oid import ExtendedKeyUsageOID, NameOID

from krim_agent.browser.protocol import validate_png
from krim_agent.development.browser_worker import BrowserWorkers
from krim_agent.development.configuration import DevelopmentBrowserConfiguration
from tests.coding_agent.prepare_preview_listener import write_private
from tests.coding_agent.verify_sqlite_terminal import RecordingDocker

IMAGE = "sha256:392fdb1ba2a7d9c4853a9483ceecad25db2d7db6253497b38c3ba5ce9d0ec7b4"
PROFILE = Path("/opt/krim-development-browser/v1.61.0/seccomp-chroot.json")
PAGE = '''<!doctype html><html lang="en"><meta charset="utf-8"><title>Browser component test</title>
<style>body{font:18px sans-serif;max-width:720px;margin:40px}
input,button{font:inherit;margin:8px}</style>
<h1>Browser component test</h1><label>Value <input id="value"></label>
<button id="apply">Apply</button><p id="result">Waiting</p>
<label>Fixture file <input id="file" type="file"></label><p id="upload">No file</p>
<script>
document.querySelector('#apply').onclick=()=>{document.querySelector('#result').textContent=
 document.querySelector('#value').value;console.error('fixed component diagnostic');};
document.querySelector('#file').onchange=async()=>{const f=document.querySelector('#file').files[0];
 const r=await fetch('/upload',{method:'POST',
 headers:{'Content-Type':'application/octet-stream'},body:f});
 document.querySelector('#upload').textContent=await r.text();};
</script></html>'''
PEER = '''from http.server import BaseHTTPRequestHandler,ThreadingHTTPServer
import hashlib
PAGE = ''' + repr(PAGE.encode()) + '''
class Handler(BaseHTTPRequestHandler):
 def log_message(self,*args): pass
 def send(self,status,data,mime,cookie=False):
  self.send_response(status);self.send_header('Content-Type',mime)
  if cookie: self.send_header('Set-Cookie','fixture-session=active; Path=/; Secure; SameSite=Strict')
  self.send_header('Content-Length',str(len(data)));self.end_headers();self.wfile.write(data)
 def do_GET(self):
  if self.path=='/': self.send(200,PAGE,'text/html; charset=utf-8',cookie=True)
  elif self.path=='/private-binary':
   if 'fixture-session=active' not in self.headers.get('Cookie',''):
    self.send(401,b'fixture login required','text/plain');return
   self.send(200,bytes(range(256))*2000,'application/octet-stream')
  elif self.path=='/anonymous-binary':
   if self.headers.get('Cookie',''):
    self.send(400,b'expected no application cookie','text/plain');return
   self.send(200,bytes(range(256))*2000,'application/octet-stream')
  elif self.path=='/binary': self.send(200,bytes(range(256))*2000,'application/octet-stream')
  else: self.send(404,b'not found','text/plain')
 def do_POST(self):
  length=int(self.headers.get('Content-Length','0'))
  if self.path!='/upload' or not 0<length<=1048576:
   self.send(400,b'invalid','text/plain');return
  data=self.rfile.read(length)
  open('/workspace/upload-proof','w').write(hashlib.sha256(data).hexdigest())
  self.send(200,('uploaded:'+str(len(data))).encode(),'text/plain')
server=ThreadingHTTPServer(('127.0.0.1',8123),Handler)
print('protocol-ready',flush=True)
server.serve_forever()
'''


def certificates(root: Path) -> None:
    """Replace this fresh fixture's leaf with a dedicated CA-signed certificate."""
    ca_key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    ca_name = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, "Krim browser fixture CA")])
    now = datetime.now(UTC)
    ca = (x509.CertificateBuilder().subject_name(ca_name).issuer_name(ca_name)
          .public_key(ca_key.public_key()).serial_number(x509.random_serial_number())
          .not_valid_before(now - timedelta(minutes=5)).not_valid_after(now + timedelta(days=1))
          .add_extension(x509.BasicConstraints(ca=True, path_length=0), critical=True)
          .add_extension(x509.KeyUsage(digital_signature=True, content_commitment=False,
                                      key_encipherment=False, data_encipherment=False,
                                      key_agreement=False, key_cert_sign=True, crl_sign=True,
                                      encipher_only=None, decipher_only=None), critical=True)
          .sign(ca_key, hashes.SHA256()))
    key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    leaf = (x509.CertificateBuilder().subject_name(x509.Name([
                x509.NameAttribute(NameOID.COMMON_NAME, "preview.example")]))
            .issuer_name(ca_name).public_key(key.public_key())
            .serial_number(x509.random_serial_number())
            .not_valid_before(now - timedelta(minutes=5)).not_valid_after(now + timedelta(days=1))
            .add_extension(x509.BasicConstraints(ca=False, path_length=None), critical=True)
            .add_extension(x509.ExtendedKeyUsage([ExtendedKeyUsageOID.SERVER_AUTH]), critical=False)
            .add_extension(x509.SubjectAlternativeName([
                x509.IPAddress(ipaddress.ip_address("127.0.0.1")),
                x509.DNSName("control.example"), x509.DNSName("*.preview.example"),
            ]), critical=False).sign(ca_key, hashes.SHA256()))
    ca_data = ca.public_bytes(serialization.Encoding.PEM)
    write_private(root / "browser-ca.pem", ca_data)
    (root / "browser-ca.pem").chmod(0o644)
    # These two paths were generated by directories() in this same new fixture.
    for filename in ("tls.key", "tls.crt"):
        assert (root / filename).is_file() and not (root / filename).is_symlink()
    (root / "tls.key").write_bytes(key.private_bytes(
        serialization.Encoding.PEM, serialization.PrivateFormat.PKCS8,
        serialization.NoEncryption(),
    ))
    (root / "tls.crt").write_bytes(leaf.public_bytes(serialization.Encoding.PEM) + ca_data)


def png() -> bytes:
    def chunk(kind, payload):
        return struct.pack("!I", len(payload)) + kind + payload + struct.pack(
            "!I", zlib.crc32(kind + payload) & 0xffffffff,
        )
    return (b"\x89PNG\r\n\x1a\n" + chunk(b"IHDR", struct.pack("!2I5B", 1, 1, 8, 2, 0, 0, 0))
            + chunk(b"IDAT", zlib.compress(b"\x00\x24\x66\xaa")) + chunk(b"IEND", b""))


async def verify(app, root, process, grant, actor, stop):
    import ssl

    service = app.development
    workers = None
    observations = []
    report = {"passed": False, "observations": observations, "application_acceptance": False}

    def record(step, **values):
        observations.append({"step": step, **values})
        (root / "browser-verification.json").write_text(json.dumps(report, indent=2))
        print(json.dumps(observations[-1]), flush=True)

    try:
        context = ssl.create_default_context(cafile=str(root / "browser-ca.pem"))
        async with httpx.AsyncClient(verify=context, trust_env=False, timeout=2) as client:
            async with asyncio.timeout(20):
                while True:
                    try:
                        response = await client.get("https://127.0.0.1:5196/healthz")
                        if response.status_code == 200:
                            break
                    except httpx.TransportError:
                        pass
                    await asyncio.sleep(0.1)
        handle = await service.register_preview(
            actor, process.environment.project_id, process.process_id, 8123,
            request_key="browser-preview",
        )
        handle = await service.inspect_preview(actor, handle.preview_id, wait_seconds=10)
        assert handle.state == "ready"
        cfg = DevelopmentBrowserConfiguration(
            image_id=IMAGE, ca_path=root / "browser-ca.pem",
            ca_sha256=hashlib.sha256((root / "browser-ca.pem").read_bytes()).hexdigest(),
            seccomp_path=PROFILE,
        )
        workers = BrowserWorkers(service.preview_app, cfg)
        workers.runner = RecordingDocker(root / "created-browser-ids.json")
        opened = await workers.open(
            actor, process.environment, process.ownership, handle.preview_id,
        )
        browser_id = opened["browser_id"]
        record("browser-ready", isolated=True, tls_verified=True)

        async def action(body):
            return await workers.action(browser_id, actor, process.environment,
                                        process.ownership, body)

        await action({"op": "goto", "path": "/"})
        await action({"op": "fill", "selector": "#value", "value": "actual-browser-ok"})
        await action({"op": "click", "selector": "#apply"})
        await action({"op": "assert", "selector": "#result", "text": "actual-browser-ok"})
        record("fill-click-dom", matched=True)
        image = png()
        await action({"op": "upload", "selector": "#file", "name": "fixture.png",
                      "media_type": "image/png", "data_base64": base64.b64encode(image).decode()})
        await action({"op": "wait", "selector": "#upload", "state": "visible"})
        # A fixed selector only matches once the page's actual POST has completed.
        await action({"op": "wait", "selector": f"#upload:text-is('uploaded:{len(image)}')",
                      "state": "visible"})
        await action({"op": "assert", "selector": "#upload", "text": "uploaded:" + str(len(image))})
        record("actual-upload", bytes=len(image), sha256=hashlib.sha256(image).hexdigest())
        screenshot = await action({"op": "screenshot"})
        data = base64.b64decode(screenshot["data_base64"], validate=True)
        validate_png(data)
        assert len(data) == screenshot["size"]
        assert hashlib.sha256(data).hexdigest() == screenshot["sha256"]
        write_private(root / "browser-component.png", data)
        record("screenshot", bytes=len(data), sha256=screenshot["sha256"])
        await action({"op": "resize", "preset": "mobile"})
        narrow = await action({"op": "screenshot"})
        narrow_bytes = base64.b64decode(narrow["data_base64"], validate=True)
        validate_png(narrow_bytes)
        assert struct.unpack("!II", narrow_bytes[16:24]) == (375, 812)
        write_private(root / "browser-component-mobile.png", narrow_bytes)
        await action({"op": "resize", "preset": "desktop"})
        record("fixed-viewport", mobile=[375, 812], desktop=[1024, 768])
        expected_hash = hashlib.sha256(bytes(range(256)) * 2000).hexdigest()
        for path, mode, status in (("/private-binary", "context", 200),
                                   ("/private-binary", "preview_only", 401),
                                   ("/anonymous-binary", "preview_only", 200)):
            downloaded = await action({"op": "download", "path": path, "cookie_mode": mode})
            assert set(downloaded) == {"status", "media_type", "size", "sha256"}
            assert downloaded["status"] == status
            if status == 200:
                assert downloaded["size"] == 512000 and downloaded["sha256"] == expected_hash
            record("bounded-download", cookie_mode=mode, **downloaded)
        diagnostic = await action({"op": "diagnostics"})
        assert diagnostic["events"].get("console_error", 0) >= 1
        record("console-diagnostics", **diagnostic)
        await service.grant_project(actor, process.environment.project_id,
                                    expected_revision=grant["revision"],
                                    expires_at=time.time() + 0.5, requires_approval=False)
        owned = workers.sessions[browser_id]
        await asyncio.wait_for(owned.task, 8)
        assert not workers.sessions
        record("expiry-closes-browser", reclaimed=True)
        report["passed"] = True
    except BaseException as error:
        report["error_type"] = type(error).__name__
        report["error_code"] = getattr(error, "code", "verification_failed")
        raise
    finally:
        try:
            if workers is not None:
                await workers.close()
                report["browser_cleanup"] = not workers.sessions
        finally:
            (root / "browser-verification.json").write_text(json.dumps(report, indent=2))
            stop.set()

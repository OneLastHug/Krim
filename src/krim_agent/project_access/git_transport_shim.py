"""Fixed Git-only loopback HTTP adapter shipped through Docker exec stdin.

Uses the stdlib HTTP server. Git handles smart-HTTP and pack negotiation. The
loopback origin carries no credential; only the host performs authenticated TLS.
No CONNECT, proxy forwarding or arbitrary URL is implemented here.
"""

from __future__ import annotations

import base64
import json
import sys
from http.server import BaseHTTPRequestHandler, HTTPServer


def main() -> None:
    source = sys.stdin.buffer
    destination = sys.stdout.buffer
    config = json.loads(source.readline(16384))
    maximum = int(config["max_bytes"])
    request_id = 0

    class Handler(BaseHTTPRequestHandler):
        protocol_version = "HTTP/1.1"

        def log_message(self, format: str, *args: object) -> None:
            pass

        def do_GET(self) -> None:
            self.handle_git()

        def do_POST(self) -> None:
            self.handle_git()

        def handle_git(self) -> None:
            nonlocal request_id
            self.connection.settimeout(30)
            try:
                if self.headers.get("Transfer-Encoding") is not None:
                    raise ValueError
                length = int(self.headers.get("Content-Length", "0"))
                if length < 0 or length > maximum:
                    raise ValueError
                host = self.headers.get_all("Host", [])
                if host != [f"127.0.0.1:{server.server_port}"]:
                    raise ValueError
                body = self.rfile.read(length)
                if len(body) != length:
                    raise ValueError
                request_id += 1
                if not self.path.startswith("/repo.git/"):
                    raise ValueError
                payload = {
                    "id": request_id,
                    "method": self.command,
                    "path": self.path[len("/repo.git") :],
                    "headers": {k.lower(): v for k, v in self.headers.items()},
                    "body": base64.b64encode(body).decode("ascii"),
                }
                destination.write(json.dumps(payload, separators=(",", ":")).encode() + b"\n")
                destination.flush()
                raw = source.readline(maximum * 2 + 65536)
                result = json.loads(raw)
                if result["id"] != request_id:
                    raise ValueError
                data = base64.b64decode(result["body"], validate=True)
                if len(data) > maximum:
                    raise ValueError
                self.send_response(int(result["status"]))
                for name, value in result["headers"].items():
                    if name.lower() not in {"content-type", "cache-control", "pragma", "expires"}:
                        raise ValueError
                    if "\r" in value or "\n" in value:
                        raise ValueError
                    self.send_header(name, value)
                self.send_header("Content-Length", str(len(data)))
                self.send_header("Connection", "close")
                self.end_headers()
                self.wfile.write(data)
                self.close_connection = True
            except Exception:
                self.close_connection = True
                try:
                    self.send_error(403)
                except OSError:
                    pass

    server = HTTPServer(("127.0.0.1", 0), Handler)
    destination.write(json.dumps({"ready": server.server_port}).encode() + b"\n")
    destination.flush()
    server.serve_forever(poll_interval=0.1)


if __name__ == "__main__":
    main()

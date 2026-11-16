"""Check the approved network namespace using reserved documentation targets."""

import errno
import json
import socket
from pathlib import Path


def main() -> None:
    interfaces = sorted(p.name for p in Path("/sys/class/net").iterdir())
    routes = Path("/proc/net/route").read_text().splitlines()
    assert interfaces == ["lo"] and len(routes) == 1
    result = {"interfaces": interfaces, "ipv4_routes": routes, "probes": []}
    # TEST-NET address: there is no route and no real public/third-party endpoint.
    for protocol, kind, port in (
        ("tcp", socket.SOCK_STREAM, 443),
        ("udp", socket.SOCK_DGRAM, 443),
        ("dns_udp", socket.SOCK_DGRAM, 53),
    ):
        with socket.socket(socket.AF_INET, kind) as sock:
            sock.settimeout(0.5)
            try:
                if kind == socket.SOCK_STREAM:
                    sock.connect(("192.0.2.1", port))
                else:
                    sock.sendto(b"fixed-network-isolation-probe", ("192.0.2.1", port))
            except OSError as exc:
                assert exc.errno in {errno.ENETUNREACH, errno.EHOSTUNREACH, errno.EACCES}
                result["probes"].append(
                    {
                        "protocol": protocol,
                        "port": port,
                        "errno": exc.errno,
                        "name": errno.errorcode[exc.errno],
                    }
                )
            else:
                raise RuntimeError("unexpected_network_access")
    print(json.dumps({"ok": True, "result": result}), flush=True)


main()

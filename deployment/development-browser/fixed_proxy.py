"""Compatibility import of the sole fixed proxy implementation."""

from krim_agent.dev_browser.fixed_proxy import (
    FixedProxy,
    ProxyError,
    check_authority,
    close_writer,
    fixed_host,
    read_headers,
    relay,
    serve_connection,
)

__all__ = [
    "FixedProxy",
    "ProxyError",
    "check_authority",
    "close_writer",
    "fixed_host",
    "read_headers",
    "relay",
    "serve_connection",
]

"""Project policy validation for B's actual egress enforcement point."""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass, field
from unicodedata import category
from urllib.parse import urlsplit, urlunsplit

from krim_agent.network.egress_policy import EgressPolicy, EgressTarget, EgressViolation
from krim_agent.project_access.types import NetworkGrant


@dataclass(frozen=True, slots=True)
class ProjectEgressPolicy:
    """A versioned policy value, NOT a container firewall or authorization lease.

    The caller must re-resolve revocation, validate each redirect, check all DNS
    addresses, pin the socket to an approved address and verify TLS host identity.
    Registry/CDN domains share a grant; package names do not require approvals.
    """

    grant: NetworkGrant
    _policy: EgressPolicy = field(init=False, repr=False, compare=False)

    def __post_init__(self) -> None:
        # Reuse existing IDNA, exact-host, public-address and port rules.
        policy = EgressPolicy(self.grant.hosts, self.grant.ports)
        for host in self.grant.hosts:
            if any(char in host for char in ("?", "#", "\\", "%", ":", "[", "]")):
                raise EgressViolation("project policy requires DNS host names")
            policy.validate_target(f"https://{host}:{self.grant.ports[0]}/")
        object.__setattr__(self, "_policy", policy)

    def validate_target(self, url: str) -> EgressTarget:
        # Package download URLs may have signed queries; authority validation is
        # still delegated to existing EgressPolicy. Queries never become logs.
        if len(url) > 16384 or any(c.isspace() or category(c) == "Cc" for c in url):
            raise EgressViolation("invalid project egress target")
        try:
            parsed = urlsplit(url)
            if parsed.fragment or "#" in url or "\\" in url:
                raise EgressViolation("invalid project egress target")
            return self._policy.validate_target(
                urlunsplit((parsed.scheme, parsed.netloc, parsed.path, "", ""))
            )
        except (ValueError, EgressViolation):
            raise EgressViolation("project egress target denied") from None

    def approve_addresses(self, addresses: Sequence[str]) -> tuple[str, ...]:
        try:
            return self._policy.approve_addresses(addresses)
        except EgressViolation:
            raise EgressViolation("project egress addresses denied") from None

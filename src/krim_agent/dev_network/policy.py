"""Host-owned, live project policy binding; never a bearer capability."""
from __future__ import annotations

import asyncio
import math
import time
from collections.abc import Awaitable, Callable
from dataclasses import dataclass

from krim_agent.dev_environment.model import EnvironmentError, RuntimeSpec
from krim_agent.project_access.egress import ProjectEgressPolicy

PolicyResolver = Callable[[RuntimeSpec], Awaitable[ProjectEgressPolicy]]


@dataclass(frozen=True)
class PolicyBinding:
    spec: RuntimeSpec
    policy: ProjectEgressPolicy
    resolver: PolicyResolver
    clock: Callable[[], float] = time.time

    async def current(self) -> ProjectEgressPolicy:
        async with asyncio.timeout(5):
            policy = await self.resolver(self.spec)
        grant = policy.grant
        h = self.spec.handle
        if (grant != self.policy.grant or grant.revoked
                or not math.isfinite(grant.expires_at) or grant.expires_at <= self.clock()
                or (grant.owner_id, grant.project_id, grant.policy_id)
                != (h.owner_id, h.project_id, self.spec.network.policy_ref)):
            raise EnvironmentError("network_authorization_expired")
        return policy


async def bind(spec: RuntimeSpec, resolver: PolicyResolver) -> PolicyBinding:
    if spec.network.network_id is not None or spec.network.policy_ref is None:
        raise EnvironmentError("network_binding_invalid")
    async with asyncio.timeout(5):
        policy = await resolver(spec)
    binding = PolicyBinding(spec, policy, resolver)
    await binding.current()
    return binding

"""Combine host-owned tool bundles without replacing an executor's pipeline."""

from __future__ import annotations

from collections.abc import Awaitable, Callable, Iterable

from krim_agent.contracts import ServiceError
from krim_agent.tools.core import Decision, Invocation, Policy, ToolSpec
from krim_agent.tools.executor import ToolExecutor
from krim_agent.tools.registry import ToolRegistry
from krim_agent.tools.session import ToolBundle

ModeGuard = Callable[[Invocation], Awaitable[Decision]]


def policy_spec(registry: ToolRegistry, invocation: Invocation) -> ToolSpec:
    """Find an exact public or bound-target definition for a host policy check."""
    registry.assert_current()
    for root, _ in registry._frozen:
        spec: ToolSpec | None = root
        seen: set[int] = set()
        while spec is not None and id(spec) not in seen:
            seen.add(id(spec))
            if spec.name == invocation.name and spec.fingerprint == invocation.tool_hash:
                return spec
            spec = spec.binding_target
    raise ServiceError("tool_binding_changed")


def compose_executor(
    base: ToolExecutor, bundles: Iterable[ToolBundle], *, guard: ModeGuard | None = None,
) -> ToolExecutor:
    base.registry.assert_current()
    specs = [spec for spec, _ in base.registry._frozen]
    public_names = {spec.name for spec in specs}
    owners: dict[tuple[str, str], list[Policy | None]] = {}

    def register(spec: ToolSpec | None, owner: Policy | None) -> None:
        seen: set[int] = set()
        while spec is not None:
            if id(spec) in seen:
                raise ValueError("cyclic tool binding")
            seen.add(id(spec))
            policies = owners.setdefault((spec.name, spec.fingerprint), [])
            if all(candidate is not owner for candidate in policies):
                policies.append(owner)
            spec = spec.binding_target

    for spec in specs:
        register(spec, base.policy)
    for bundle in bundles:
        for spec in bundle.specs:
            if spec.name in public_names:
                raise ValueError(f"duplicate tool: {spec.name}")
            public_names.add(spec.name)
            register(spec, bundle.policy)
            specs.append(spec)
    registry = ToolRegistry(tuple(specs), dependencies=(base.registry,))

    async def policy(invocation: Invocation) -> Decision:
        policies = owners.get((invocation.name, invocation.tool_hash))
        if not policies:
            return "deny"
        verdict: Decision = "allow"
        for owner in policies:
            if owner is None:
                return "deny"
            decision = await owner(invocation)
            if decision not in {"allow", "ask"}:
                return "deny"
            if decision == "ask":
                verdict = "ask"
        if guard is not None:
            guarded = await guard(invocation)
            if guarded not in {"allow", "ask"}:
                return "deny"
            if guarded == "ask":
                return "ask"
        return verdict

    return ToolExecutor(
        registry, policy, base.pre, base.post, middleware=base.middleware, wrap=base.wrap,
    )

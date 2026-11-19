"""Route identity validation; lifecycle/authorization require real B/E/I tests."""

import pytest
from pydantic import ValidationError

from krim_agent.development.preview_ports import RouteBinding, RouteLease, ServiceInstance


def values() -> dict[str, object]:
    return {
        "preview_id": "preview-a",
        "owner_id": "owner-a",
        "project_id": "project-a",
        "environment_id": "environment-a",
        "generation": 1,
        "service_id": "process-a",
        "process_id": "process-a",
        "operation_id": "operation-a",
        "request_digest": "a" * 64,
        "execution_id": "b" * 64,
        "started_at": "2026-11-15T00:00:00.123456789Z",
        "port": 3000,
        "route_epoch": 1,
    }


def test_binding_round_trip_retains_container_instance_and_epoch() -> None:
    binding = RouteBinding.model_validate(values())
    restored = RouteBinding.model_validate_json(binding.model_dump_json())
    assert restored == binding
    assert restored.started_at == "2026-11-15T00:00:00.123456789Z"
    changed = values() | {"started_at": "2026-11-15T00:00:00.123456790Z"}
    assert RouteBinding.model_validate(changed) != binding
    assert RouteBinding.model_validate(values() | {"route_epoch": 2}) != binding
    lease = RouteLease(binding=binding, lease_id="opaque-host-lease")
    assert not hasattr(lease, "address")


@pytest.mark.parametrize("field,value", [
    ("service_id", "another-service"),
    ("execution_id", "short-container-id"),
    ("execution_id", "b" * 63),
    ("execution_id", "B" * 64),
    ("request_digest", "unbound"),
    ("generation", 0),
    ("route_epoch", 0),
    ("port", 0),
    ("port", 65536),
    ("port", True),
    ("started_at", "2026-11-15"),
    ("started_at", "2026-11-15T00:00:00Z\n"),
    ("address", "127.0.0.1"),
])
def test_route_rejects_ambiguous_or_unbound_target(field: str, value: object) -> None:
    with pytest.raises(ValidationError):
        RouteBinding.model_validate(values() | {field: value})


def test_service_instance_is_exact_and_immutable() -> None:
    instance = ServiceInstance(
        execution_id="c" * 64,
        started_at="2026-11-15T00:00:00Z",
    )
    with pytest.raises(ValidationError):
        instance.started_at = "2026-11-15T00:00:01Z"  # type: ignore[misc]

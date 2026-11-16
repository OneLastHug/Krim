"""Screenshot provenance, private filesystem reads and action validation."""

import hashlib

import pytest
from pydantic import ValidationError

from krim_agent.contracts import ServiceError
from krim_agent.development.browser_artifacts import BrowserArtifacts
from krim_agent.development.contracts import EnvironmentHandle, RunOwnership
from krim_agent.development.service import BrowserActionInput
from tests.coding_agent.browser_combination import png


@pytest.fixture
def saved(tmp_path):
    artifacts = BrowserArtifacts(tmp_path / "artifacts")
    environment = EnvironmentHandle(owner_id="owner", project_id="project",
                                    environment_id="environment", generation=1, state="ready")
    ownership = RunOwnership(owner_id="owner", session_id="session", run_id="run")
    image = png()
    result = artifacts.save(image, environment, ownership, 1, "preview")
    return artifacts, environment, ownership, image, result


def test_screenshot_roundtrip_and_provenance(saved):
    artifacts, environment, ownership, image, result = saved
    assert result["sha256"] == hashlib.sha256(image).hexdigest()
    data, metadata = artifacts.read(result["reference"], environment, ownership, 1)
    assert data == image and metadata["preview_id"] == "preview"
    assert "path" not in result and "data_base64" not in result
    for altered in (environment.model_copy(update={"project_id": "other"}),
                    environment.model_copy(update={"generation": 2})):
        with pytest.raises(ServiceError, match="permission_denied"):
            artifacts.read(result["reference"], altered, ownership, 1)
    with pytest.raises(ServiceError, match="permission_denied"):
        artifacts.read(result["reference"], environment, ownership, 2)
    with pytest.raises(ServiceError, match="not_found"):
        artifacts.read(result["reference"], environment,
                       ownership.model_copy(update={"run_id": "other"}), 1)


@pytest.mark.parametrize("change", ["bytes", "symlink", "hardlink", "permissions"])
def test_artifact_mutation_rejected(saved, change, tmp_path):
    artifacts, environment, ownership, _, result = saved
    path = next(artifacts.root.rglob(result["reference"] + ".png"))
    if change == "bytes":
        path.write_bytes(b"broken")
    elif change == "symlink":
        other = tmp_path / "other.png"
        path.rename(other)
        path.symlink_to(other)
    elif change == "hardlink":
        (tmp_path / "other.png").hardlink_to(path)
    else:
        path.chmod(0o644)
    with pytest.raises(ServiceError, match="artifact_invalid"):
        artifacts.read(result["reference"], environment, ownership, 1)


@pytest.mark.parametrize("values", [
    {"action": "fill", "selector": "#x"},
    {"action": "click", "selector": "#x", "path": "/"},
    {"action": "assert", "selector": "#x"},
    {"action": "diagnostics", "value": "unexpected"},
    {"action": "eval", "value": "anything"},
])
def test_browser_actions_reject_extra_or_missing_fields(values):
    with pytest.raises(ValidationError):
        BrowserActionInput(browser_id="browser", **values)

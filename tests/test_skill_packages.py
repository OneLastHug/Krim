"""Skills文件包的受限解析：来源、许可、路径和容量边界。"""

import asyncio
import base64
import io
import os
import stat
import subprocess
import sys
import zipfile

import pytest

from krim_agent.client import KrimClient
from krim_agent.protocol.domain import ChatRequest, ServiceError
from krim_agent.skill_packages import (
    SkillPackageRequest,
    package_from_archive,
    package_from_directory,
)


def skill(name: str = "review", *, body: str = "检查输入") -> bytes:
    return (
        f"---\nname: {name}\ndescription: 检查技能\nlicense: MIT\n"
        "compatibility: Python 3.12\nallowed-tools: workspace_read\n---\n" + body
    ).encode()


def archive(entries: dict[str, bytes]) -> bytes:
    value = io.BytesIO()
    with zipfile.ZipFile(value, "w", zipfile.ZIP_DEFLATED) as file:
        for name, content in entries.items():
            file.writestr(name, content)
    return value.getvalue()


def test_directory_import_preserves_license_source_digest_and_text_resources(tmp_path):
    root = tmp_path / "review"
    (root / "references").mkdir(parents=True)
    (root / "SKILL.md").write_bytes(skill())
    (root / "references" / "guide.md").write_text("清单", encoding="utf-8")
    definition = package_from_directory(root)
    assert definition.name == "review"
    assert definition.license == "MIT"
    assert definition.metadata["krim-package-source"] == "directory"
    assert definition.metadata["krim-package-format"] == "agent-skills-v1"
    assert definition.resources[0].content == "清单"
    assert len(definition.metadata["krim-package-sha256"]) == 64


def test_zip_import_and_request_digest_are_bounded():
    raw = archive({"SKILL.md": skill(), "references/guide.md": b"zip resource"})
    definition = package_from_archive(raw)
    assert definition.metadata["krim-package-source"] == "zip"
    request = SkillPackageRequest(archive_base64=base64.b64encode(raw).decode())
    assert len(base64.b64decode(request.archive_base64)) == len(raw)


@pytest.mark.parametrize(
    "entries,code",
    [
        (
            {"SKILL.md": b"---\nname: ../escape\ndescription: bad\n---\ntext"},
            "skill_package_definition",
        ),
        (
            {"SKILL.md": b"---\nname: review\ndescription: bad\na: 1\na: 2\n---\ntext"},
            "skill_package_yaml",
        ),
        (
            {"SKILL.md": b"---\nname: review\ndescription: bad\n---\ntext", "a.bin": b"\0"},
            "skill_package_limit",
        ),
    ],
)
def test_zip_rejects_traversal_duplicate_yaml_and_binary(entries, code):
    with pytest.raises(ServiceError) as error:
        package_from_archive(archive(entries))
    assert error.value.code == code


def test_directory_rejects_symlink_and_package_name_mismatch(tmp_path):
    root = tmp_path / "wrong"
    root.mkdir()
    (root / "SKILL.md").write_bytes(skill("review"))
    (root / "link.md").symlink_to(root / "SKILL.md")
    link_path = root / "link.md"
    with pytest.raises(ServiceError) as link:
        package_from_directory(root)
    assert link.value.code == "skill_package_file_type"
    link_path.unlink()
    with pytest.raises(ServiceError) as name:
        package_from_directory(root)
    assert name.value.code == "skill_package_name"


@pytest.mark.parametrize("path", ["../escape.md", "/etc/passwd", "references/../x.md", "x\\y.md"])
def test_archive_rejects_unsafe_relative_keys(path):
    with pytest.raises(ServiceError) as failure:
        package_from_archive(archive({"SKILL.md": skill(), path: b"never read"}))
    assert failure.value.code == "skill_package_path"


@pytest.mark.parametrize(
    "header",
    [
        "name: review\ndescription: !unsafe bad",
        "name: &x review\ndescription: *x",
        "name: review\ndescription: bad\nmetadata: " + "[" * 10 + "x" + "]" * 10,
    ],
)
def test_yaml_complex_features_do_not_construct_objects(header):
    with pytest.raises(ServiceError) as failure:
        package_from_archive(archive({"SKILL.md": ("---\n" + header + "\n---\nbody").encode()}))
    assert failure.value.code == "skill_package_yaml"


@pytest.mark.parametrize("count,size", [(18, 4), (2, 32769)])
def test_zip_file_count_and_expanded_file_capacity_are_enforced(count, size):
    entries = {"SKILL.md": skill()}
    entries.update({f"references/{index}.md": b"x" * size for index in range(count - 1)})
    with pytest.raises(ServiceError) as failure:
        package_from_archive(archive(entries))
    assert failure.value.code in {"skill_package_files", "skill_package_limit"}


def test_directory_hardlink_and_zip_symlink_are_rejected(tmp_path):
    root = tmp_path / "review"
    root.mkdir()
    (root / "SKILL.md").write_bytes(skill())
    os.link(root / "SKILL.md", root / "hard.md")
    with pytest.raises(ServiceError) as hard:
        package_from_directory(root)
    assert hard.value.code == "skill_package_file_type"
    value = io.BytesIO()
    with zipfile.ZipFile(value, "w") as zip_file:
        zip_file.writestr("SKILL.md", skill())
        info = zipfile.ZipInfo("link.md")
        info.external_attr = (stat.S_IFLNK | 0o777) << 16
        zip_file.writestr(info, "../../outside")
    with pytest.raises(ServiceError) as link:
        package_from_archive(value.getvalue())
    assert link.value.code == "skill_package_file_type"


def test_duplicate_zip_names_are_rejected_before_import():
    value = io.BytesIO()
    with zipfile.ZipFile(value, "w") as zip_file:
        zip_file.writestr("SKILL.md", skill())
        with pytest.warns(UserWarning, match="Duplicate name"):
            zip_file.writestr("SKILL.md", skill(body="duplicate"))
    with pytest.raises(ServiceError) as duplicate:
        package_from_archive(value.getvalue())
    assert duplicate.value.code == "skill_package_files"


async def test_repository_package_conflict_explicit_replace_and_frozen_run(repository):
    topic = await repository.create_topic("package")
    original = package_from_archive(archive({"SKILL.md": skill(body="BODY_ONE")}))
    first = await repository.import_skill_package(topic.id, original)
    again = await repository.import_skill_package(topic.id, original)
    assert first == again and len(first.skills) == 1
    run, _ = await repository.begin_run(
        topic.id, ChatRequest(text="inspect", idempotency_key="original", skill_names=["review"])
    )
    changed = package_from_archive(archive({"SKILL.md": skill(body="BODY_TWO")}))
    with pytest.raises(ServiceError) as conflict:
        await repository.import_skill_package(topic.id, changed)
    assert conflict.value.code == "skill_package_conflict"
    updated = await repository.import_skill_package(
        topic.id, changed, expected_digest=first.skills[0].digest
    )
    assert updated.skills[0].digest != first.skills[0].digest
    assert (await repository.run_skill_snapshot(run.id)).skills[0].content == "BODY_ONE"
    assert (await repository.read_skill(topic.id, "review")).content == "BODY_TWO"


async def test_http_archive_import_is_data_only_and_preserves_resources(api):
    topic = (await api.post("/v1/topics", json={"title": "archive"})).json()
    raw = archive({"SKILL.md": skill(), "scripts/hello.py": b"raise RuntimeError('must not run')"})
    response = await api.post(
        f"/v1/topics/{topic['id']}/skills/import-archive",
        json={"archive_base64": base64.b64encode(raw).decode()},
    )
    assert response.status_code == 200
    assert response.json()["skills"][0]["license"] == "MIT"
    read = await api.get(f"/v1/topics/{topic['id']}/skills/review?resource=scripts/hello.py")
    assert read.status_code == 200 and "must not run" in read.json()["content"]
    rejected = await api.post(
        f"/v1/topics/{topic['id']}/skills/import-archive",
        json={"archive_base64": base64.b64encode(raw).decode(), "directory_root": "/etc"},
    )
    assert rejected.status_code == 422


async def test_real_tcp_client_and_cli_import_archive(server_url, tmp_path):
    raw = archive({"SKILL.md": skill(), "references/guide.md": b"CLI_RESOURCE"})
    path = tmp_path / "review.zip"
    path.write_bytes(raw)

    def scenario():
        with KrimClient(server_url) as client:
            topic = client.create_topic("skill CLI")
            result = subprocess.run(
                [
                    sys.executable,
                    "-m",
                    "krim_agent",
                    "skills",
                    "import-archive",
                    topic.id,
                    "--archive",
                    str(path),
                    "--url",
                    server_url,
                ],
                capture_output=True,
                text=True,
                timeout=20,
            )
            assert result.returncode == 0, result.stderr
            assert client.skills(topic.id).skills[0].name == "review"
            assert (
                client.read_skill(topic.id, "review", "references/guide.md").content
                == "CLI_RESOURCE"
            )

    await asyncio.to_thread(scenario)

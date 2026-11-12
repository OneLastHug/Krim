"""Skill 解析器验收：规范文档通过，歧义、危险路径与超限内容明确拒绝。"""

import pytest
from pydantic import ValidationError

from krim_agent.skills import (
    SKILL_BODY_MAX_BYTES,
    SKILL_DESCRIPTION_MAX_CHARS,
    SKILL_METADATA_KEY_MAX_CHARS,
    SKILL_METADATA_MAX_ENTRIES,
    SKILL_METADATA_VALUE_MAX_CHARS,
    SKILL_NAME_MAX_CHARS,
    SKILL_RESOURCE_MAX_BYTES,
    SKILL_RESOURCE_MAX_COUNT,
    SKILL_RESOURCES_TOTAL_MAX_BYTES,
    SKILL_TOTAL_MAX_BYTES,
    SkillDocument,
    SkillParseError,
    SkillSummary,
    parse_skill,
)

_BODY = "## 步骤\n先读目录，再逐节提取结论。\n"


def _frontmatter_text(body: str = _BODY, **fields: str) -> str:
    lines = ["---", *(f"{key}: {value}" for key, value in fields.items()), "---"]
    return "\n".join([*lines, body])


NORMATIVE = {
    "name": "pdf-report",
    "description": "解读月度 PDF 报告并汇总结论",
    "license": "MIT",
    "compatibility": "krim>=0.4",
    "metadata": "{team: finance, version: v2}",
}


def _normative_text() -> str:
    return _frontmatter_text(**NORMATIVE)


def test_normative_document_parses() -> None:
    resources = {"references/format.md": "表格列说明\n", "scripts/说明 v2.md": "示例脚本说明\n"}
    doc = parse_skill(_normative_text(), resources)
    assert doc.name == "pdf-report"
    assert doc.description == "解读月度 PDF 报告并汇总结论"
    assert doc.license == "MIT"
    assert doc.compatibility == "krim>=0.4"
    assert doc.body == _BODY
    assert doc.resources == resources
    assert doc.metadata == {"team": "finance", "version": "v2"}
    assert len(doc.content_hash) == 64
    int(doc.content_hash, 16)


def test_document_is_frozen() -> None:
    doc = parse_skill(_normative_text())
    with pytest.raises(ValidationError):
        doc.name = "other"  # type: ignore[misc]


def test_content_hash_binds_known_content() -> None:
    base = parse_skill(_normative_text(), {"refs/a.md": "A\n"})
    assert parse_skill(_normative_text(), {"refs/a.md": "A\n"}).content_hash == base.content_hash
    first = {"refs/a.md": "A\n", "refs/b.md": "B\n"}
    second = {"refs/b.md": "B\n", "refs/a.md": "A\n"}
    assert (
        parse_skill(_normative_text(), first).content_hash
        == parse_skill(_normative_text(), second).content_hash
    )
    assert parse_skill(_normative_text().replace("逐节", "分节")).content_hash != base.content_hash
    assert parse_skill(_normative_text().replace("v2", "v3")).content_hash != base.content_hash
    assert parse_skill(_normative_text(), {"refs/a.md": "B\n"}).content_hash != base.content_hash
    relicensed = parse_skill(_normative_text().replace("MIT", "Apache-2.0"))
    assert relicensed.content_hash != base.content_hash


def test_unknown_fields_ignored_and_hash_stable() -> None:
    base = parse_skill(_normative_text())
    extra = dict(NORMATIVE, source="community", version="3", experimental="{nested: [1, 2]}")
    doc = parse_skill(_frontmatter_text(**extra))
    assert doc.name == base.name
    assert doc.metadata == {"team": "finance", "version": "v2"}
    assert doc.content_hash == base.content_hash


def test_direct_construction_hash_mismatch_rejected() -> None:
    doc = parse_skill(_normative_text())
    payload = doc.model_dump()
    payload["content_hash"] = "0" * 64
    with pytest.raises(ValidationError):
        SkillDocument(**payload)


def test_direct_document_construction_validated() -> None:
    payload = parse_skill(_normative_text()).model_dump()
    payload["resources"] = {"../bad.md": "x"}
    with pytest.raises(ValidationError):
        SkillDocument(**payload)


@pytest.mark.parametrize("missing", ["name", "description"])
def test_missing_required_field_rejected(missing: str) -> None:
    fields = {key: value for key, value in NORMATIVE.items() if key != missing}
    with pytest.raises(SkillParseError):
        parse_skill(_frontmatter_text(**fields))


@pytest.mark.parametrize(
    "name",
    ["PDF-Report", "-lead", "trail-", "double--hyphen", "under_score", "spa ce", "中文", "a" * 65],
)
def test_invalid_name_rejected(name: str) -> None:
    with pytest.raises(SkillParseError):
        parse_skill(_frontmatter_text(name=name, description="说明"))


def test_valid_name_variants() -> None:
    for name in ("a", "v2", "pdf-report", "a-b-1", "a" * SKILL_NAME_MAX_CHARS):
        doc = parse_skill(_frontmatter_text(name=name, description="说明"))
        assert doc.name == name


def test_description_limits() -> None:
    parse_skill(_frontmatter_text(name="x", description="说" * SKILL_DESCRIPTION_MAX_CHARS))
    with pytest.raises(SkillParseError):
        parse_skill(
            _frontmatter_text(name="x", description="说" * (SKILL_DESCRIPTION_MAX_CHARS + 1))
        )
    with pytest.raises(SkillParseError):
        parse_skill(_frontmatter_text(name="x", description='"   "'))


@pytest.mark.parametrize(
    "broken",
    [
        ("name: pdf-report", "name: pdf-report\nname: pdf-report"),
        ("name: pdf-report", "name: !!str pdf-report"),
        ("name: pdf-report", 'name: !!python/object/apply:os.system ["echo hi"]'),
        ("name: pdf-report", "name: &a pdf-report\ndescription: *a"),
        ("name: pdf-report", "<<: {extra: 1}"),
        ("name: pdf-report", "123: abc"),
        ("name: pdf-report", "true: yes"),
    ],
)
def test_yaml_ambiguity_rejected(broken: tuple[str, str]) -> None:
    with pytest.raises(SkillParseError):
        parse_skill(_normative_text().replace(*broken))


def test_yaml_multi_document_rejected() -> None:
    text = "---\nname: a\ndescription: b\n...\nname: c\n---\n正文\n"
    with pytest.raises(SkillParseError):
        parse_skill(text)


@pytest.mark.parametrize(
    "replacement",
    [
        ("name: pdf-report", "name: 123"),
        ("description: 解读月度 PDF 报告并汇总结论", "description: [a, b]"),
        ("license: MIT", "license: [MIT]"),
        ("compatibility: krim>=0.4", "compatibility: 12"),
        ("metadata: {team: finance, version: v2}", "metadata: team"),
        ("metadata: {team: finance, version: v2}", "metadata: {team: {a: 1}}"),
        ("metadata: {team: finance, version: v2}", "metadata: {team: 7}"),
        ("metadata: {team: finance, version: v2}", "metadata: {created: 2024-01-02}"),
    ],
)
def test_type_mismatch_rejected(replacement: tuple[str, str]) -> None:
    with pytest.raises(SkillParseError):
        parse_skill(_normative_text().replace(*replacement))


def test_document_nul_rejected() -> None:
    with pytest.raises(SkillParseError):
        parse_skill(_normative_text().replace("逐节", "正\x00文"))


def test_frontmatter_structure_rejected() -> None:
    for text in (
        "# 说明\n---\nname: x\ndescription: y\n---\n正文\n",
        "---\nname: x\ndescription: y\n",
        "---\n---\n",
        "---\n- a\n- b\n---\n正文\n",
        "---\nscalar\n---\n正文\n",
    ):
        with pytest.raises(SkillParseError):
            parse_skill(text)


def test_body_required_and_bounded() -> None:
    maxed = "x" * SKILL_BODY_MAX_BYTES
    assert parse_skill(_frontmatter_text(body=maxed, name="x", description="y")).body == maxed
    with pytest.raises(SkillParseError):
        parse_skill(
            _frontmatter_text(body="x" * (SKILL_BODY_MAX_BYTES + 1), name="x", description="y")
        )
    assert parse_skill("---\nname: x\ndescription: y\n---\n   \n").body == "   \n"
    assert parse_skill("---\nname: x\ndescription: y\n---").body == ""


def test_body_may_contain_delimiter_lines() -> None:
    text = "---\nname: x\ndescription: y\n---\n段落\n---\n水平线后仍在正文\n"
    doc = parse_skill(text)
    assert doc.body.startswith("段落\n---\n")


def test_total_bytes_limit() -> None:
    base = _frontmatter_text(body="x" * SKILL_BODY_MAX_BYTES, name="x", description="y")
    room = SKILL_TOTAL_MAX_BYTES - len(base.encode())
    assert room <= SKILL_RESOURCES_TOTAL_MAX_BYTES
    half = room // 2
    doc = parse_skill(base, {"a.md": "y" * half, "b.md": "y" * (room - half)})
    assert len(doc.resources) == 2
    with pytest.raises(SkillParseError):
        parse_skill(base, {"a.md": "y" * half, "b.md": "y" * (room - half + 1)})


@pytest.mark.parametrize(
    "path",
    [
        "../escape.md",
        "/absolute.md",
        "C:/notes.md",
        "back\\slash.md",
        "./current.md",
        "..",
        ".",
        "a//b.md",
        "folder/",
        "%2e%2e.md",
        "%41.md",
        "a\x01b.md",
        "a" + chr(0x202E) + "b.md",
        "a:b.md",
        "",
        "x" * 257,
    ],
)
def test_invalid_resource_path_rejected(path: str) -> None:
    with pytest.raises(SkillParseError):
        parse_skill(_normative_text(), {path: "内容\n"})


@pytest.mark.parametrize(
    "path", ["refs/guide.md", "notes v2.md", ".hidden.md", "指南/说明.md", "SKILL.md"]
)
def test_valid_resource_paths(path: str) -> None:
    doc = parse_skill(_normative_text(), {path: "x"})
    assert doc.resources[path] == "x"


def test_resource_path_length_boundary() -> None:
    parse_skill(_normative_text(), {"a" * 253 + ".md": "x"})
    with pytest.raises(SkillParseError):
        parse_skill(_normative_text(), {"a" * 254 + ".md": "x"})


def test_resource_case_collision_rejected() -> None:
    with pytest.raises(SkillParseError):
        parse_skill(_normative_text(), {"a.md": "1\n", "A.md": "2\n"})


def test_resource_nfc_collision_rejected() -> None:
    with pytest.raises(SkillParseError):
        parse_skill(_normative_text(), {"cafe\u0301.md": "1\n", "caf\u00e9.md": "2\n"})


def test_resource_count_limit() -> None:
    resources = {f"r{i}.md": "x" for i in range(SKILL_RESOURCE_MAX_COUNT)}
    assert len(parse_skill(_normative_text(), resources).resources) == SKILL_RESOURCE_MAX_COUNT
    with pytest.raises(SkillParseError):
        parse_skill(_normative_text(), {**resources, "extra.md": "x"})


def test_resource_bytes_limits() -> None:
    text = _normative_text()
    big = {"big.md": "x" * SKILL_RESOURCE_MAX_BYTES}
    assert parse_skill(text, big).resources == big
    with pytest.raises(SkillParseError):
        parse_skill(text, {"big.md": "x" * (SKILL_RESOURCE_MAX_BYTES + 1)})
    with pytest.raises(SkillParseError):
        parse_skill(text, {"a.md": "x" * 16385, "b.md": "x" * 16384})
    assert len(parse_skill(text, {"a.md": "x" * 16384, "b.md": "x" * 16384}).resources) == 2


def test_resource_content_validation() -> None:
    with pytest.raises(SkillParseError):
        parse_skill(_normative_text(), {"a.md": "正\x00文"})
    with pytest.raises(SkillParseError):
        parse_skill(_normative_text(), {"a.md": 123})  # type: ignore[dict-item]


def test_metadata_limits() -> None:
    entries = ", ".join(f"k{i}: v" for i in range(SKILL_METADATA_MAX_ENTRIES))
    doc = parse_skill(_frontmatter_text(name="x", description="y", metadata="{" + entries + "}"))
    assert len(doc.metadata) == SKILL_METADATA_MAX_ENTRIES
    over = ", ".join(f"k{i}: v" for i in range(SKILL_METADATA_MAX_ENTRIES + 1))
    with pytest.raises(SkillParseError):
        parse_skill(_frontmatter_text(name="x", description="y", metadata="{" + over + "}"))
    long_key = "k" * (SKILL_METADATA_KEY_MAX_CHARS + 1)
    with pytest.raises(SkillParseError):
        parse_skill(_frontmatter_text(name="x", description="y", metadata=f"{{{long_key}: v}}"))
    long_value = "v" * (SKILL_METADATA_VALUE_MAX_CHARS + 1)
    with pytest.raises(SkillParseError):
        parse_skill(_frontmatter_text(name="x", description="y", metadata=f"{{k: {long_value}}}"))


def test_metadata_values() -> None:
    doc = parse_skill(_frontmatter_text(name="x", description="y", metadata='{k: "", 说: "值"}'))
    assert doc.metadata == {"k": "", "说": "值"}


def test_crlf_document_parses() -> None:
    text = "---\r\nname: x\r\ndescription: y\r\n---\r\n正文第一行\r\n正文第二行\r\n"
    doc = parse_skill(text)
    assert doc.name == "x"
    assert doc.body == "正文第一行\r\n正文第二行\r\n"


def test_input_type_validation() -> None:
    with pytest.raises(SkillParseError):
        parse_skill(123)  # type: ignore[arg-type]
    with pytest.raises(SkillParseError):
        parse_skill(_normative_text(), ["a.md"])  # type: ignore[arg-type]


def test_summary_from_document() -> None:
    doc = parse_skill(_normative_text())
    summary = SkillSummary.from_document(doc)
    assert summary.name == doc.name
    assert summary.description == doc.description
    assert summary.content_hash == doc.content_hash
    with pytest.raises(ValidationError):
        SkillSummary(name="Bad_Name", description="x", content_hash=doc.content_hash)

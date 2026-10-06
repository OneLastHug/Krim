import re
from pathlib import Path


def test_markdown_links_and_fences():
    files = [Path("README.md"), Path("AGENTS.md"), *Path("docs").rglob("*.md")]
    errors = []
    for file in files:
        text = file.read_text("utf-8")
        fences = re.findall(r"^```", text, re.MULTILINE)
        if len(fences) % 2:
            errors.append(f"{file}: 代码围栏未闭合")
        for target in re.findall(r"\[[^\]]+\]\(([^\s)]+)\)", text):
            if "://" in target or target.startswith("#"):
                continue
            if not (file.parent / target.split("#", 1)[0]).exists():
                errors.append(f"{file}: 无效链接 {target}")
    assert not errors, "\n".join(errors)


def test_project_branding_and_unique_requirements():
    files = [
        Path("README.md"),
        Path("AGENTS.md"),
        *Path("docs").rglob("*.md"),
        *Path("src").rglob("*.py"),
        *Path("frontend").rglob("*.ts"),
    ]
    for file in files:
        assert not re.search(r"chaos[- _]agent", file.read_text("utf-8"), re.IGNORECASE), file
    matrix = Path("docs/requirements/requirements-traceability.md").read_text("utf-8")
    matrix = matrix.split("## M1 实际证据", 1)[0]
    identifiers = re.findall(r"^\| (REQ-[A-Z]+-\d+) \|", matrix, re.MULTILINE)
    assert len(identifiers) == 58 and len(set(identifiers)) == 58

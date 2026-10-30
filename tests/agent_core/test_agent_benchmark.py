from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Any

import pytest

from krim_agent.evaluation.agent_benchmark import (
    BENCHMARK_NAME,
    BENCHMARK_VERSION,
    benchmark_case,
    benchmark_cases,
    benchmark_dataset,
)
from krim_agent.evaluation.benchmark_verifier import (
    SCHEMA_VERSION,
    parse_benchmark_result,
    verify_benchmark,
)
from krim_agent.evaluation.domain import neutral_fingerprint

# These are hand-written trusted fixtures for offline checker tests, never model outputs.
TRUSTED_CANDIDATES: dict[str, dict[str, str]] = {
    "amount-parse-v1": {
        "money.py": (
            "from decimal import Decimal\n\n"
            "def parse_amount(text):\n"
            "    if not isinstance(text, str):\n"
            "        raise ValueError('amount must be text')\n"
            "    value = Decimal(text)\n"
            "    if not value.is_finite() or value.as_tuple().exponent < -2:\n"
            "        raise ValueError('invalid amount')\n"
            "    return value\n"
        )
    },
    "inventory-snapshot-v1": {
        "inventory/catalog.py": (
            "def build_snapshot(rows):\n"
            "    quantities = {}\n"
            "    for row in rows:\n"
            "        sku = row['sku'].strip()\n"
            "        quantity = row['quantity']\n"
            "        if not sku or type(quantity) is not int or quantity <= 0:\n"
            "            raise ValueError('invalid row')\n"
            "        quantities[sku] = quantities.get(sku, 0) + quantity\n"
            "    items = [\n"
            "        {'sku': key, 'quantity': quantities[key]}\n"
            "        for key in sorted(quantities)\n"
            "    ]\n"
            "    total = sum(row['quantity'] for row in items)\n"
            "    return {'items': items, 'total_quantity': total}\n"
        ),
        "inventory/cli.py": (
            "import json\n"
            "from pathlib import Path\n"
            "from typing import Sequence\n"
            "from .catalog import build_snapshot\n\n"
            "def main(argv: Sequence[str] | None = None) -> int:\n"
            "    import sys\n"
            "    args = list(sys.argv[1:] if argv is None else argv)\n"
            "    if len(args) != 3 or args[0] != 'snapshot':\n"
            "        return 2\n"
            "    rows = json.loads(Path(args[1]).read_text(encoding='utf-8'))\n"
            "    value = build_snapshot(rows)\n"
            "    text = json.dumps(value, ensure_ascii=False) + '\\n'\n"
            "    Path(args[2]).write_text(text, encoding='utf-8')\n"
            "    return 0\n"
        ),
    },
    "report-refactor-v1": {
        "report.py": (
            "def normalize_rows(rows):\n"
            "    return sorted(\n"
            "        ((row['name'].strip().casefold(), int(row['count'])) for row in rows),\n"
            "        key=lambda item: item[0],\n"
            "    )\n\n"
            "def render_report(rows):\n"
            "    return 'name,count\\n' + ''.join(f'{name},{count}\\n' for name, count in rows)\n\n"
            "def build_report(rows):\n"
            "    return render_report(normalize_rows(rows))\n"
        )
    },
    "csv-groups-v1": {
        "csv_report.py": (
            "import csv\n"
            "import io\n"
            "import json\n"
            "import sys\n"
            "import unicodedata\n"
            "from decimal import Decimal\n"
            "from pathlib import Path\n\n"
            "def summarize_csv(text):\n"
            "    rows = csv.DictReader(io.StringIO(text.lstrip('\\ufeff')))\n"
            "    seen = set()\n"
            "    groups = {}\n"
            "    for row in rows:\n"
            "        identity = row['id'].strip()\n"
            "        if identity in seen:\n"
            "            continue\n"
            "        seen.add(identity)\n"
            "        group = unicodedata.normalize('NFC', row['group'].strip()) or '(missing)'\n"
            "        label = unicodedata.normalize('NFC', row['label'].strip())\n"
            "        amount = Decimal(row['amount'].strip() or '0').quantize(Decimal('0.01'))\n"
            "        item = groups.setdefault(\n"
            "            group,\n"
            "            {'count': 0, 'total': Decimal('0.00'), 'labels': []},\n"
            "        )\n"
            "        item['count'] += 1\n"
            "        item['total'] += amount\n"
            "        item['labels'].append(label)\n"
            "    output = []\n"
            "    for group in sorted(groups):\n"
            "        item = groups[group]\n"
            "        output.append({\n"
            "            'group': group,\n"
            "            'count': item['count'],\n"
            "            'total': format(item['total'], '.2f'),\n"
            "            'labels': sorted(item['labels']),\n"
            "        })\n"
            "    return {'groups': output}\n\n"
            "def main(argv=None):\n"
            "    args = list(sys.argv[1:] if argv is None else argv)\n"
            "    if len(args) != 2:\n"
            "        return 2\n"
            "    value = summarize_csv(Path(args[0]).read_text(encoding='utf-8'))\n"
            "    content = json.dumps(value, ensure_ascii=False) + '\\n'\n"
            "    Path(args[1]).write_text(content, encoding='utf-8')\n"
            "    return 0\n\n"
            "if __name__ == '__main__':\n"
            "    raise SystemExit(main())\n"
        )
    },
    "json-records-v1": {
        "records.py": (
            "def normalize_record(record):\n"
            "    errors = []\n"
            "    identity = record.get('id')\n"
            "    if not isinstance(identity, str) or not identity.strip():\n"
            "        errors.append({\n"
            "            'path': '/id', 'code': 'required_string',\n"
            "            'message': 'must be a non-empty string',\n"
            "        })\n"
            "    email = record.get('email')\n"
            "    valid_email = (\n"
            "        isinstance(email, str)\n"
            "        and email.count('@') == 1\n"
            "        and all(email.strip().split('@'))\n"
            "    )\n"
            "    if not valid_email:\n"
            "        errors.append({\n"
            "            'path': '/email', 'code': 'invalid_email',\n"
            "            'message': 'must be a valid email address',\n"
            "        })\n"
            "    age = record.get('age')\n"
            "    if age is not None and (type(age) is not int or age < 0):\n"
            "        errors.append({\n"
            "            'path': '/age', 'code': 'invalid_age',\n"
            "            'message': 'must be a non-negative integer or null',\n"
            "        })\n"
            "    if errors:\n"
            "        return {'ok': False, 'record': None, 'errors': errors}\n"
            "    value = dict(record)\n"
            "    value['id'] = identity.strip()\n"
            "    value['email'] = email.strip().lower()\n"
            "    if 'display_name' in value:\n"
            "        value['display_name'] = value['display_name'].strip()\n"
            "    return {'ok': True, 'record': value, 'errors': []}\n"
        )
    },
    "inventory-artifacts-v1": {
        "pipeline.py": (
            "import json\n"
            "from pathlib import Path\n\n"
            "def build_report(records):\n"
            "    skus = sorted(record['sku'] for record in records)\n"
            "    total = sum(record['quantity'] for record in records)\n"
            "    summary = {'records': len(records), 'total_quantity': total, 'skus': skus}\n"
            "    markdown = (\n"
            "        '# Inventory report\\n\\n'\n"
            "        f'- Records: {len(records)}\\n'\n"
            "        f'- Total quantity: {total}\\n'\n"
            "        f'- SKUs: {', '.join(skus)}\\n'\n"
            "    )\n"
            "    return summary, markdown\n\n"
            "def main(input_path, output_dir):\n"
            "    records = json.loads(Path(input_path).read_text(encoding='utf-8'))\n"
            "    summary, markdown = build_report(records)\n"
            "    output = Path(output_dir)\n"
            "    content = json.dumps(summary, ensure_ascii=False) + '\\n'\n"
            "    (output / 'summary.json').write_text(content, encoding='utf-8')\n"
            "    (output / 'summary.md').write_text(markdown, encoding='utf-8')\n"
        )
    },
}

EXPECTED_CSV = {
    "groups": [
        {"group": "(missing)", "count": 1, "total": "2.00", "labels": ["Café"]},
        {"group": "咖啡", "count": 1, "total": "0.00", "labels": ["浅焙"]},
        {"group": "茶", "count": 2, "total": "2.00", "labels": ["乌龙", "绿茶"]},
    ]
}
EXPECTED_SNAPSHOT = {
    "items": [{"sku": "A", "quantity": 1}, {"sku": "B", "quantity": 5}],
    "total_quantity": 6,
}
EXPECTED_ARTIFACT_SUMMARY = {"records": 2, "total_quantity": 3, "skus": ["K-1", "K-2"]}
EXPECTED_ARTIFACT_MARKDOWN = (
    "# Inventory report\n\n- Records: 2\n- Total quantity: 3\n- SKUs: K-1, K-2\n"
)


def _prepare_workspace(root: Path, case_id: str) -> dict[str, dict[str, Any]]:
    case = benchmark_case(case_id)
    initial: dict[str, dict[str, Any]] = {}
    for relative, text in case.files.items():
        path = root / relative
        path.parent.mkdir(parents=True, exist_ok=True)
        data = text.encode("utf-8")
        path.write_bytes(data)
        initial[relative] = {"sha256": hashlib.sha256(data).hexdigest(), "bytes": len(data)}
    instructions = case.instruction.encode("utf-8")
    (root / "INSTRUCTIONS.md").write_bytes(instructions)
    initial["INSTRUCTIONS.md"] = {
        "sha256": hashlib.sha256(instructions).hexdigest(),
        "bytes": len(instructions),
    }
    (root / "outputs").mkdir()
    return initial


def _install_candidate(root: Path, case_id: str) -> None:
    for relative, source in TRUSTED_CANDIDATES[case_id].items():
        (root / relative).write_text(source, encoding="utf-8")
    if case_id == "inventory-snapshot-v1":
        (root / "outputs/snapshot.json").write_text(
            json.dumps(EXPECTED_SNAPSHOT, ensure_ascii=False), encoding="utf-8"
        )
    elif case_id == "csv-groups-v1":
        (root / "outputs/groups.json").write_text(
            json.dumps(EXPECTED_CSV, ensure_ascii=False), encoding="utf-8"
        )
    elif case_id == "inventory-artifacts-v1":
        (root / "outputs/summary.json").write_text(
            json.dumps(EXPECTED_ARTIFACT_SUMMARY, ensure_ascii=False), encoding="utf-8"
        )
        (root / "outputs/summary.md").write_text(EXPECTED_ARTIFACT_MARKDOWN, encoding="utf-8")


def test_benchmark_dataset_freezes_six_provider_neutral_cases() -> None:
    dataset = benchmark_dataset()
    assert dataset.name == BENCHMARK_NAME
    assert dataset.version == BENCHMARK_VERSION
    assert len(dataset.cases) == 6
    assert dataset.ref().fingerprint == benchmark_dataset().ref().fingerprint
    assert [case.case_id for case in benchmark_cases()] == [
        "amount-parse-v1",
        "inventory-snapshot-v1",
        "report-refactor-v1",
        "csv-groups-v1",
        "json-records-v1",
        "inventory-artifacts-v1",
    ]
    assert all(case.verifier == "agent-bench/1" for case in dataset.cases)
    assert all(len(case.instruction) <= 4096 for case in dataset.cases)
    assert all(1 <= len(case.files) <= 16 for case in dataset.cases)
    total_input_bytes = sum(
        len(text.encode("utf-8"))
        for case in dataset.cases
        for text in case.files.values()
    )
    assert total_input_bytes < 32 * 1024
    assert all("gold" not in " ".join(case.files).lower() for case in dataset.cases)
    first = benchmark_cases()[0]
    first.files["money.py"] = "changed by caller"
    assert "return float(text)" in benchmark_case("amount-parse-v1").files["money.py"]
    with pytest.raises(ValueError, match="unknown_benchmark_case"):
        benchmark_case("user-supplied-checker")


@pytest.mark.parametrize(
    "case_id",
    [
        "amount-parse-v1",
        "inventory-snapshot-v1",
        "report-refactor-v1",
        "csv-groups-v1",
        "json-records-v1",
        "inventory-artifacts-v1",
    ],
)
def test_initial_phase_executes_baseline_and_detects_incomplete_target(
    tmp_path: Path, case_id: str
) -> None:
    initial = _prepare_workspace(tmp_path, case_id)
    outcome = verify_benchmark(case_id, tmp_path, initial, "initial")
    parsed = parse_benchmark_result(outcome, case_id=case_id, phase="initial")
    assert parsed["schema_version"] == SCHEMA_VERSION
    assert parsed["passed"] is True
    assert parsed["baseline_checks_complete"] is True
    assert parsed["checks"]["baseline.target_not_yet_met"] is True
    assert parsed["checks"]["baseline.probes_executed"] is True
    assert parsed["test_run"]["discovered"] > 0
    assert parsed["test_run"]["executed"] == parsed["test_run"]["discovered"]
    if case_id == "report-refactor-v1":
        assert parsed["checks"]["baseline.original_semantics_held"] is True
        assert parsed["checks"]["baseline.refactor_target_missing"] is True


@pytest.mark.parametrize(
    "case_id",
    [
        "amount-parse-v1",
        "inventory-snapshot-v1",
        "report-refactor-v1",
        "csv-groups-v1",
        "json-records-v1",
        "inventory-artifacts-v1",
    ],
)
def test_fixed_checker_accepts_only_handwritten_trusted_candidate_fixtures(
    tmp_path: Path, case_id: str
) -> None:
    initial = _prepare_workspace(tmp_path, case_id)
    _install_candidate(tmp_path, case_id)
    outcome = verify_benchmark(case_id, tmp_path, initial, "final")
    parsed = parse_benchmark_result(outcome, case_id=case_id, phase="final")
    assert parsed["passed"] is True, parsed["checks"]
    assert parsed["environment_error"] is False
    assert parsed["case_fingerprint"] == neutral_fingerprint(
        benchmark_case(case_id).model_dump(mode="json")
    )
    assert parsed["test_run"]["discovered"] > 0
    assert parsed["test_run"]["executed"] == parsed["test_run"]["discovered"]
    assert parsed["test_run"]["failures"] == 0
    assert parsed["test_run"]["errors"] == 0
    assert parsed["test_run"]["skipped"] == 0


def test_missing_initial_workspace_does_not_claim_baseline_complete(tmp_path: Path) -> None:
    initial = _prepare_workspace(tmp_path, "amount-parse-v1")
    (tmp_path / "money.py").unlink()
    result = verify_benchmark("amount-parse-v1", tmp_path, initial, "initial")
    assert result["environment_error"] is True
    assert result["baseline_checks_complete"] is False
    assert result["test_run"] is None
    assert result["passed"] is False


def test_protected_input_mutation_and_extra_files_are_rejected(tmp_path: Path) -> None:
    initial = _prepare_workspace(tmp_path, "inventory-snapshot-v1")
    _install_candidate(tmp_path, "inventory-snapshot-v1")
    (tmp_path / "fixtures/items.json").write_text("[]\n", encoding="utf-8")
    outcome = verify_benchmark("inventory-snapshot-v1", tmp_path, initial, "final")
    assert outcome["environment_error"] is False
    assert outcome["checks"]["protected.inputs_unchanged"] is False
    assert outcome["passed"] is False

    (tmp_path / "fixtures/items.json").write_text(
        benchmark_case("inventory-snapshot-v1").files["fixtures/items.json"],
        encoding="utf-8",
    )
    (tmp_path / "unexpected.txt").write_text("extra", encoding="utf-8")
    extra = verify_benchmark("inventory-snapshot-v1", tmp_path, initial, "final")
    assert extra["environment_error"] is True
    assert extra["passed"] is False


def test_symlink_workspace_file_is_rejected(tmp_path: Path) -> None:
    initial = _prepare_workspace(tmp_path, "amount-parse-v1")
    source = tmp_path / "money.py"
    replacement = tmp_path / "replacement.py"
    replacement.write_text(source.read_text(encoding="utf-8"), encoding="utf-8")
    source.unlink()
    source.symlink_to(replacement)
    outcome = verify_benchmark("amount-parse-v1", tmp_path, initial, "initial")
    assert outcome["environment_error"] is True
    assert outcome["baseline_checks_complete"] is False


def test_result_parser_rejects_non_boolean_checks_and_empty_test_counts(
    tmp_path: Path,
) -> None:
    initial = _prepare_workspace(tmp_path, "amount-parse-v1")
    result = verify_benchmark("amount-parse-v1", tmp_path, initial, "initial")
    bad_check = dict(result)
    bad_check["checks"] = dict(result["checks"])
    bad_check["checks"]["baseline.probes_executed"] = 1
    with pytest.raises(ValueError, match="benchmark_verifier_protocol"):
        parse_benchmark_result(bad_check, case_id="amount-parse-v1", phase="initial")

    bad_counts = dict(result)
    bad_counts["test_run"] = {
        "discovered": 0,
        "executed": 0,
        "passed": 0,
        "failures": 0,
        "errors": 0,
        "skipped": 0,
    }
    with pytest.raises(ValueError, match="benchmark_verifier_protocol"):
        parse_benchmark_result(bad_counts, case_id="amount-parse-v1", phase="initial")


def test_case_fingerprint_binds_reference_identity_and_files() -> None:
    case = benchmark_case("amount-parse-v1")
    spec = case.model_dump(mode="json")
    assert spec["reference"]["checker_id"] == "amount-parse-v1/fixed-checker-1"
    assert spec["verifier"] == "agent-bench/1"
    fingerprint = neutral_fingerprint(spec)
    changed_files = dict(spec)
    changed_files["files"] = dict(spec["files"])
    changed_files["files"]["money.py"] = "different source"
    assert neutral_fingerprint(changed_files) != fingerprint
    changed_reference = dict(spec)
    changed_reference["reference"] = dict(spec["reference"])
    changed_reference["reference"]["reference_id"] = "another-reference"
    assert neutral_fingerprint(changed_reference) != fingerprint

"""Unit tests for the external-service challenge image and CSV oracle helper."""

from __future__ import annotations

import csv
import dataclasses
import itertools
import json
import re
import struct
import zlib

import pytest

from krim_agent.media.formats import inspect_image
from tests.coding_agent import external_challenge
from tests.coding_agent.external_challenge import (
    COLORS,
    INPUT_CSV,
    TASK_CONTRACT,
    Challenge,
    grade_csv_output,
    make_challenge,
    parse_vision_answer,
)

PERMUTATIONS = tuple(itertools.permutations(COLORS))
PIXEL_RGB = {"red": b"\xff\x00\x00", "green": b"\x00\xff\x00", "blue": b"\x00\x00\xff"}
WHITE = b"\xff\xff\xff"
DATA_ROWS = (
    (2, "Alice", "30"),
    (3, "Bob", "-1"),
    (4, "", "20"),
    (5, "Chen, Li", "42"),
    (6, "Eve", "oops"),
    (7, "Fox", "10"),
)


def chunks(image: bytes) -> list[tuple[bytes, bytes]]:
    found, offset = [], 8
    while offset < len(image):
        length = struct.unpack_from(">I", image, offset)[0]
        found.append((image[offset + 4:offset + 8], image[offset + 8:offset + 8 + length]))
        offset += 12 + length
    return found


def decoded_rows(image: bytes) -> list[bytes]:
    info = inspect_image(image, validate_pixels=True, max_pixels=2_000_000)
    compressed = b"".join(data for kind, data in chunks(image) if kind == b"IDAT")
    raw = zlib.decompress(compressed)
    stride = info.width * 3 + 1
    assert len(raw) == stride * info.height
    assert all(raw[start] == 0 for start in range(0, len(raw), stride))
    return [raw[start + 1:start + stride] for start in range(0, len(raw), stride)]


def span_of(row: bytes, color: bytes) -> tuple[int, int] | None:
    matches = [x for x in range(len(row) // 3) if row[3 * x:3 * x + 3] == color]
    if not matches:
        return None
    assert matches == list(range(matches[0], matches[-1] + 1))
    return matches[0], matches[-1]


def oracle_rows(order: tuple[str, ...]) -> list[dict[str, object]]:
    left, middle, right = order
    rows = []
    for line, name, age in DATA_ROWS:
        code: str | None
        if name == "":
            code = left
        else:
            try:
                parsed = int(age)
            except ValueError:
                code = middle
            else:
                code = None if parsed >= 0 else right
        rows.append({"line": line, "valid": code is None, "code": code})
    return rows


def output(rows: list[dict[str, object]]) -> bytes:
    return json.dumps({"rows": rows}).encode("utf-8")


def test_challenge_is_frozen_with_exactly_image_and_order():
    challenge = make_challenge(PERMUTATIONS[0])
    assert dataclasses.is_dataclass(Challenge)
    assert {field.name for field in dataclasses.fields(Challenge)} == {"image", "order"}
    assert isinstance(challenge, Challenge)
    assert type(challenge.image) is bytes and challenge.order == PERMUTATIONS[0]
    with pytest.raises(dataclasses.FrozenInstanceError):
        challenge.order = PERMUTATIONS[1]


def test_explicit_order_is_deterministic():
    assert make_challenge(PERMUTATIONS[3]) == make_challenge(PERMUTATIONS[3])


def test_order_accepts_any_sequence_form():
    challenge = make_challenge(["green", "blue", "red"])
    assert challenge.order == ("green", "blue", "red")


@pytest.mark.parametrize("order", [
    (), ("red",), ("Red", "Green", "Blue"), ("red", "red", "green"),
    ("red", "green", "yellow"), ("red", "green", "blue", "blue"),
    ("red", "green", 3), ("red", "green", None),
])
def test_invalid_order_is_rejected(order):
    with pytest.raises(ValueError):
        make_challenge(order)
    with pytest.raises(ValueError):
        grade_csv_output(output(oracle_rows(PERMUTATIONS[0])), order=order)


class ScriptedRandom:
    """Stand-in for secrets.SystemRandom that records its draw."""

    def __init__(self, sample):
        self.fixed = sample
        self.calls = []

    def sample(self, population, k):
        self.calls.append((tuple(population), k))
        return list(self.fixed)


def test_random_order_uses_systemrandom_over_all_colors(monkeypatch):
    scripted = ScriptedRandom(["blue", "red", "green"])
    monkeypatch.setattr(external_challenge, "_random", scripted)
    challenge = make_challenge()
    assert challenge.order == ("blue", "red", "green")
    assert scripted.calls == [(COLORS, 3)]
    inspect_image(challenge.image)


def test_repeated_random_draws_stay_valid_permutations():
    draws = [make_challenge() for _ in range(24)]
    assert {challenge.order for challenge in draws} <= set(PERMUTATIONS)
    assert all(type(challenge.image) is bytes for challenge in draws)
    decoded_rows(draws[0].image)


@pytest.mark.parametrize("order", PERMUTATIONS)
def test_image_shows_three_large_solid_rectangles_in_order(order):
    challenge = make_challenge(order)
    info = inspect_image(challenge.image)
    assert info.media_type == "image/png" and info.frames == 1
    assert (info.width, info.height) >= (600, 200)
    rows = decoded_rows(challenge.image)
    blank = WHITE * info.width
    band = [number for number, row in enumerate(rows) if row != blank]
    assert band and band == list(range(band[0], band[-1] + 1))
    assert band[-1] - band[0] + 1 >= 100
    per_row = []
    for number in band:
        found = {name: span for name in PIXEL_RGB
                 if (span := span_of(rows[number], PIXEL_RGB[name])) is not None}
        assert len(found) == 3
        per_row.append(found)
    spans = per_row[0]
    assert all(found == spans for found in per_row)
    assert set(spans) == set(COLORS)
    ordered = sorted(spans.items(), key=lambda item: item[1][0])
    assert [name for name, _ in ordered] == list(order)
    assert all(stop - start + 1 >= 100 for start, stop in spans.values())
    pairwise = list(zip(ordered, ordered[1:], strict=False))
    assert all(before[1][1] < after[1][0] for before, after in pairwise)
    for number, found in zip(band, per_row, strict=True):
        rebuilt = bytearray(blank)
        for name, (start, stop) in found.items():
            rebuilt[3 * start:3 * stop + 3] = PIXEL_RGB[name] * (stop - start + 1)
        assert rows[number] == rebuilt
    outside = set(range(info.height)) - set(band)
    assert all(rows[number] == blank for number in outside)


def test_png_is_minimal_truecolor_rgb_without_metadata_chunks():
    challenge = make_challenge(PERMUTATIONS[0])
    assert [kind for kind, _ in chunks(challenge.image)] == [b"IHDR", b"IDAT", b"IEND"]
    width, height, depth, color, compression, filtering, interlace = struct.unpack(
        ">IIBBBBB", chunks(challenge.image)[0][1]
    )
    assert (depth, color, compression, filtering, interlace) == (8, 2, 0, 0, 0)
    assert width >= 600 and height >= 200


@pytest.mark.parametrize("order", PERMUTATIONS)
def test_exact_validator_output_passes_for_every_permutation(order):
    expected = oracle_rows(order)
    assert grade_csv_output(output(expected), order=order) == {
        "passed": True, "expected_rows": 6, "matched_rows": 6,
    }
    compact = json.dumps({"rows": expected}, separators=(",", ":")).encode("utf-8")
    assert grade_csv_output(compact, order=order) == {
        "passed": True, "expected_rows": 6, "matched_rows": 6,
    }
    reversed_keys = [{"code": row["code"], "valid": row["valid"], "line": row["line"]}
                     for row in expected]
    assert grade_csv_output(json.dumps({"rows": reversed_keys}, indent=1).encode("utf-8"),
                            order=order)["passed"] is True


def test_wrong_code_fails_with_matched_rows_evidence():
    order = PERMUTATIONS[0]
    wrong = [dict(row) for row in oracle_rows(order)]
    wrong[1]["code"] = next(name for name in COLORS if name != oracle_rows(order)[1]["code"])
    assert grade_csv_output(output(wrong), order=order) == {
        "passed": False, "expected_rows": 6, "matched_rows": 5,
    }


def test_missing_row_fails_and_shifts_positional_matches():
    order = PERMUTATIONS[0]
    rows = oracle_rows(order)
    del rows[1]
    assert grade_csv_output(output(rows), order=order) == {
        "passed": False, "expected_rows": 6, "matched_rows": 1,
    }


def test_extra_fabricated_row_fails():
    order = PERMUTATIONS[0]
    rows = oracle_rows(order) + [{"line": 8, "valid": True, "code": None}]
    assert grade_csv_output(output(rows), order=order) == {
        "passed": False, "expected_rows": 6, "matched_rows": 6,
    }


def test_relabelled_line_fails():
    order = PERMUTATIONS[0]
    rows = [dict(row) for row in oracle_rows(order)]
    rows[2]["line"] = 9
    assert grade_csv_output(output(rows), order=order) == {
        "passed": False, "expected_rows": 6, "matched_rows": 5,
    }


def test_reordered_rows_fail():
    order = PERMUTATIONS[0]
    rows = oracle_rows(order)
    rows[0], rows[1] = rows[1], rows[0]
    assert grade_csv_output(output(rows), order=order) == {
        "passed": False, "expected_rows": 6, "matched_rows": 4,
    }


def test_empty_rows_and_a_different_order_fail():
    order, other = PERMUTATIONS[0], PERMUTATIONS[1]
    assert grade_csv_output(b'{"rows": []}', order=order) == {
        "passed": False, "expected_rows": 6, "matched_rows": 0,
    }
    emitted, expected = oracle_rows(order), oracle_rows(other)
    pairs = list(zip(emitted, expected, strict=True))
    assert emitted != expected
    assert grade_csv_output(output(emitted), order=other) == {
        "passed": False, "expected_rows": 6,
        "matched_rows": sum(1 for got, want in pairs if got == want),
    }


def test_order_is_keyword_only():
    with pytest.raises(TypeError):
        grade_csv_output(output(oracle_rows(PERMUTATIONS[0])), PERMUTATIONS[0])


MALFORMED_OUTPUT = (
    b"", b"not json", b'The answer is {"rows": []}',
    b'```json\n{"rows": []}\n```', b'{"rows": []} trailing prose',
    b"\xff\xfe{}\x00", b"[]", b'"rows"', b"{}",
    b'{"rows": [], "note": "extra top level"}', b'{"lines": []}', b'{"rows": {}}',
    b'{"rows": null}',
    json.dumps({"rows": [{"line": 2, "valid": True}]}).encode("utf-8"),
    json.dumps({"rows": [{"line": 2, "valid": True, "code": None, "note": 1}]}).encode("utf-8"),
    json.dumps({"rows": [{"line": True, "valid": True, "code": None}]}).encode("utf-8"),
    json.dumps({"rows": [{"line": 2.0, "valid": True, "code": None}]}).encode("utf-8"),
    json.dumps({"rows": [{"line": 1, "valid": True, "code": None}]}).encode("utf-8"),
    json.dumps({"rows": [{"line": 0, "valid": True, "code": None}]}).encode("utf-8"),
    json.dumps({"rows": [{"line": 2, "valid": 1, "code": None}]}).encode("utf-8"),
    json.dumps({"rows": [{"line": 2, "valid": True, "code": "purple"}]}).encode("utf-8"),
    json.dumps({"rows": [{"line": 2, "valid": True, "code": 3}]}).encode("utf-8"),
    json.dumps({"rows": [{"line": 2, "valid": True, "code": ["red"]}]}).encode("utf-8"),
    json.dumps({"rows": [["line"], {"line": 2}]}).encode("utf-8"),
    json.dumps({"rows": [{"line": 2, "valid": True, "code": None},
                         {"line": 2, "valid": False, "code": "red"}]}).encode("utf-8"),
    b'{"rows": [{"line": 2, "valid": true, "code": null}], "rows": []}',
)


@pytest.mark.parametrize("raw", MALFORMED_OUTPUT)
def test_malformed_output_raises_value_error(raw):
    with pytest.raises(ValueError):
        grade_csv_output(raw, order=PERMUTATIONS[0])


@pytest.mark.parametrize("order", PERMUTATIONS)
def test_parse_vision_answer_accepts_one_strict_document(order):
    document = json.dumps({"order": list(order)})
    assert parse_vision_answer(document) == order
    assert parse_vision_answer("  " + document + "\n") == order


@pytest.mark.parametrize("text", [
    "", " ", "red, green, blue", "not json",
    'The order is {"order": ["red", "green", "blue"]}.',
    '```json\n{"order": ["red", "green", "blue"]}\n```',
    '{"order": ["red", "green", "blue"]} enjoy',
    "{order: [red, green, blue]}",
    '{"colors": ["red", "green", "blue"]}',
    '{"ORDER": ["red", "green", "blue"]}',
    '{"order": ["red", "green", "blue"], "confidence": 0.9}',
    '{"order": ["red", "green", "blue"], "order": ["blue", "green", "red"]}',
    '{"order": "red green blue"}', '{"order": null}', '{"order": {"0": "red"}}',
    '{"order": ["red", "green"]}',
    '{"order": ["red", "green", "blue", "red"]}',
    '{"order": ["red", "red", "blue"]}',
    '{"order": ["red", "green", "yellow"]}',
    '{"order": ["RED", "GREEN", "BLUE"]}',
    '{"order": [0, 1, 2]}',
    '{"order": [true, false, true]}',
    '["red", "green", "blue"]',
])
def test_parse_vision_answer_refuses_non_strict_documents(text):
    with pytest.raises(ValueError):
        parse_vision_answer(text)


def test_input_csv_lines_and_quoted_field():
    assert type(INPUT_CSV) is bytes
    lines = INPUT_CSV.decode("utf-8").splitlines()
    assert lines == ["name,age", "Alice,30", "Bob,-1", ",20",
                     '"Chen, Li",42', "Eve,oops", "Fox,10"]
    parsed = list(csv.reader(lines))
    assert len(parsed) == 7
    assert parsed[0] == ["name", "age"]
    assert parsed[3] == ["", "20"]
    assert parsed[4] == ["Chen, Li", "42"]
    assert parsed[5] == ["Eve", "oops"]


def test_task_contract_describes_validator_without_oracle_colors():
    assert type(TASK_CONTRACT) is str
    for token in ("validator.py", "argv[1]", '"rows"', '"line"', '"valid"', '"code"',
                  "leftmost", "middle", "rightmost", "null"):
        assert token in TASK_CONTRACT, token
    for name in COLORS:
        assert not re.search(rf"\b{name}\b", TASK_CONTRACT), name

"""Visual challenge and CSV oracle for the external-service acceptance runner.

The host runner delivers ``Challenge.image`` to the task project and grades the
emitted ``validator.py`` output with ``grade_csv_output`` using the challenge's
host-only ``order``. The oracle order never leaves the host: it is not written
into the task project, and ``TASK_CONTRACT`` deliberately names no concrete
color. This module is standard library only, so the host needs no product
dependencies.

Error policy, applied consistently and covered by tests: input that does not
match the wire contract at all (invalid UTF-8, non-JSON text, missing or extra
fields, wrong value types, duplicate lines) raises ``ValueError``; output that
is well formed but disagrees with the oracle (wrong, missing, extra or
reordered rows) is graded ``passed=False`` with ``matched_rows`` evidence and
never raises.
"""

from __future__ import annotations

import csv
import dataclasses
import io
import json
import secrets
import struct
import zlib

COLORS = ("red", "green", "blue")

INPUT_CSV = (
    b"name,age\n"
    b"Alice,30\n"
    b"Bob,-1\n"
    b",20\n"
    b'"Chen, Li",42\n'
    b"Eve,oops\n"
    b"Fox,10\n"
)

TASK_CONTRACT = """\
Create a program named validator.py that validates a CSV file of people and
reports every data row. The CSV filename is given to the program as sys.argv[1]
and the file starts with the header line name,age. A row is valid exactly when
its name is non-empty, its age field parses as an integer and that integer is
not negative.

validator.py prints exactly one JSON document to standard output and nothing
else. The document has this shape, with one object per data row and no other
fields anywhere:

{"rows": [{"line": <integer, at least 2>, "valid": <boolean>,
           "code": <string or null>}]}

"line" is the row's 1-based line number in the CSV file; the header is line 1,
so data rows start at line 2, and every data row appears exactly once.

The "code" of an invalid row is a color name that you read from the challenge
image attached to this task: it shows three large solid rectangles arranged
left to right on a white background. Use the color of the leftmost rectangle
for a row whose name is empty, the color of the middle rectangle for a row
whose age does not parse as an integer, and the color of the rightmost
rectangle for a row whose age is a negative integer. Valid rows use code null.
"""

_RECTANGLE_WIDTH, _RECTANGLE_HEIGHT, _MARGIN, _GAP = 200, 180, 30, 30
_IMAGE_WIDTH = 3 * _RECTANGLE_WIDTH + 2 * _GAP + 2 * _MARGIN
_IMAGE_HEIGHT = _RECTANGLE_HEIGHT + 2 * _MARGIN
_RECTANGLE_RGB = {"red": (255, 0, 0), "green": (0, 255, 0), "blue": (0, 0, 255)}
_BACKGROUND = (255, 255, 255)
_ROW_FIELDS = frozenset({"line", "valid", "code"})

_random = secrets.SystemRandom()


@dataclasses.dataclass(frozen=True, slots=True)
class Challenge:
    """Host-side challenge: ``image`` goes to the task, ``order`` stays on the host."""

    image: bytes
    order: tuple[str, ...]


def _validated_order(order: tuple[str, ...]) -> tuple[str, ...]:
    candidate = tuple(order)
    if (len(candidate) != len(COLORS)
            or not all(type(name) is str for name in candidate)
            or set(candidate) != set(COLORS)):
        raise ValueError(f"order must be a permutation of {COLORS}, got {order!r}")
    return candidate


def _chunk(kind: bytes, payload: bytes) -> bytes:
    return (struct.pack(">I", len(payload)) + kind + payload
            + struct.pack(">I", zlib.crc32(kind + payload) & 0xFFFFFFFF))


def _render(order: tuple[str, ...]) -> bytes:
    spans: list[tuple[int, int, tuple[int, int, int]]] = []
    cursor = 0
    for index, name in enumerate(order):
        left = _MARGIN + index * (_RECTANGLE_WIDTH + _GAP)
        spans.append((cursor, left, _BACKGROUND))
        spans.append((left, left + _RECTANGLE_WIDTH, _RECTANGLE_RGB[name]))
        cursor = left + _RECTANGLE_WIDTH
    spans.append((cursor, _IMAGE_WIDTH, _BACKGROUND))
    band = b"".join(bytes(color) * (stop - start) for start, stop, color in spans)
    blank = bytes(_BACKGROUND) * _IMAGE_WIDTH
    top, bottom = _MARGIN, _MARGIN + _RECTANGLE_HEIGHT
    rows = b"".join(b"\x00" + (band if top <= number < bottom else blank)
                    for number in range(_IMAGE_HEIGHT))
    header = struct.pack(">IIBBBBB", _IMAGE_WIDTH, _IMAGE_HEIGHT, 8, 2, 0, 0, 0)
    return (b"\x89PNG\r\n\x1a\n" + _chunk(b"IHDR", header)
            + _chunk(b"IDAT", zlib.compress(rows, 9)) + _chunk(b"IEND", b""))


def make_challenge(order: tuple[str, ...] | None = None) -> Challenge:
    """Build a challenge image; a missing order is drawn with secrets.SystemRandom."""
    if order is None:
        order = tuple(_random.sample(COLORS, len(COLORS)))
    order = _validated_order(order)
    return Challenge(image=_render(order), order=order)


def _json_object(text: str, what: str) -> object:
    def unique(pairs):
        result = {}
        for key, value in pairs:
            if key in result:
                raise ValueError(f"duplicate JSON key {key!r}")
            result[key] = value
        return result

    try:
        return json.loads(text, object_pairs_hook=unique)
    except ValueError as error:
        raise ValueError(f"{what} is not one strict JSON document: {error}") from None


def parse_vision_answer(text: str) -> tuple[str, ...]:
    """Parse the strict model answer {"order": [...]} listing each color once.

    The whole text must be one JSON object with only the key "order" and a list
    holding each of red, green, blue exactly once. Surrounding whitespace is
    allowed; any prose, Markdown fences, extra fields, unknown or repeated
    colors raise ValueError.
    """
    if type(text) is not str:
        raise ValueError("vision answer must be a str")
    value = _json_object(text, "vision answer")
    if type(value) is not dict or set(value) != {"order"}:
        raise ValueError('vision answer must be one object with only the key "order"')
    entries = value["order"]
    if (type(entries) is not list
            or len(entries) != len(COLORS)
            or not all(type(entry) is str for entry in entries)
            or set(entries) != set(COLORS)):
        raise ValueError(f'"order" must list each of {COLORS} exactly once')
    return tuple(entries)


def _is_integer(text: str) -> bool:
    try:
        int(text)
    except ValueError:
        return False
    return True


def _expected_rows(order: tuple[str, ...]) -> list[dict[str, object]]:
    left, middle, right = order
    rows: list[dict[str, object]] = []
    reader = csv.reader(io.StringIO(INPUT_CSV.decode("utf-8"), newline=""))
    if next(reader, None) != ["name", "age"]:
        raise AssertionError("INPUT_CSV must start with a name,age header")
    for line, fields in enumerate(reader, start=2):
        name, age = fields
        code: str | None
        if not name:
            code = left
        elif not _is_integer(age):
            code = middle
        elif int(age) < 0:
            code = right
        else:
            code = None
        rows.append({"line": line, "valid": code is None, "code": code})
    return rows


def grade_csv_output(raw: bytes, *, order: tuple[str, ...]) -> dict[str, bool | int]:
    """Grade emitted validator output against INPUT_CSV under the given order.

    Envelope strictness: ``raw`` must be bytes holding strict UTF-8 and exactly
    one JSON object {"rows": [...]} whose rows each carry exactly the keys
    "line" (unique integer >= 2), "valid" (boolean) and "code" (null or a
    rectangle color). Prose, fences, extra fields, wrong value types and
    duplicate lines raise ValueError.

    Content: rows are compared as an exact ordered sequence against the rows
    expected for INPUT_CSV, where an empty name maps to the left rectangle's
    color, a non-integer age to the middle rectangle's color and a negative age
    to the right rectangle's color. Wrong, missing, extra or reordered rows
    return {"passed": False, "expected_rows": 6, "matched_rows": <number of
    positional row matches>} and never raise.
    """
    expected = _expected_rows(_validated_order(order))
    if type(raw) is not bytes:
        raise ValueError("validator output must be bytes")
    try:
        text = raw.decode("utf-8")
    except UnicodeDecodeError:
        raise ValueError("validator output is not valid UTF-8") from None
    value = _json_object(text, "validator output")
    if type(value) is not dict or set(value) != {"rows"}:
        raise ValueError('validator output must be one object with only the key "rows"')
    rows = value["rows"]
    if type(rows) is not list:
        raise ValueError('"rows" must be a JSON array')
    seen: set[int] = set()
    for row in rows:
        if type(row) is not dict or set(row) != _ROW_FIELDS:
            raise ValueError(
                'each row must be one object with exactly the keys "line", "valid", "code"'
            )
        line = row["line"]
        if type(line) is not int or line < 2:
            raise ValueError('"line" must be an integer >= 2')
        if line in seen:
            raise ValueError(f'duplicate "line" {line}')
        seen.add(line)
        if type(row["valid"]) is not bool:
            raise ValueError('"valid" must be a boolean')
        code = row["code"]
        if code is not None and (type(code) is not str or code not in _RECTANGLE_RGB):
            raise ValueError(f'"code" must be null or one of {COLORS}')
    matched = sum(1 for produced, want in zip(rows, expected, strict=False)
                  if produced == want)
    return {
        "passed": rows == expected,
        "expected_rows": len(expected),
        "matched_rows": matched,
    }

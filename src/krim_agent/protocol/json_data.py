"""SDK/评估共用的严格有限 JSON；拒绝重复键、非有限和不安全数字。"""

import json

from pydantic import JsonValue, TypeAdapter

from .models import check_json


def parse_json(raw: str) -> JsonValue:
    def unique(pairs: list[tuple[str, JsonValue]]) -> dict[str, JsonValue]:
        values: dict[str, JsonValue] = {}
        for key, value in pairs:
            if key in values:
                raise ValueError("JSON 对象不能包含重复键")
            values[key] = value
        return values

    try:
        value: JsonValue = TypeAdapter(JsonValue).validate_python(
            json.loads(raw, object_pairs_hook=unique), strict=True
        )
        check_json(value, max_bytes=32 * 1024 * 1024)
        return value
    except (RecursionError, OverflowError) as error:
        raise ValueError("JSON 嵌套或数值超过限制") from error

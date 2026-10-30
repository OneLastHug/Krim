"""供应商无关的冻结合成 Agent 基准案例注册表。"""

from __future__ import annotations

import json
from types import MappingProxyType
from typing import Final

from .domain import CaseSpec, DatasetSpec

BENCHMARK_NAME: Final = "agent-benchmark"
BENCHMARK_VERSION: Final = "2026-10-30.1"
REFERENCE_SCHEMA: Final = "agent-bench-reference/1"

# 每个条目只含公开任务输入；gold 与 checker 逻辑固定保存在 benchmark_verifier.py。
_CASES: Final[tuple[CaseSpec, ...]] = (
    CaseSpec(
        case_id="amount-parse-v1",
        verifier="agent-bench/1",
        instruction=(
            "修复 money.py 中的 parse_amount(text)。只允许修改 money.py；不得改动其他输入。"
            "接口必须返回 Decimal，按十进制精确解析金额，允许负数；最多两位小数，"
            "超过两位、非数字或非有限值必须拒绝（抛 ValueError 或 ArithmeticError）。"
            "不要通过 float 中转。不得新增输出文件；工作区输出目录统一为 outputs/。"
        ),
        files={
            "money.py": (
                "from decimal import Decimal\n\n"
                "def parse_amount(text: str) -> float:\n"
                "    return float(text)\n"
            )
        },
        reference={
            "schema_version": REFERENCE_SCHEMA,
            "case_id": "amount-parse-v1",
            "checker_id": "amount-parse-v1/fixed-checker-1",
            "reference_id": "amount-decimal-contract-v1",
        },
    ),
    CaseSpec(
        case_id="inventory-snapshot-v1",
        verifier="agent-bench/1",
        instruction=(
            "实现小型跨文件库存快照功能。只可修改 inventory/catalog.py 和 inventory/cli.py；"
            "inventory/__init__.py、inventory/__main__.py、fixtures/items.json 均为保护文件。"
            "接口 build_snapshot(rows) 按 sku 去空白后合并数量、按 sku 升序输出"
            "{'items':[{'sku':str,'quantity':int},...],'total_quantity':int}；"
            "sku 必须非空，quantity 必须是正整数且 bool 不算整数，非法输入应拒绝。"
            "CLI 固定为 python -m inventory snapshot fixtures/items.json outputs/snapshot.json，"
            "成功返回 0，并写入 UTF-8 JSON。只能在 outputs/ 写结果。"
        ),
        files={
            "inventory/__init__.py": '"""Inventory snapshot package."""\n',
            "inventory/__main__.py": "from .cli import main\n\nraise SystemExit(main())\n",
            "inventory/catalog.py": (
                "from typing import Any\n\n"
                "def build_snapshot(rows: list[dict[str, Any]]) -> dict[str, Any]:\n"
                "    return {\"items\": [], \"total_quantity\": 0}\n"
            ),
            "inventory/cli.py": (
                "from typing import Sequence\n\n"
                "def main(argv: Sequence[str] | None = None) -> int:\n"
                "    return 0\n"
            ),
            "fixtures/items.json": (
                '[{"sku":"B","quantity":2},{"sku":"A","quantity":1},'
                '{"sku":"B","quantity":3}]\n'
            ),
        },
        reference={
            "schema_version": REFERENCE_SCHEMA,
            "case_id": "inventory-snapshot-v1",
            "checker_id": "inventory-snapshot-v1/fixed-checker-1",
            "reference_id": "inventory-snapshot-contract-v1",
        },
    ),
    CaseSpec(
        case_id="report-refactor-v1",
        verifier="agent-bench/1",
        instruction=(
            "对 report.py 做保持行为的重构，只允许修改 report.py。现有 build_report(rows) 的"
            "CSV 文本行为正确，不要改变输出语义或格式。目标是抽出 normalize_rows(rows)"
            "和 render_report(rows) 两个可独立调用的函数，并让 build_report 调用二者。"
            "保持 UTF-8、name 先 strip().casefold()、count 用 int 转换，"
            "按规范化后的名称排序，表头 name,count 及末尾换行。不得新增输出文件；"
            "本案例先检查现有语义基线，再单独检查必要结构目标。"
        ),
        files={
            "report.py": (
                "def build_report(rows):\n"
                "    cleaned = []\n"
                "    for row in rows:\n"
                "        cleaned.append((row['name'].strip().casefold(), int(row['count'])))\n"
                "    cleaned.sort(key=lambda item: item[0])\n"
                "    return 'name,count\\n' + ''.join(\n"
                "        f'{name},{count}\\n' for name, count in cleaned\n"
                "    )\n"
            )
        },
        reference={
            "schema_version": REFERENCE_SCHEMA,
            "case_id": "report-refactor-v1",
            "checker_id": "report-refactor-v1/fixed-checker-1",
            "reference_id": "report-output-semantics-v1",
        },
    ),
    CaseSpec(
        case_id="csv-groups-v1",
        verifier="agent-bench/1",
        instruction=(
            "修复 csv_report.py，只允许修改该文件；transactions.csv 是保护输入。实现"
            "summarize_csv(text)，按 id 去重并保留首次记录，空 amount 作为 0.00，"
            "空 group 归入 '(missing)'，名称和 group 去首尾空格并做 Unicode NFC 规范化，"
            "按 group 排序输出 groups/count/total/labels；金额用 Decimal，total 固定两位小数，"
            "labels 排序。CLI 固定为 python csv_report.py transactions.csv outputs/groups.json，"
            "写入 UTF-8 JSON。只在 outputs/ 写结果。"
        ),
        files={
            "csv_report.py": (
                "def summarize_csv(text: str):\n"
                "    return {\"groups\": []}\n\n"
                "def main(argv=None):\n"
                "    return 0\n"
            ),
            "transactions.csv": (
                "id,group,amount,label\n"
                "1,茶,1.25,绿茶\n"
                "2,,2.00,Café\n"
                "1,茶,9.00,重复\n"
                "3,咖啡,,浅焙\n"
                "4,茶,0.75,乌龙\n"
            ),
        },
        reference={
            "schema_version": REFERENCE_SCHEMA,
            "case_id": "csv-groups-v1",
            "checker_id": "csv-groups-v1/fixed-checker-1",
            "reference_id": "csv-unicode-groups-contract-v1",
        },
    ),
    CaseSpec(
        case_id="json-records-v1",
        verifier="agent-bench/1",
        instruction=(
            "修复 records.py，只允许修改该文件；records.json 是保护输入。实现"
            "normalize_record(record)：有效时返回 {'ok':True,'record':规范化对象,'errors':[]}；"
            "无效时返回 {'ok':False,'record':None,'errors':[...]}。"
            "id/email 为必需非空字符串并 trim；email 小写化，且必须恰有一个 @、两侧非空，"
            "错误 code=invalid_email、message='must be a valid email address'；id 错误"
            "code=required_string、message='must be a non-empty string'。display_name 可选且 trim；"
            "age 可省略、为 null 或非负整数（bool 不算整数），错误 code=invalid_age、"
            "message='must be a non-negative integer or null'。错误 path 分别为 /id、/email、/age；"
            "必须收集所有错误并按 id、email、age 顺序输出。保留所有无关字段及其值不变。"
            "不得改输入文件。"
        ),
        files={
            "records.py": (
                "def normalize_record(record):\n"
                "    return {\"ok\": False, \"record\": None, \"errors\": []}\n"
            ),
            "records.json": (
                '[{"id":" r-7 ","email":" A@EXAMPLE.COM ","display_name":"  李雷  ",'
                '"age":0,"custom":{"label":" keep "}},'
                '{"id":" ","email":"bad","age":-1,"custom":[1]}]\n'
            ),
        },
        reference={
            "schema_version": REFERENCE_SCHEMA,
            "case_id": "json-records-v1",
            "checker_id": "json-records-v1/fixed-checker-1",
            "reference_id": "json-record-validation-contract-v1",
        },
    ),
    CaseSpec(
        case_id="inventory-artifacts-v1",
        verifier="agent-bench/1",
        instruction=(
            "实现 pipeline.py，将保护输入 source/stock.json 读入后，经 Python 代码生成"
            "outputs/summary.json 与 outputs/summary.md；只允许修改 pipeline.py。"
            "公开 build_report(records) 返回 (summary_dict, markdown_text)，"
            "summary 为 records 数和 quantity 总和及排序后的 sku 列表；Markdown 格式见任务要求："
            "标题 '# Inventory report'，空行后依次列 Records、Total quantity、SKUs 三行项目符号，"
            "末尾必须有换行。提供 main(input_path, output_dir) 写出两个产物；不得改输入。"
        ),
        files={
            "pipeline.py": (
                "def build_report(records):\n"
                "    return ({\"records\": 0, \"total_quantity\": 0, \"skus\": []}, \"\")\n\n"
                "def main(input_path, output_dir):\n"
                "    return None\n"
            ),
            "source/stock.json": (
                '[{"sku":"K-2","quantity":1},{"sku":"K-1","quantity":2}]\n'
            ),
        },
        reference={
            "schema_version": REFERENCE_SCHEMA,
            "case_id": "inventory-artifacts-v1",
            "checker_id": "inventory-artifacts-v1/fixed-checker-1",
            "reference_id": "inventory-json-markdown-artifacts-v1",
        },
    ),
)

_CASE_DOCUMENTS: Final = MappingProxyType(
    {
        case.case_id: json.dumps(
            case.model_dump(mode="json"), ensure_ascii=False, sort_keys=True, separators=(",", ":")
        )
        for case in _CASES
    }
)


def benchmark_cases() -> tuple[CaseSpec, ...]:
    """返回固定顺序、无 gold 内容的六个合成案例副本。"""
    return tuple(CaseSpec.model_validate_json(_CASE_DOCUMENTS[case.case_id]) for case in _CASES)


def benchmark_case(case_id: str) -> CaseSpec:
    """只按固定 case_id 取案例；不接受调用方提供 checker 或 gold。"""
    try:
        return CaseSpec.model_validate_json(_CASE_DOCUMENTS[case_id])
    except KeyError:
        raise ValueError("unknown_benchmark_case") from None


def benchmark_dataset() -> DatasetSpec:
    """构造可由现有 DatasetSpec.ref() 冻结和哈希的首轮数据集。"""
    return DatasetSpec(
        name=BENCHMARK_NAME,
        version=BENCHMARK_VERSION,
        cases=list(benchmark_cases()),
    )

"""有限真实参考程序：只读本Trial CSV，输出普通对账文件；无模型/网络。"""

import csv
import json
import sys
from decimal import Decimal, InvalidOperation
from pathlib import Path


def main() -> int:
    try:
        if not Path("INSTRUCTIONS.md").read_text(encoding="utf-8").strip():
            raise ValueError("instruction_missing")
        with Path("catalog.csv").open(newline="", encoding="utf-8") as source:
            catalog = csv.DictReader(source)
            if not {"account", "name"} <= set(catalog.fieldnames or []):
                raise ValueError("missing_columns")
            accounts = {row["account"] for row in catalog}
        balances: dict[str, int] = {}
        seen: dict[str, tuple[str, int]] = {}
        with Path("transactions.csv").open(newline="", encoding="utf-8") as source:
            rows = csv.DictReader(source)
            if not {"id", "account", "amount"} <= set(rows.fieldnames or []):
                raise ValueError("missing_columns")
            for row in rows:
                amount = Decimal(row["amount"]) * 100
                if (
                    not amount.is_finite()
                    or amount != amount.to_integral_value()
                    or row["account"] not in accounts
                ):
                    raise ValueError("invalid_transaction")
                value = (row["account"], int(amount))
                if row["id"] in seen:
                    if seen[row["id"]] != value:
                        raise ValueError("duplicate_conflict")
                    continue
                seen[row["id"]] = value
                balances[value[0]] = balances.get(value[0], 0) + value[1]
        total, count = sum(balances.values()), len(seen)
        output = Path("outputs")
        (output / "ledger.json").write_text(
            json.dumps(
                {
                    "accounts": balances,
                    "total_cents": total,
                    "transaction_count": count,
                },
                sort_keys=True,
            )
            + "\n",
            encoding="utf-8",
        )
        (output / "summary.md").write_text(
            f"total_cents={total}\ntransactions={count}\n",
            encoding="utf-8",
        )
        print("reconciliation_complete")
        return 0
    except (ValueError, KeyError, InvalidOperation) as error:
        print(str(error), file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())

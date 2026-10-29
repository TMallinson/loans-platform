"""Data quality rules. Each returns a list of failure messages; an empty list means pass."""
from decimal import Decimal

from pyspark.sql import DataFrame, functions as F


def check_quarantine_rate(valid_count: int, invalid_count: int, max_pct: float = 5.0) -> list[str]:
    total = valid_count + invalid_count
    if total == 0:
        return ["batch is empty"]
    pct = 100 * invalid_count / total
    return [f"quarantine rate {pct:.1f}% exceeds {max_pct}%"] if pct > max_pct else []


def check_silver(df: DataFrame) -> list[str]:
    if df.limit(1).count() == 0:
        return ["silver.loans is empty"]
    failures = []
    if df.filter(F.col("loan_id").isNull()).limit(1).count():
        failures.append("null loan_id in silver")
    if df.groupBy("loan_id").count().filter(F.col("count") > 1).limit(1).count():
        failures.append("duplicate loan_id in silver")
    if df.filter(F.col("balance") < 0).limit(1).count():
        failures.append("negative balance in silver")
    return failures


def check_reconciliation(source_rows: int, landed_rows: int,
                         source_total: Decimal, landed_total: Decimal) -> list[str]:
    """Ingestion completeness: what landed in bronze must equal what the source holds."""
    failures = []
    if source_rows != landed_rows:
        failures.append(f"row count mismatch: source={source_rows:,} bronze={landed_rows:,}")
    if abs((source_total or 0) - (landed_total or 0)) > Decimal("0.01"):
        failures.append(f"balance total mismatch: source={source_total} bronze={landed_total}")
    return failures
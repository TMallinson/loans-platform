"""Pure transformation logic. No reads, no writes: everything here is unit-testable."""
from pyspark.sql import DataFrame, functions as F

RAW_COLUMNS = ["balance_raw", "open_date_num"]


def parse_cyymmdd(col_name: str):
    """IBM iSeries CYYMMDD integer to DATE. C=1 means 2000s: 1250115 -> 2025-01-15.
    0 means 'no date' on iSeries and becomes NULL."""
    n = F.col(col_name)
    return F.when(n > 0, F.to_date((n + 19000000).cast("string"), "yyyyMMdd"))


def clean_loans(df: DataFrame) -> DataFrame:
    """Fix legacy formats. Keep raw values so quarantined rows can be investigated."""
    return df.select(
        F.trim("loan_id").alias("loan_id"),
        F.trim("member_id").alias("member_id"),
        "ssn",
        F.trim("product").alias("product"),
        F.upper(F.trim("status")).alias("status"),
        # try_cast: a bad value becomes NULL (and gets quarantined) instead of crashing the job
        F.expr("try_cast(trim(balance) AS DECIMAL(18,2))").alias("balance"),
        parse_cyymmdd("open_date_num").alias("open_date"),
        F.col("balance").alias("balance_raw"),
        "open_date_num",
    )


def split_valid_invalid(df: DataFrame) -> tuple[DataFrame, DataFrame]:
    """Tag each row with the first rule it breaks. Valid rows are deduplicated."""
    reason = (
        F.when(F.col("loan_id").isNull(), "null loan_id")
        .when(F.col("balance").isNull(), "unparseable balance")
        .when(F.col("balance") < 0, "negative balance")
        .when(F.col("open_date").isNull(), "missing open_date")
    )
    tagged = df.withColumn("_dq_reason", reason)
    valid = (
        tagged.filter(F.col("_dq_reason").isNull())
        .drop("_dq_reason", *RAW_COLUMNS)
        .dropDuplicates(["loan_id"])
    )
    invalid = tagged.filter(F.col("_dq_reason").isNotNull())
    return valid, invalid


def summarize_portfolio(df: DataFrame) -> DataFrame:
    return df.groupBy("product", "status").agg(
        F.count("*").alias("loan_count"),
        F.sum("balance").cast("decimal(18,2)").alias("total_balance"),
        F.round(F.avg("balance"), 2).cast("decimal(18,2)").alias("avg_balance"),
    )
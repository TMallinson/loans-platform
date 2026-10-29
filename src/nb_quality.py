# Databricks notebook source
# Quality gate: Gold only builds if every check passes.
from pyspark.sql import functions as F

from quality import check_reconciliation, check_silver

# COMMAND ----------

dbutils.widgets.text("catalog", "dev")
dbutils.widgets.text("source_catalog", "corebank_masked")
catalog = dbutils.widgets.get("catalog")
source_catalog = dbutils.widgets.get("source_catalog")

# COMMAND ----------


def rows_and_total(df):
    r = df.agg(F.count("*").alias("n"),
               F.expr("sum(try_cast(trim(balance) AS DECIMAL(18,2)))").alias("total")).first()
    return r["n"], r["total"]


bronze = spark.table(f"{catalog}.bronze.loans")
latest_batch = bronze.filter(F.col("_ingested_at") == bronze.agg(F.max("_ingested_at")).first()[0])

src_rows, src_total = rows_and_total(spark.table(f"{source_catalog}.dbo.loans"))
bronze_rows, bronze_total = rows_and_total(latest_batch)

failures = check_reconciliation(src_rows, bronze_rows, src_total, bronze_total)
failures += check_silver(spark.table(f"{catalog}.silver.loans"))
if failures:
    raise ValueError(f"Quality gate failed: {failures}")
print(f"Quality gate passed: {bronze_rows:,} rows reconciled, silver clean")
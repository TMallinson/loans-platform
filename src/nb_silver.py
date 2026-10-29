# Databricks notebook source
# Silver: clean the latest bronze batch, quarantine bad rows, MERGE the good ones.
from pyspark.sql import functions as F

from transforms import clean_loans, split_valid_invalid
from quality import check_quarantine_rate

# COMMAND ----------

dbutils.widgets.text("catalog", "dev")
catalog = dbutils.widgets.get("catalog")

# COMMAND ----------

bronze = spark.table(f"{catalog}.bronze.loans")
latest = bronze.agg(F.max("_ingested_at")).first()[0]
batch = bronze.filter(F.col("_ingested_at") == latest)

valid, invalid = split_valid_invalid(clean_loans(batch))

# Always keep the evidence
(invalid.withColumn("_quarantined_at", F.current_timestamp())
    .write.mode("append").saveAsTable(f"{catalog}.silver.loans_quarantine"))


# Circuit breaker: if too much of this batch is bad, stop before touching silver
failures = check_quarantine_rate(valid.count(), invalid.count())
if failures:
    raise ValueError(f"Circuit breaker tripped: {failures}. Bad rows are in loans_quarantine.")
# COMMAND ----------

# Upsert on the business key: update known loans, insert new ones. Reruns give the same result.
valid.withColumn("_updated_at", F.current_timestamp()).createOrReplaceTempView("silver_updates")
spark.sql(f"""
MERGE INTO {catalog}.silver.loans AS t
USING silver_updates AS s
ON t.loan_id = s.loan_id
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *
""")
print(f"Merged {valid.count():,} rows; quarantined {invalid.count():,}")
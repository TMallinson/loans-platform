# Databricks notebook source
# Bronze: land a raw snapshot exactly as the source sent it, plus lineage columns.
from pyspark.sql import functions as F

# COMMAND ----------

dbutils.widgets.text("catalog", "dev")
dbutils.widgets.text("source_catalog", "corebank_masked")
catalog = dbutils.widgets.get("catalog")
source_table = f"{dbutils.widgets.get('source_catalog')}.dbo.loans"

# COMMAND ----------

(spark.table(source_table)
    .withColumn("_ingested_at", F.current_timestamp())
    .withColumn("_source", F.lit(source_table))
    .write.mode("append")
    .saveAsTable(f"{catalog}.bronze.loans"))

print(f"Appended a batch from {source_table} to {catalog}.bronze.loans")
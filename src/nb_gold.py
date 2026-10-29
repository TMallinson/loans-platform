# Databricks notebook source
# Gold: business-ready summary for reporting. Rebuilt in full each run.
from transforms import summarize_portfolio

# COMMAND ----------

dbutils.widgets.text("catalog", "dev")
catalog = dbutils.widgets.get("catalog")

# COMMAND ----------

summarize_portfolio(spark.table(f"{catalog}.silver.loans")).createOrReplaceTempView("gold_updates")
spark.sql(f"""
INSERT OVERWRITE {catalog}.gold.loan_portfolio_summary
SELECT product, status, loan_count, total_balance, avg_balance FROM gold_updates
""")
display(spark.table(f"{catalog}.gold.loan_portfolio_summary").orderBy("product", "status"))
# Databricks notebook source
# Creates schemas and tables for one environment. Idempotent: safe to run every time.
dbutils.widgets.text("catalog", "dev")   # default for interactive runs; the job overrides it
catalog = dbutils.widgets.get("catalog")

# COMMAND ----------

for schema in ["bronze", "silver", "gold", "governance"]:
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {catalog}.bronze.loans (
  loan_id STRING, member_id STRING, ssn STRING, product STRING, status STRING,
  balance STRING, open_date_num INT, _ingested_at TIMESTAMP, _source STRING)
COMMENT 'Raw loan snapshots from core banking. Append-only; one batch per run.'
""")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {catalog}.silver.loans (
  loan_id STRING NOT NULL, member_id STRING, ssn STRING, product STRING, status STRING,
  balance DECIMAL(18,2), open_date DATE, _updated_at TIMESTAMP)
COMMENT 'Current state of each loan. One row per loan_id.'
""")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {catalog}.silver.loans_quarantine (
  loan_id STRING, member_id STRING, ssn STRING, product STRING, status STRING,
  balance DECIMAL(18,2), open_date DATE, balance_raw STRING, open_date_num INT,
  _dq_reason STRING, _quarantined_at TIMESTAMP)
COMMENT 'Rows that failed validation, kept for investigation.'
""")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {catalog}.gold.loan_portfolio_summary (
  product STRING, status STRING, loan_count BIGINT,
  total_balance DECIMAL(18,2), avg_balance DECIMAL(18,2))
COMMENT 'Loan count and balances by product and status.'
""")

# COMMAND ----------

# Governance as code: every environment gets identical tags
spark.sql(f"ALTER TABLE {catalog}.silver.loans SET TAGS ('data_owner' = 'lending', 'data_steward' = 'data_engineering')")
spark.sql(f"ALTER TABLE {catalog}.silver.loans ALTER COLUMN ssn SET TAGS ('pii' = 'ssn')")
print(f"{catalog} is ready")
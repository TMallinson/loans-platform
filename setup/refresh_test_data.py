# Databricks notebook source

from pyspark.sql import functions as F

SALT = "practice-only-salt-test"   # At work: dbutils.secrets.get("tdm", "ssn_salt")
replica = spark.table("corebank_replica.dbo.loans")

# 1. SUBSET by member so every loan for a sampled member comes along (~20% of members)
subset = replica.filter(F.pmod(F.hash("member_id"), F.lit(5)) == 0)

# 2. MASK: same real SSN -> same fake SSN, so joins still work.
#    Fakes start with 9, which real SSNs never do, so a fake can't equal a real one.
digits = F.lpad(
    F.pmod(F.xxhash64(F.concat(F.col("ssn"), F.lit(SALT))), F.lit(100_000_000)).cast("string"), 8, "0")
fake_ssn = F.concat(F.lit("9"), F.substring(digits, 1, 2), F.lit("-"),
                    F.substring(digits, 3, 2), F.lit("-"), F.substring(digits, 5, 4))
masked = subset.withColumn("ssn", F.when(F.col("ssn").isNotNull(), fake_ssn))

# 3. SYNTHESIZE the edge cases every test run must exercise
schema = ("loan_id string, member_id string, ssn string, product string, "
          "status string, balance string, open_date_num int")
edge_cases = spark.createDataFrame([
    ("T0000001", "M900001", "900-00-0001", "AUTO", "CURRENT   ", "100.00", 1250115),
    ("T0000001", "M900001", "900-00-0001", "AUTO", "CURRENT   ", "100.00", 1250115),   # duplicate
    (None,       "M900002", "900-00-0002", "AUTO", "LATE      ", "50.00", 1250115),    # null loan_id
    ("T0000002", "M900003", "900-00-0003", "HELOC", "CURRENT   ", "-10.00", 1250115),  # negative
    ("T0000003", "M900004", "900-00-0004", "AUTO", "CURRENT   ", "N/A", 1250115),     # unparseable
    ("T0000004", "M900005", "900-00-0005", "AUTO", "CURRENT   ", "75.00", 0),           # no date
    ("T0000005", "M900006", None, "PERSONAL", "PAIDOFF   ", "0.00", 991231),           # null SSN, 1999, $0
], schema)
test_source = masked.unionByName(edge_cases)

# 4. PROVE it BEFORE writing: fail closed if any real SSN survived
leaks = test_source.join(replica.select("ssn").distinct(), "ssn").count()
assert leaks == 0, f"{leaks} real SSNs would leak into non-prod. Nothing was written."

spark.sql("CREATE SCHEMA IF NOT EXISTS corebank_masked.dbo")
test_source.write.mode("overwrite").saveAsTable("corebank_masked.dbo.loans")
print(f"Refreshed {spark.table('corebank_masked.dbo.loans').count():,} rows, 0 leaks")
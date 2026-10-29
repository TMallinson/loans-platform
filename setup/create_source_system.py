# Databricks notebook source

from pyspark.sql import functions as F

N = 10_000
products = F.array(*[F.lit(p) for p in ["AUTO", "MORTGAGE", "HELOC", "PERSONAL"]])
statuses = F.array(*[F.lit(s) for s in ["CURRENT", "LATE", "PAIDOFF", "CHARGEOFF"]])
m = F.col("id") % 4000                       # 4,000 members, ~2.5 loans each
open_date = F.date_add(F.lit("2015-01-01").cast("date"), (F.rand(11) * 3650).cast("int"))

loans = spark.range(N).select(
    F.format_string("L%07d", "id").alias("loan_id"),
    F.format_string("M%06d", m).alias("member_id"),
    # One fake SSN per member, so a member's loans share an SSN
    F.format_string("%03d-%02d-%04d", 100 + m % 600, 10 + m % 89, 1000 + m).alias("ssn"),
    F.element_at(products, (F.col("id") % 4 + 1).cast("int")).alias("product"),
    F.rpad(F.element_at(statuses, (F.rand(4) * 4 + 1).cast("int")), 10, " ").alias("status"),
    F.format_string("%.2f", F.rand(5) * 50000).alias("balance"),
    (F.date_format(open_date, "yyyyMMdd").cast("int") - 19000000).alias("open_date_num"),
)

schema = ("loan_id string, member_id string, ssn string, product string, "
          "status string, balance string, open_date_num int")
dirty = spark.createDataFrame([
    ("L0000001", "M000001", "101-11-1001", "AUTO", "CURRENT   ", "1200.00", 1250115),  # duplicate loan_id
    (None,       "M000002", "102-12-1002", "AUTO", "LATE      ", "300.00", 1250115),   # null loan_id
    ("L9000001", "M000003", "103-13-1003", "HELOC", "CURRENT   ", "-50.00", 1250115),  # negative balance
    ("L9000002", "M000004", "104-14-1004", "PERSONAL", "CURRENT   ", "N/A", 1250115), # unparseable balance
    ("L9000003", "M000005", "105-15-1005", "AUTO", "CURRENT   ", "900.00", 0),        # 0 = 'no date' on iSeries
], schema)

loans.unionByName(dirty).write.mode("overwrite").saveAsTable("corebank_replica.dbo.loans")
display(spark.table("corebank_replica.dbo.loans").limit(10))
from decimal import Decimal

from quality import check_quarantine_rate, check_reconciliation, check_silver


def test_quarantine_rate_threshold():
    assert check_quarantine_rate(95, 5) == []       # exactly 5% passes
    assert check_quarantine_rate(90, 10) != []      # 10% trips the breaker
    assert check_quarantine_rate(0, 0) == ["batch is empty"]


def test_silver_catches_duplicates_and_negatives(spark):
    df = spark.createDataFrame(
        [("L1", Decimal("10.00")), ("L1", Decimal("10.00")), ("L2", Decimal("-1.00"))],
        "loan_id string, balance decimal(18,2)")
    assert check_silver(df) == ["duplicate loan_id in silver", "negative balance in silver"]


def test_reconciliation():
    assert check_reconciliation(100, 100, Decimal("50.00"), Decimal("50.00")) == []
    assert len(check_reconciliation(100, 99, Decimal("50.00"), Decimal("49.00"))) == 2
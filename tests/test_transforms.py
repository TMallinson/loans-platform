from datetime import date
from decimal import Decimal

from transforms import clean_loans, split_valid_invalid, summarize_portfolio

RAW_SCHEMA = ("loan_id string, member_id string, ssn string, product string, "
              "status string, balance string, open_date_num int")


def raw(spark, *rows):
    return spark.createDataFrame(list(rows), RAW_SCHEMA)


def loan(loan_id="L1", balance="10.00", open_date_num=1250115, status="CURRENT   "):
    return (loan_id, "M1", "900-00-0001", "AUTO", status, balance, open_date_num)


def test_parses_iseries_dates(spark):
    df = clean_loans(raw(spark, loan("L1", open_date_num=1250115), loan("L2", open_date_num=991231)))
    assert {r.loan_id: r.open_date for r in df.collect()} == {
        "L1": date(2025, 1, 15), "L2": date(1999, 12, 31)}


def test_trims_status_and_casts_balance(spark):
    row = clean_loans(raw(spark, loan(balance=" 1200.50 ", status="late      "))).first()
    assert row.status == "LATE"
    assert row.balance == Decimal("1200.50")


def test_each_bad_row_gets_exactly_one_reason(spark):
    df = clean_loans(raw(spark,
                         loan("L1"),                     # good
                         loan(None),                     # null id
                         loan("L2", balance="-5.00"),    # negative
                         loan("L3", balance="N/A"),      # unparseable
                         loan("L4", open_date_num=0)))   # no date
    valid, invalid = split_valid_invalid(df)
    assert [r.loan_id for r in valid.collect()] == ["L1"]
    assert sorted(r._dq_reason for r in invalid.collect()) == [
        "missing open_date", "negative balance", "null loan_id", "unparseable balance"]


def test_duplicates_are_removed(spark):
    valid, _ = split_valid_invalid(clean_loans(raw(spark, loan("L1"), loan("L1"))))
    assert valid.count() == 1


def test_summary_totals(spark):
    valid, _ = split_valid_invalid(clean_loans(raw(spark, loan("L1", "10.00"), loan("L2", "20.00"))))
    row = summarize_portfolio(valid).first()
    assert (row.loan_count, row.total_balance, row.avg_balance) == (2, Decimal("30.00"), Decimal("15.00"))
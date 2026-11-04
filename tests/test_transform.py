"""
Tests for the pandas transform (Part 4) - airflow/dags/quickbite_transform.py

    pytest -q tests/test_transform.py --target=student     # your version
    pytest -q tests/test_transform.py                      # the solution

Small hand-made fixtures first (easy to read when something fails), then a run over the
real 500-document seed with the exact numbers published in the handout.
"""
import math
from datetime import datetime, timezone
from decimal import Decimal

import pandas as pd
import pytest
from bson import Decimal128


# ----------------------------------------------------------------------- field level
@pytest.mark.parametrize("raw, expected", [
    ("delivered", "delivered"), ("DELIVERED", "delivered"), (" Picked_up ", "picked_up"),
    (None, None), ("", None), (42, None),
])
def test_normalize_status(qt, raw, expected):
    assert qt.normalize_status(raw) == expected


@pytest.mark.parametrize("raw, expected", [
    (Decimal128(Decimal("192.00")), 192.0),
    (88.5, 88.5),
    (120, 120.0),
    ("143.00", 143.0),
    (" 99.90 ", 99.9),
])
def test_to_float_total_good(qt, raw, expected):
    assert qt.to_float_total(raw) == pytest.approx(expected)


@pytest.mark.parametrize("raw", [None, float("nan"), "abc", True, {"x": 1}])
def test_to_float_total_bad_is_nan(qt, raw):
    assert math.isnan(qt.to_float_total(raw))


def test_order_total_coalesces_v3_payment_amount(qt):
    assert qt.order_total({"total": 50.0}) == 50.0
    assert qt.order_total({"payment": {"method": "bit", "amount": 77.0}}) == 77.0   # Part 5 drift
    assert math.isnan(qt.order_total({"payment": {"method": "bit"}}))


@pytest.mark.parametrize("raw", [
    datetime(2026, 10, 3, 8, 15, 0),                              # BSON Date (naive = UTC)
    datetime(2026, 10, 3, 8, 15, 0, tzinfo=timezone.utc),
    "2026-10-03T08:15:00Z",                                       # ISO string
    1791015300000,                                                # epoch millis
])
def test_to_utc_timestamp_three_formats(qt, raw):
    assert qt.to_utc_timestamp(raw) == pd.Timestamp("2026-10-03T08:15:00Z")


def test_to_utc_timestamp_missing(qt):
    assert qt.to_utc_timestamp(None) is pd.NaT
    assert qt.to_utc_timestamp("not a date") is pd.NaT


def test_coalesce_email_v1_v2(qt):
    assert qt.coalesce_email({"email": "Noa.Cohen1@Example.com"}) == "noa.cohen1@example.com"
    assert qt.coalesce_email({"contact": {"email": "maya@example.com", "phone": "x"}}) == "maya@example.com"
    assert qt.coalesce_email({"name": "no email"}) is None
    assert qt.coalesce_email(None) is None


@pytest.mark.parametrize("raw, expected", [
    ("Tel Aviv", "Tel Aviv"), ("tel aviv", "Tel Aviv"), ("Tel-Aviv", "Tel Aviv"), ("TLV", "Tel Aviv"),
    ("Tel Aviv ", "Tel Aviv"), ("Tel Aviv-Yafo", "Tel Aviv"),
    ("Be'er Sheva", "Be'er Sheva"), ("Beer Sheva", "Be'er Sheva"), ("Beersheba", "Be'er Sheva"),
    ("Giv'atayim", "Givatayim"), ("Ramat-Gan", "Ramat Gan"), ("Herzlia", "Herzliya"),
    ("Petach Tikva", "Petah Tikva"), ("Eilat", "Eilat"), (None, None),
])
def test_normalize_city(qt, raw, expected):
    assert qt.normalize_city(raw) == expected


# ----------------------------------------------------------------------- data quality
def _doc(order_id, status="delivered", total=100.0, updated="2026-10-05T10:40:00Z", **extra):
    d = {
        "order_id": order_id, "status": status, "total": total,
        "created_at": "2026-10-05T10:00:00Z", "promised_at": datetime(2026, 10, 5, 10, 35),
        "delivered_at": datetime(2026, 10, 5, 10, 30), "updated_at": pd.Timestamp(updated).to_pydatetime(),
        "customer": {"email": "a@example.com"}, "delivery_address": {"city": "TLV"},
        "restaurant_id": "R-01", "courier_id": "C-01", "items": [{"dish": "x", "qty": "2", "unit_price": 50.0}],
        "schema_v": 1,
    }
    d.update(extra)
    if status is None:
        d.pop("status")
    return d


def test_split_quality_rules(qt):
    docs = [
        _doc("QB-1"),
        _doc("QB-2", updated="2026-10-05T10:41:00Z"),
        _doc("QB-2", status="CANCELLED", updated="2026-10-05T11:00:00Z"),   # newer copy wins
        _doc("QB-3", status=None),                                         # missing status
        _doc("QB-4", total="oops"),                                        # bad total
        _doc("QB-5", total=-5.0),                                          # negative total
        _doc("QB-6", status=None, total=None),                             # both problems
    ]
    clean, quarantine, report = qt.split_quality(qt.flatten_orders(docs))
    assert list(clean["order_id"]) == ["QB-1", "QB-2"]
    assert clean.loc[clean.order_id == "QB-2", "status"].item() == "cancelled"
    reasons = dict(zip(quarantine["order_id"] + "|" + quarantine["reason"], [1] * len(quarantine)))
    assert "QB-2|duplicate" in reasons
    assert "QB-3|missing_status" in reasons
    assert "QB-4|bad_total" in reasons and "QB-5|bad_total" in reasons
    assert "QB-6|missing_status;bad_total" in reasons
    assert report["duplicate_rows"] == 1 and report["duplicate_order_ids"] == ["QB-2"]
    assert report["missing_status"] == 2 and report["bad_total"] == 3
    assert report["bad_pct"] == round(100 * 4 / 7, 2)


def test_quality_decision(qt):
    assert qt.quality_decision({"bad_pct": 7.2, "duplicate_rows": 2}, 10, 5) is True
    assert qt.quality_decision({"bad_pct": 12.0, "duplicate_rows": 0}, 10, 5) is False
    assert qt.quality_decision({"bad_pct": 0.0, "duplicate_rows": 6}, 10, 5) is False


def test_is_late_and_item_qty(qt):
    df = qt.flatten_orders([
        _doc("QB-1"),                                                       # 10:30 <= 10:35 on time
        _doc("QB-2", delivered_at=datetime(2026, 10, 5, 10, 50)),           # late
        _doc("QB-3", status="cancelled", delivered_at=None),
    ])
    assert list(df["is_late"]) == [False, True, False]
    assert list(df["item_qty"]) == [2, 2, 2]


# ----------------------------------------------------------------------- silver + gold
def _restaurants():
    return pd.DataFrame([{"restaurant_id": "R-01", "name": "Hummus Abu Dani", "commission_pct": 18.0}])


def test_merge_silver_is_an_idempotent_upsert(qt):
    first = qt.flatten_orders([_doc("QB-1", status="picked_up", delivered_at=None, updated="2026-10-05T10:20:00Z")])
    second = qt.flatten_orders([_doc("QB-1"), _doc("QB-2")])
    merged = qt.merge_silver(first, second)
    assert list(merged["order_id"]) == ["QB-1", "QB-2"]
    assert merged.loc[0, "status"] == "delivered"
    again = qt.merge_silver(merged, second)                    # re-run the same batch
    pd.testing.assert_frame_equal(merged, again)


def test_gold_tables(qt):
    silver = qt.flatten_orders([
        _doc("QB-1"),                                                        # on time
        _doc("QB-2", delivered_at=datetime(2026, 10, 5, 10, 50)),            # late
        _doc("QB-3", status="cancelled", delivered_at=None),                 # not paid out
    ])
    payouts = qt.courier_payouts(silver)
    assert payouts.to_dict("records") == [{"date": "2026-10-05", "courier_id": "C-01", "deliveries": 2,
                                           "on_time": 1, "late": 1, "payout_ils": 33.0}]
    fees = qt.restaurant_fees(silver, _restaurants())
    row = fees.iloc[0]
    assert (row.orders, row.gross_ils, row.fee_ils, row.net_ils) == (2, 200.0, 36.0, 164.0)
    summary = qt.daily_summary(silver, payouts, fees)
    s = summary.iloc[0]
    assert (s.orders, s.delivered, s.cancelled, s.late_deliveries, s.late_pct) == (3, 2, 1, 1, 50.0)
    assert (s.revenue_ils, s.fees_ils, s.courier_payouts_ils) == (200.0, 36.0, 33.0)


def test_silver_csv_roundtrip(qt, tmp_path):
    silver = qt.flatten_orders([_doc("QB-1"), _doc("QB-2", courier_id=None, status="cancelled", delivered_at=None)])
    path = tmp_path / "s.csv"
    silver.to_csv(path, index=False)
    back = qt.read_silver_csv(path)
    assert list(back["order_id"]) == ["QB-1", "QB-2"]
    assert back["is_late"].dtype == bool
    assert str(back["created_at"].dt.tz) == "UTC"


def test_summary_docs_are_json_friendly(qt):
    import json
    silver = qt.flatten_orders([_doc("QB-1")])
    p = qt.courier_payouts(silver)
    f = qt.restaurant_fees(silver, _restaurants())
    docs = qt.summary_docs(qt.daily_summary(silver, p, f), {"run_id": "manual__x"})
    json.dumps(docs)                                           # must not raise
    assert docs[0]["date"] == "2026-10-05" and docs[0]["run_id"] == "manual__x"


# ----------------------------------------------------------------------- the real seed
def test_seed_expected_numbers(qt, seed_docs):
    """The numbers published in handout Part 4 ("checkpoint") for the first DAG run."""
    df = qt.flatten_orders(seed_docs)
    clean, quarantine, report = qt.split_quality(df)
    assert report == {
        "input_rows": 500, "clean_rows": 462, "quarantined_rows": 38, "duplicate_rows": 2,
        "duplicate_order_ids": ["QB-100040", "QB-100041"], "missing_status": 18, "bad_total": 18,
        "empty_items": 10, "bad_pct": 7.2,
    }
    assert df["city"].nunique() == 8
    assert df["email"].notna().all()
    assert df["created_at"].notna().all()

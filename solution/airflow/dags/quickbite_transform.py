"""
SOLUTION - quickbite_transform.py: the pandas "T" of the QuickBite hourly pipeline.
=====================================================================================
This file holds ONLY plain Python + pandas functions (no Airflow, no MongoDB, no Elasticsearch),
so it is easy to read, test (see tests/) and replace.

    In the Spark module you will replace this pandas transform with PySpark:
    the same steps (flatten, normalize, quality checks, group-by) become DataFrame
    operations that run on a cluster instead of one Python process.

Medallion vocabulary used below:
    raw     = the documents exactly as MongoDB returned them (messy)
    silver  = one clean row per order (types fixed, names normalized, duplicates removed)
    gold    = business tables that Finance uses (courier payouts, restaurant fees)
"""
import math
import re
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation

import pandas as pd

# ----- Business rules (Finance gave us these numbers) ---------------------------------
COURIER_BASE_FEE_ILS = 15.0       # paid per delivered order
COURIER_ON_TIME_BONUS_ILS = 3.0   # extra when delivered_at <= promised_at
VALID_STATUSES = {"created", "accepted", "picked_up", "delivered", "cancelled"}

# ----- City normalization ------------------------------------------------------------------
# Key = the spelling after city_key() (lower case, no apostrophes, "-" -> " ", single spaces).
CITY_ALIASES = {
    "tel aviv": "Tel Aviv", "tlv": "Tel Aviv", "tel aviv yafo": "Tel Aviv",
    "ramat gan": "Ramat Gan",
    "givatayim": "Givatayim",
    "holon": "Holon",
    "herzliya": "Herzliya", "herzlia": "Herzliya",
    "petah tikva": "Petah Tikva", "petach tikva": "Petah Tikva", "petah tiqva": "Petah Tikva",
    "beer sheva": "Be'er Sheva", "beersheba": "Be'er Sheva",
    "haifa": "Haifa",
}

SILVER_COLUMNS = [
    "order_id", "status", "total", "created_at", "updated_at", "promised_at", "delivered_at",
    "email", "city", "restaurant_id", "courier_id", "n_items", "item_qty", "schema_v",
    "cancel_reason", "is_late", "delivery_minutes",
]
DATE_COLUMNS = ["created_at", "updated_at", "promised_at", "delivered_at"]


# =====================================================================================
# 1. Field-level normalizers (one messy value in -> one clean value out)
# =====================================================================================
def _is_missing(value):
    return value is None or (isinstance(value, float) and math.isnan(value))


def normalize_status(value):
    """'DELIVERED' / ' Delivered' -> 'delivered'. Missing or not a string -> None."""
    if not isinstance(value, str) or not value.strip():
        return None
    return value.strip().lower()


def to_float_total(value):
    """
    total arrives as Decimal128 / double / int / string / missing.
    Returns a float, or NaN when it is missing or cannot be parsed.

    Decimal128 is BSON's exact decimal type (good for money). pymongo returns it as
    bson.Decimal128, which pandas does not understand -> convert via .to_decimal().
    We do not import bson here (keeps this file pure); we "duck-type" on to_decimal().
    """
    if _is_missing(value) or isinstance(value, bool):
        return float("nan")
    if hasattr(value, "to_decimal"):                # bson.Decimal128
        value = value.to_decimal()
    if isinstance(value, Decimal):
        return float(value) if value.is_finite() else float("nan")
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        try:
            return float(Decimal(value.strip().replace(",", "")))
        except InvalidOperation:
            return float("nan")
    return float("nan")


def order_total(doc):
    """
    Part 5 (drift) fix: the "v3 release" moved the money from `total` to `payment.amount`.
    COALESCE: use `total` when present, else `payment.amount`. (Like COALESCE() in SQL.)
    """
    total = doc.get("total")
    if _is_missing(total):
        total = (doc.get("payment") or {}).get("amount")
    return to_float_total(total)


def to_utc_timestamp(value):
    """
    created_at arrives in 3 formats:
      - BSON Date        -> pymongo gives a datetime (naive, but it IS UTC)
      - ISO string       -> '2026-10-03T08:15:00Z'
      - epoch millis     -> 1791015300000  (an integer!)
    Returns a tz-aware pandas Timestamp in UTC, or NaT.
    """
    if _is_missing(value) or isinstance(value, bool):
        return pd.NaT
    try:
        if isinstance(value, datetime):
            ts = pd.Timestamp(value)
            return ts.tz_localize("UTC") if ts.tzinfo is None else ts.tz_convert("UTC")
        if isinstance(value, (int, float)):
            # heuristic: > 1e11 means milliseconds (seconds would be the year 5138)
            unit = "ms" if abs(value) > 1e11 else "s"
            return pd.Timestamp(int(value), unit=unit, tz="UTC")
        if isinstance(value, str):
            return pd.Timestamp(value.strip()).tz_convert("UTC") if _has_tz(value) \
                else pd.Timestamp(value.strip()).tz_localize("UTC")
    except (ValueError, OverflowError):
        return pd.NaT
    return pd.NaT


def _has_tz(text):
    return text.strip().endswith("Z") or bool(re.search(r"[+-]\d\d:?\d\d$", text.strip()))


def coalesce_email(customer):
    """schema v1: customer.email   |   schema v2+: customer.contact.email"""
    customer = customer or {}
    email = customer.get("email") or (customer.get("contact") or {}).get("email")
    return email.strip().lower() if isinstance(email, str) and email.strip() else None


def city_key(value):
    text = value.lower().strip().replace("'", "").replace("-", " ")
    return re.sub(r"\s+", " ", text)


def normalize_city(value):
    """'TLV' / 'tel aviv' / 'Tel-Aviv' -> 'Tel Aviv'. Unknown -> stripped original. Missing -> None."""
    if not isinstance(value, str) or not value.strip():
        return None
    return CITY_ALIASES.get(city_key(value), value.strip())


def item_quantity(items):
    """Sum of qty over the items array. qty is sometimes a string ("2") -> int()."""
    total = 0
    for it in items or []:
        try:
            total += int(it.get("qty") or 0)
        except (TypeError, ValueError):
            pass
    return total


# =====================================================================================
# 2. Raw documents -> one flat DataFrame
# =====================================================================================
def flatten_orders(docs):
    """List of raw Mongo documents -> DataFrame with SILVER_COLUMNS (not yet deduplicated)."""
    rows = []
    for d in docs:
        items = d.get("items") if isinstance(d.get("items"), list) else []
        rows.append({
            "order_id": d.get("order_id"),
            "status": normalize_status(d.get("status")),
            "total": order_total(d),
            "created_at": to_utc_timestamp(d.get("created_at")),
            "updated_at": to_utc_timestamp(d.get("updated_at")),
            "promised_at": to_utc_timestamp(d.get("promised_at")),
            "delivered_at": to_utc_timestamp(d.get("delivered_at")),
            "email": coalesce_email(d.get("customer")),
            "city": normalize_city((d.get("delivery_address") or {}).get("city")),
            "restaurant_id": d.get("restaurant_id"),
            "courier_id": d.get("courier_id"),
            "n_items": len(items),
            "item_qty": item_quantity(items),
            "schema_v": d.get("schema_v"),
            "cancel_reason": d.get("cancel_reason"),
        })
    df = pd.DataFrame(rows, columns=[c for c in SILVER_COLUMNS if c not in ("is_late", "delivery_minutes")])
    for col in DATE_COLUMNS:
        df[col] = pd.to_datetime(df[col], utc=True)
    df["total"] = df["total"].astype("float64")
    df["is_late"] = (df["status"] == "delivered") & df["delivered_at"].notna() & \
        (df["delivered_at"] > df["promised_at"])
    df["delivery_minutes"] = ((df["delivered_at"] - df["created_at"]).dt.total_seconds() / 60).round(1)
    return df[SILVER_COLUMNS]


# =====================================================================================
# 3. Data quality: split into clean rows + quarantined rows, count the problems
# =====================================================================================
def split_quality(df):
    """
    Returns (clean_df, quarantine_df, report).

      - duplicates      same order_id more than once in this batch -> keep the LATEST
                        (by updated_at), the extra copies go to quarantine
      - missing_status  status is missing / not one of the 5 official values -> quarantine
      - bad_total       total is missing, not a number, or negative -> quarantine
      - empty_items     0 items (warning only - a cancelled order can be empty)

    A row can have several problems; `reason` lists all of them ("missing_status;bad_total").
    """
    df = df.copy()
    n_input = len(df)

    # --- duplicates: sort so the latest version is last, then mark the earlier copies
    df = df.sort_values(["order_id", "updated_at"], kind="mergesort", na_position="first")
    is_dup = df.duplicated(subset="order_id", keep="last")
    duplicate_ids = sorted(df.loc[is_dup, "order_id"].dropna().unique().tolist())

    missing_status = ~df["status"].isin(VALID_STATUSES)
    bad_total = df["total"].isna() | (df["total"] < 0)

    reasons = pd.Series("", index=df.index)
    reasons = reasons.where(~is_dup, reasons + "duplicate;")
    reasons = reasons.where(~missing_status, reasons + "missing_status;")
    reasons = reasons.where(~bad_total, reasons + "bad_total;")
    reasons = reasons.str.rstrip(";")

    bad = reasons != ""
    quarantine = df[bad].copy()
    quarantine["reason"] = reasons[bad]
    clean = df[~bad].sort_values("order_id").reset_index(drop=True)
    quarantine = quarantine.sort_values("order_id").reset_index(drop=True)

    report = {
        "input_rows": int(n_input),
        "clean_rows": int(len(clean)),
        "quarantined_rows": int(len(quarantine)),
        "duplicate_rows": int(is_dup.sum()),
        "duplicate_order_ids": duplicate_ids,
        "missing_status": int(missing_status.sum()),
        "bad_total": int(bad_total.sum()),
        "empty_items": int((df["n_items"] == 0).sum()),
        # duplicates are "expected noise" (we keep one copy), so they do not count as bad rows
        "bad_pct": round(100.0 * int((missing_status | bad_total).sum()) / n_input, 2) if n_input else 0.0,
    }
    return clean, quarantine, report


def quality_decision(report, max_bad_pct, max_duplicates):
    """Pure helper used by the BranchPythonOperator: True = data is good enough to publish."""
    return report["bad_pct"] <= max_bad_pct and report["duplicate_rows"] <= max_duplicates


# =====================================================================================
# 4. Silver merge (incremental upsert) and Gold tables
# =====================================================================================
def merge_silver(existing, new_rows):
    """
    UPSERT by order_id: the incremental extract only brings orders that CHANGED since the
    last run. We add them to what we already have and keep the newest version of each order.
    Re-running the same window gives the same result (idempotent).
    """
    if existing is None or existing.empty:
        merged = new_rows.copy()
    else:
        merged = pd.concat([existing, new_rows], ignore_index=True)
    merged = merged.sort_values(["order_id", "updated_at"], kind="mergesort", na_position="first")
    merged = merged.drop_duplicates(subset="order_id", keep="last")
    return merged.sort_values("order_id").reset_index(drop=True)[SILVER_COLUMNS]


def _delivered(silver):
    d = silver[(silver["status"] == "delivered") & silver["delivered_at"].notna()].copy()
    d["date"] = d["delivered_at"].dt.strftime("%Y-%m-%d")       # UTC calendar day
    return d


def courier_payouts(silver):
    """Gold #1 - what we owe each courier per day."""
    d = _delivered(silver)
    d = d[d["courier_id"].notna()]
    if d.empty:
        return pd.DataFrame(columns=["date", "courier_id", "deliveries", "on_time", "late", "payout_ils"])
    g = (d.assign(on_time=~d["is_late"], late=d["is_late"])
         .groupby(["date", "courier_id"], as_index=False)
         .agg(deliveries=("order_id", "count"), on_time=("on_time", "sum"), late=("late", "sum")))
    g["on_time"] = g["on_time"].astype(int)
    g["late"] = g["late"].astype(int)
    g["payout_ils"] = (g["deliveries"] * COURIER_BASE_FEE_ILS + g["on_time"] * COURIER_ON_TIME_BONUS_ILS).round(2)
    return g.sort_values(["date", "courier_id"]).reset_index(drop=True)


def restaurant_fees(silver, restaurants):
    """
    Gold #2 - per restaurant per day: gross revenue, QuickBite's commission (fee) and the net.
    `restaurants` is a DataFrame with restaurant_id, name, commission_pct (from quickbite.restaurants).
    """
    cols = ["date", "restaurant_id", "restaurant_name", "orders", "gross_ils", "commission_pct", "fee_ils", "net_ils"]
    d = _delivered(silver)
    if d.empty:
        return pd.DataFrame(columns=cols)
    g = d.groupby(["date", "restaurant_id"], as_index=False).agg(orders=("order_id", "count"),
                                                                gross_ils=("total", "sum"))
    dim = restaurants[["restaurant_id", "name", "commission_pct"]].rename(columns={"name": "restaurant_name"})
    g = g.merge(dim, on="restaurant_id", how="left")
    g["gross_ils"] = g["gross_ils"].round(2)
    g["fee_ils"] = (g["gross_ils"] * g["commission_pct"] / 100).round(2)
    g["net_ils"] = (g["gross_ils"] - g["fee_ils"]).round(2)
    return g[cols].sort_values(["date", "restaurant_id"]).reset_index(drop=True)


def daily_summary(silver, payouts, fees):
    """One row per UTC day - indexed to Elasticsearch `daily-summary` for the Kibana dashboard."""
    s = silver[silver["created_at"].notna()].copy()
    s["date"] = s["created_at"].dt.strftime("%Y-%m-%d")
    base = s.groupby("date").agg(
        orders=("order_id", "count"),
        delivered=("status", lambda x: int((x == "delivered").sum())),
        cancelled=("status", lambda x: int((x == "cancelled").sum())),
        late_deliveries=("is_late", "sum"),
        avg_delivery_minutes=("delivery_minutes", "mean"),
        # Part 5 (errors) addition: Finance wants to see how many orders failed at payment
        payment_failed=("cancel_reason", lambda x: int((x == "payment_failed").sum())),
    )
    rev = fees.groupby("date").agg(revenue_ils=("gross_ils", "sum"), fees_ils=("fee_ils", "sum")) \
        if not fees.empty else pd.DataFrame(columns=["revenue_ils", "fees_ils"])
    pay = payouts.groupby("date").agg(courier_payouts_ils=("payout_ils", "sum")) \
        if not payouts.empty else pd.DataFrame(columns=["courier_payouts_ils"])
    out = base.join(rev, how="left").join(pay, how="left").reset_index()
    out[["revenue_ils", "fees_ils", "courier_payouts_ils"]] = \
        out[["revenue_ils", "fees_ils", "courier_payouts_ils"]].astype(float).fillna(0.0).round(2)
    out["late_deliveries"] = out["late_deliveries"].astype(int)
    out["late_pct"] = (100.0 * out["late_deliveries"] / out["delivered"].where(out["delivered"] > 0)).round(1).fillna(0.0)
    out["avg_delivery_minutes"] = out["avg_delivery_minutes"].round(1)
    return out.sort_values("date").reset_index(drop=True)


# =====================================================================================
# 5. CSV round-trip helpers (Silver is stored as CSV between runs)
# =====================================================================================
def read_silver_csv(path):
    df = pd.read_csv(path, dtype={"order_id": str, "status": str, "restaurant_id": str,
                                  "courier_id": str, "email": str, "city": str, "cancel_reason": str})
    for col in DATE_COLUMNS:
        df[col] = pd.to_datetime(df[col], utc=True, format="ISO8601")
    df["is_late"] = df["is_late"].astype(str).str.lower().eq("true")
    return df[SILVER_COLUMNS]


def summary_docs(summary_df, run_info):
    """DataFrame rows -> JSON-friendly dicts (NaN -> None) with run metadata attached."""
    docs = []
    for row in summary_df.to_dict(orient="records"):
        clean = {k: (None if isinstance(v, float) and math.isnan(v) else v) for k, v in row.items()}
        for k, v in list(clean.items()):
            if hasattr(v, "item"):                  # numpy int64 / float64 -> plain Python
                clean[k] = v.item()
        clean.update(run_info)
        docs.append(clean)
    return docs


def utc_now_iso():
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"

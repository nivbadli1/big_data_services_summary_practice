"""
The EXACT numbers published in handout Part 1 - checked three ways:

  1. plain Python over seed_orders.build_docs()         (always runs)
  2. the same Mongo queries in mongomock (an in-memory fake MongoDB) where it supports them
  3. the same queries against a REAL MongoDB, if MONGO_URI is set   (pytest -m mongo)
       docker compose exec apps pytest -q /tests/test_seed_expected.py -m mongo
     WARNING: (3) DROPS and re-seeds quickbite.orders.

If you change the seed generator, these tests tell you which handout numbers to update.
"""
import os
from collections import Counter
from decimal import Decimal

import pytest
from bson import Decimal128

# ------------------------------------------------------------------ expected values
EXPECTED = {
    "count": 500,
    "distinct_status_raw": 14,
    "total_types": {"decimal": 323, "double": 120, "string": 39, "missing": 18},
    "created_at_types": {"date": 427, "string": 43, "long": 30},
    "missing_status": 18,
    "schema_v": {1: 241, 2: 259},
    "empty_items": 10,
    "string_qty_orders": 85,
    "distinct_cities": 23,
    "duplicates": {"QB-100040": 2, "QB-100041": 2},
    "delivered_before_fix": 334,
    "status_fix_modified": 57,
    "delivered_after_fix": 374,
    "per_status_after_fix": {"delivered": 374, "cancelled": 50, "created": 24, "picked_up": 20, None: 18, "accepted": 14},
    "revenue_groups": 23,
    "revenue_top2": [("Tel Aviv", 70, "11010.00"), ("Herzliya", 36, "10522.00")],
    "lower_trim_groups": 18,
    "lower_trim_tel_aviv": 98,
    "top_restaurants": [("R-15", "Marina Fish", 27, "8634.00"), ("R-03", "Pizza Yafo", 28, "5622.00"),
                        ("R-16", "Pituah Poke", 25, "4902.00"), ("R-18", "Hadar Schnitzel", 19, "4551.00"),
                        ("R-13", "Holon Home Food", 18, "4343.00")],
    "top_dishes": [("Pizza Funghi", 90), ("Schnitzel Plate", 72), ("Garlic Bread", 66), ("Sabich", 60),
                   ("Shawarma Laffa", 58)],
    "late_delivered": 117,
    "updated_after_oct29_no_fix": 38,       # bonus 1.5 if you skipped the 1.3 fix
    "updated_after_oct29_after_fix": 88,    # bonus 1.5 after the 1.3 fix (it moved updated_at to NOW on 57 docs)
}


def bson_type(v):
    from datetime import datetime
    if isinstance(v, Decimal128):
        return "decimal"
    if isinstance(v, bool):
        return "bool"
    if isinstance(v, float):
        return "double"
    if isinstance(v, int):
        return "long" if abs(v) > 2**31 else "int"
    if isinstance(v, str):
        return "string"
    if isinstance(v, datetime):
        return "date"
    return type(v).__name__


def money(v):
    """$convert to decimal with onError 0 / onNull 0."""
    if v is None:
        return Decimal(0)
    if isinstance(v, Decimal128):
        return v.to_decimal()
    try:
        return Decimal(str(v))
    except Exception:  # noqa: BLE001
        return Decimal(0)


def lower_status(docs):
    out = []
    for d in docs:
        d = dict(d)
        if isinstance(d.get("status"), str):
            d["status"] = d["status"].lower()
        out.append(d)
    return out


# ------------------------------------------------------------------ 1. plain Python
def test_python_profile(seed_docs):
    docs = seed_docs
    assert len(docs) == EXPECTED["count"]
    assert len({d["status"] for d in docs if "status" in d}) == EXPECTED["distinct_status_raw"]
    assert Counter(bson_type(d["total"]) if "total" in d else "missing" for d in docs) == EXPECTED["total_types"]
    assert Counter(bson_type(d["created_at"]) for d in docs) == EXPECTED["created_at_types"]
    assert sum("status" not in d for d in docs) == EXPECTED["missing_status"]
    assert Counter(d["schema_v"] for d in docs) == EXPECTED["schema_v"]
    assert sum(1 for d in docs if "email" in d["customer"]) == EXPECTED["schema_v"][1]
    assert sum(1 for d in docs if "email" in d["customer"].get("contact", {})) == EXPECTED["schema_v"][2]
    assert sum(d["items"] == [] for d in docs) == EXPECTED["empty_items"]
    assert sum(any(isinstance(i["qty"], str) for i in d["items"]) for d in docs) == EXPECTED["string_qty_orders"]
    assert len({d["delivery_address"]["city"] for d in docs}) == EXPECTED["distinct_cities"]
    dup = {k: v for k, v in Counter(d["order_id"] for d in docs).items() if v > 1}
    assert dup == EXPECTED["duplicates"]
    assert sum(d.get("status") == "delivered" for d in docs) == EXPECTED["delivered_before_fix"]
    assert sum(isinstance(d.get("status"), str) and d["status"] != d["status"].lower() for d in docs) \
        == EXPECTED["status_fix_modified"]
    # every document has a Date updated_at (the Airflow watermark depends on it)
    assert all(bson_type(d["updated_at"]) == "date" for d in docs)


def test_python_aggregations(seed_docs):
    docs = lower_status(seed_docs)
    assert dict(Counter(d.get("status") for d in docs)) == EXPECTED["per_status_after_fix"]
    delivered = [d for d in docs if d.get("status") == "delivered"]

    rev, cnt = Counter(), Counter()
    for d in delivered:
        c = d["delivery_address"]["city"]
        rev[c] += money(d.get("total"))
        cnt[c] += 1
    ranked = sorted(rev, key=lambda c: -rev[c])
    assert len(ranked) == EXPECTED["revenue_groups"]
    assert [(c, cnt[c], f"{rev[c]:.2f}") for c in ranked[:2]] == EXPECTED["revenue_top2"]

    lt = Counter(d["delivery_address"]["city"].strip().lower() for d in delivered)
    assert len(lt) == EXPECTED["lower_trim_groups"] and lt["tel aviv"] == EXPECTED["lower_trim_tel_aviv"]

    rrev, rcnt, names = Counter(), Counter(), {}
    for d in delivered:
        rrev[d["restaurant_id"]] += money(d.get("total"))
        rcnt[d["restaurant_id"]] += 1
        names[d["restaurant_id"]] = d["restaurant_name"]
    top = sorted(rrev, key=lambda r: -rrev[r])[:5]
    assert [(r, names[r], rcnt[r], f"{rrev[r]:.2f}") for r in top] == EXPECTED["top_restaurants"]

    dish = Counter()
    for d in delivered:
        for it in d["items"]:
            dish[it["dish"]] += int(it["qty"])
    assert sorted(dish.items(), key=lambda kv: (-kv[1], kv[0]))[:5] == EXPECTED["top_dishes"]

    assert sum(d["delivered_at"] > d["promised_at"] for d in delivered) == EXPECTED["late_delivered"]

    from datetime import datetime, timezone
    cut = datetime(2026, 10, 29, tzinfo=timezone.utc)
    assert sum(d["updated_at"] > cut for d in seed_docs) == EXPECTED["updated_after_oct29_no_fix"]
    # after the fix: the 57 re-cased documents have updated_at = NOW (> cut)
    fixed = [isinstance(d.get("status"), str) and d["status"] != d["status"].lower() for d in seed_docs]
    assert sum(f or d["updated_at"] > cut for f, d in zip(fixed, seed_docs)) == EXPECTED["updated_after_oct29_after_fix"]


# ------------------------------------------------------------------ 2 + 3. Mongo queries
def run_queries(coll):
    """The handout's Part 1 queries (same as solution/part1_mongo/explore.js). Returns a dict."""
    r = {}
    r["count"] = coll.count_documents({})
    r["missing_status"] = coll.count_documents({"status": {"$exists": False}})
    r["schema_v"] = {g["_id"]: g["n"] for g in coll.aggregate([{"$group": {"_id": "$schema_v", "n": {"$sum": 1}}}])}
    r["v1_email"] = coll.count_documents({"customer.email": {"$exists": True}})
    r["v2_email"] = coll.count_documents({"customer.contact.email": {"$exists": True}})
    r["empty_items"] = coll.count_documents({"items": {"$size": 0}})
    r["distinct_cities"] = len(coll.distinct("delivery_address.city"))
    r["distinct_status_raw"] = len(coll.distinct("status"))
    r["duplicates"] = {g["_id"]: g["n"] for g in coll.aggregate([
        {"$group": {"_id": "$order_id", "n": {"$sum": 1}}}, {"$match": {"n": {"$gt": 1}}}])}
    r["delivered_before_fix"] = coll.count_documents({"status": "delivered"})
    return r


def run_full_queries(coll):
    """Queries that need operators mongomock does not fully support ($type agg, $convert, $expr ...)."""
    r = {}
    r["total_types"] = {g["_id"]: g["n"] for g in coll.aggregate([{"$group": {"_id": {"$type": "$total"}, "n": {"$sum": 1}}}])}
    r["created_at_types"] = {g["_id"]: g["n"] for g in coll.aggregate([{"$group": {"_id": {"$type": "$created_at"}, "n": {"$sum": 1}}}])}
    r["string_qty_orders"] = coll.count_documents({"items.qty": {"$type": "string"}})
    res = coll.update_many({"status": {"$type": "string"}, "$expr": {"$ne": ["$status", {"$toLower": "$status"}]}},
                           [{"$set": {"status": {"$toLower": "$status"}, "updated_at": "$$NOW"}}])
    r["status_fix_modified"] = res.modified_count
    r["delivered_after_fix"] = coll.count_documents({"status": "delivered"})
    r["per_status_after_fix"] = {g["_id"]: g["n"] for g in coll.aggregate([{"$group": {"_id": "$status", "n": {"$sum": 1}}}])}
    conv = {"$convert": {"input": "$total", "to": "decimal", "onError": 0, "onNull": 0}}
    rev = list(coll.aggregate([
        {"$match": {"status": "delivered"}},
        {"$group": {"_id": "$delivery_address.city", "orders": {"$sum": 1}, "revenue": {"$sum": conv}}},
        {"$set": {"revenue": {"$round": ["$revenue", 2]}}}, {"$sort": {"revenue": -1}}]))
    r["revenue_groups"] = len(rev)
    r["revenue_top2"] = [(g["_id"], g["orders"], f"{g['revenue'].to_decimal():.2f}") for g in rev[:2]]
    lt = list(coll.aggregate([{"$match": {"status": "delivered"}},
                              {"$group": {"_id": {"$toLower": {"$trim": {"input": "$delivery_address.city"}}}, "n": {"$sum": 1}}}]))
    r["lower_trim_groups"] = len(lt)
    r["lower_trim_tel_aviv"] = next(g["n"] for g in lt if g["_id"] == "tel aviv")
    top = list(coll.aggregate([
        {"$match": {"status": "delivered"}},
        {"$group": {"_id": {"id": "$restaurant_id", "name": "$restaurant_name"}, "orders": {"$sum": 1}, "revenue": {"$sum": conv}}},
        {"$set": {"revenue": {"$round": ["$revenue", 2]}}}, {"$sort": {"revenue": -1}}, {"$limit": 5}]))
    r["top_restaurants"] = [(g["_id"]["id"], g["_id"]["name"], g["orders"], f"{g['revenue'].to_decimal():.2f}") for g in top]
    dishes = list(coll.aggregate([
        {"$match": {"status": "delivered"}}, {"$unwind": "$items"},
        {"$group": {"_id": "$items.dish", "qty": {"$sum": {"$toInt": "$items.qty"}}}},
        {"$sort": {"qty": -1, "_id": 1}}, {"$limit": 5}]))
    r["top_dishes"] = [(g["_id"], g["qty"]) for g in dishes]
    r["late_delivered"] = coll.count_documents({"status": "delivered", "$expr": {"$gt": ["$delivered_at", "$promised_at"]}})
    from datetime import datetime
    r["updated_after_oct29_after_fix"] = coll.count_documents({"updated_at": {"$gt": datetime(2026, 10, 29)}})
    return r


def _check(r):
    for key, value in r.items():
        if key in ("v1_email", "v2_email"):
            continue
        assert value == EXPECTED[key], key
    if "v1_email" in r:
        assert (r["v1_email"], r["v2_email"]) == (EXPECTED["schema_v"][1], EXPECTED["schema_v"][2])


def test_mongomock_basic_queries(seed_docs):
    mongomock = pytest.importorskip("mongomock")
    coll = mongomock.MongoClient()["quickbite"]["orders"]
    coll.insert_many([dict(d) for d in seed_docs])
    _check(run_queries(coll))


@pytest.mark.mongo
def test_real_mongodb_all_queries(seed_module):
    uri = os.getenv("MONGO_URI")
    if not uri:
        pytest.skip("set MONGO_URI to run against a real MongoDB")
    from pymongo import MongoClient
    coll = MongoClient(uri, serverSelectionTimeoutMS=5000)["quickbite"]["orders"]
    coll.drop()
    coll.insert_many(seed_module.build_docs())
    _check(run_queries(coll))
    _check(run_full_queries(coll))
    coll.drop()
    coll.insert_many(seed_module.build_docs())            # leave a fresh seed behind

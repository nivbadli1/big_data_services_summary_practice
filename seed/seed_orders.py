"""
seed_orders.py - loads ~500 HISTORICAL QuickBite orders into MongoDB (quickbite.orders).

The data is intentionally MESSY, exactly like in the MongoDB lesson. This is the point:
you will find these problems yourself in Part 1 and clean them in Part 4 (Airflow + pandas).

  - total:       Decimal128 / double / string / missing
  - status:      mixed case ("delivered", "DELIVERED", "Delivered") / missing
  - created_at:  BSON Date / ISO string / epoch-millis integer
  - customer:    schema v1 has customer.email,
                 schema v2 (app release of 2026-10-16) moved it to customer.contact.email (+ phone)
  - items:       array of 0-4 sub-documents; qty sometimes a string ("2")
  - city:        "Tel Aviv" / "tel aviv" / "Tel-Aviv" / "TLV" / "Be'er Sheva" / "Beer Sheva" ...
  - 2 duplicated order_id values (a retry bug in the old app inserted the same order twice)
  - restaurant_id, courier_id, promised_at, delivered_at, updated_at (a Date on EVERY document)

The generator is DETERMINISTIC (fixed seed): every student gets exactly the same 500 documents,
so the expected outputs in handout.md match your screen.

Run it inside the `apps` container (see handout Part 0):
    docker compose exec apps python /seed/seed_orders.py
    docker compose exec apps python /seed/seed_orders.py --json-only   # only writes orders.json

The connection string comes from $MONGO_URI (set in docker-compose.yml), else localhost.
"""
import argparse
import copy
import os
import random
import sys
from datetime import datetime, timedelta, timezone
from decimal import Decimal

from bson import Decimal128, ObjectId
from bson.json_util import JSONMode, JSONOptions, dumps

# The catalog lives in apps/common. Inside the container it is on PYTHONPATH (/apps);
# when you run this file from the repo folder on your laptop we add ../apps ourselves.
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "apps"))
from common.catalog import (CITY_SPELLINGS, COURIERS, FIRST_NAMES, LAST_NAMES,  # noqa: E402
                            MENU, RESTAURANTS, STREETS)

SEED = 2026
N_UNIQUE = 498                                              # + 2 duplicates = 500 documents
START = datetime(2026, 10, 1, tzinfo=timezone.utc)          # history starts 1 Oct 2026
DAYS = 30                                                   # ... and covers 30 days
SCHEMA_V2_FROM = datetime(2026, 10, 16, tzinfo=timezone.utc)  # app release that moved email
PROMISE_MINUTES = 35                                        # "your food arrives in 35 min"
PAYMENT_METHODS = ["credit_card", "apple_pay", "google_pay", "bit", "cash"]


def _ms(dt):
    """Round a datetime to whole milliseconds (BSON Dates keep milliseconds only)."""
    return dt.replace(microsecond=(dt.microsecond // 1000) * 1000)


def build_docs():
    rnd = random.Random(SEED)
    docs = []
    for i in range(N_UNIQUE):
        created = _ms(START + timedelta(seconds=rnd.randint(0, DAYS * 24 * 3600 - 1)))
        rest_id, rest_name, rest_city, cuisine, _commission = rnd.choice(RESTAURANTS)
        city_spelling = rnd.choice(CITY_SPELLINGS[rest_city])
        first, last = rnd.choice(FIRST_NAMES), rnd.choice(LAST_NAMES)
        cust_no = rnd.randint(1, 120)

        # ---- items (array of sub-documents) -----------------------------------
        n_items = 0 if i % 50 == 7 else rnd.choice([1, 1, 2, 2, 2, 3, 4])   # 10 orders: empty!
        items = []
        for _ in range(n_items):
            dish, price = rnd.choice(MENU[cuisine])
            qty = rnd.randint(1, 3)
            items.append({
                "dish": dish,
                "qty": str(qty) if rnd.random() < 0.08 else qty,   # mixed type!
                "unit_price": price,
            })
        amount = round(sum(int(it["qty"]) * it["unit_price"] for it in items), 2)

        # ---- lifecycle --------------------------------------------------------
        r = rnd.random()
        if r < 0.78:
            status = "delivered"
        elif r < 0.88:
            status = "cancelled"
        elif r < 0.92:
            status = "picked_up"     # these 3 are "stuck" orders the old app never closed
        elif r < 0.96:
            status = "accepted"
        else:
            status = "created"

        promised_at = created + timedelta(minutes=PROMISE_MINUTES)
        delivered_at = None
        courier_id = None
        if status == "delivered":
            courier_id = rnd.choice(COURIERS)
            delivered_at = _ms(created + timedelta(minutes=rnd.triangular(14, 52, 26)))
            updated_at = delivered_at
        elif status == "cancelled":
            if rnd.random() < 0.5:
                courier_id = rnd.choice(COURIERS)
            updated_at = _ms(created + timedelta(minutes=rnd.uniform(2, 10)))
        elif status == "picked_up":
            courier_id = rnd.choice(COURIERS)
            updated_at = _ms(created + timedelta(minutes=rnd.uniform(8, 20)))
        elif status == "accepted":
            courier_id = rnd.choice(COURIERS)
            updated_at = _ms(created + timedelta(minutes=rnd.uniform(1, 4)))
        else:
            updated_at = created

        # ---- customer: schema drift v1 -> v2 ----------------------------------
        email = f"{first}.{last}{cust_no}@example.com".lower()
        customer = {"customer_id": f"CU-{cust_no:04d}", "name": f"{first} {last}"}
        if created < SCHEMA_V2_FROM:
            schema_v = 1
            customer["email"] = email
        else:
            schema_v = 2
            customer["contact"] = {"email": email,
                                   "phone": f"+972-5{rnd.randint(0, 9)}-{rnd.randint(1000000, 9999999)}"}

        doc = {
            # deterministic ObjectId: 4-byte timestamp = order time, rest = counter
            "_id": ObjectId(f"{int(created.timestamp()):08x}{i:016x}"),
            "order_id": f"QB-{100000 + i}",
            "schema_v": schema_v,
            "customer": customer,
            "restaurant_id": rest_id,
            "restaurant_name": rest_name,
            "delivery_address": {"street": f"{rnd.choice(STREETS)} {rnd.randint(1, 180)}",
                                 "city": city_spelling},
            "items": items,
        }

        # ---- total: Decimal128 / double / string / missing --------------------
        r = rnd.random()
        if r < 0.60:
            doc["total"] = Decimal128(Decimal(f"{amount:.2f}"))
        elif r < 0.85:
            doc["total"] = float(amount)
        elif r < 0.95:
            doc["total"] = f"{amount:.2f}"           # a string. Classic.
        # else: missing

        # ---- status: mixed case / missing -------------------------------------
        r = rnd.random()
        if r < 0.85:
            doc["status"] = status
        elif r < 0.92:
            doc["status"] = status.upper()
        elif r < 0.97:
            doc["status"] = status.capitalize()
        # else: missing

        # ---- created_at: Date / ISO string / epoch millis ---------------------
        r = rnd.random()
        if r < 0.85:
            doc["created_at"] = created
        elif r < 0.95:
            doc["created_at"] = created.strftime("%Y-%m-%dT%H:%M:%SZ")
        else:
            doc["created_at"] = int(created.timestamp() * 1000)

        if courier_id:
            doc["courier_id"] = courier_id
        doc["promised_at"] = promised_at
        if delivered_at:
            doc["delivered_at"] = delivered_at
        doc["payment"] = {"method": rnd.choice(PAYMENT_METHODS)}
        doc["updated_at"] = updated_at              # ALWAYS a Date - the Airflow watermark uses it
        docs.append(doc)

    # Two duplicated business keys: the old app's retry logic inserted the same order twice
    # (same order_id, same content, a different _id).
    for src, n in ((40, N_UNIQUE), (41, N_UNIQUE + 1)):
        dup = copy.deepcopy(docs[src])
        dup["_id"] = ObjectId(f"{int(dup['_id'].generation_time.timestamp()):08x}{n:016x}")
        docs.append(dup)
    return docs


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--uri", default=os.getenv("MONGO_URI", "mongodb://localhost:27017/?directConnection=true"))
    p.add_argument("--json-only", action="store_true", help="only write orders.json, do not touch MongoDB")
    p.add_argument("--out", default=os.path.join(os.path.dirname(os.path.abspath(__file__)), "orders.json"))
    args = p.parse_args()

    docs = build_docs()
    with open(args.out, "w", encoding="utf-8") as f:
        f.write(dumps(docs, json_options=JSONOptions(json_mode=JSONMode.CANONICAL), indent=1))
    print(f"wrote {args.out} ({len(docs)} docs)")
    if args.json_only:
        return

    from pymongo import MongoClient
    client = MongoClient(args.uri, serverSelectionTimeoutMS=10000)
    db = client["quickbite"]
    db["orders"].drop()               # re-running the seed always gives a clean start
    db["orders"].insert_many(docs)
    print(f"inserted {db['orders'].count_documents({})} docs into quickbite.orders")

    # A small, CLEAN dimension table: Finance's commission per restaurant (used by the DAG)
    db["restaurants"].drop()
    db["restaurants"].insert_many(restaurant_docs())
    print(f"inserted {db['restaurants'].count_documents({})} docs into quickbite.restaurants")


def restaurant_docs():
    return [{"restaurant_id": r[0], "name": r[1], "city": r[2], "cuisine": r[3], "commission_pct": r[4]}
            for r in RESTAURANTS]


if __name__ == "__main__":
    main()

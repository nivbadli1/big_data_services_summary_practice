"""
SOLUTION - Part 2 (+ the Part 5 fixes): QuickBite live consumer
================================================================
Kafka (order.events + courier.locations)  ->  Elasticsearch (deliveries-live + courier-locations)

Compare with your apps/consumer/consumer.py. Every TODO from the skeleton is solved here,
and the comments explain WHY each decision was made - that is the part worth reading.

The solution folder is mounted in the apps container at /solution, so you can run it with:
    docker compose exec apps python /solution/apps/consumer/consumer.py

The big ideas:
  1. AT-LEAST-ONCE delivery: we commit the Kafka offset only AFTER Elasticsearch confirmed
     the write. If we crash in between, Kafka gives us the same events again -> duplicates
     are normal, not a bug. So the pipeline must be safe against duplicates:
  2. DEDUPE by event_id (each event has a unique id) - skips events we already handled.
  3. IDEMPOTENT WRITES: deliveries-live uses order_id as the Elasticsearch _id and an
     "upsert" (update if exists, insert if not). Writing the same event twice gives the
     same final document - no double counting, even if dedupe misses something
     (e.g. after a restart the in-memory dedupe set is empty).
"""
import json
import os
import time
from collections import OrderedDict
from datetime import datetime, timezone

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "course-kafka:9092")
ES_URL = os.getenv("ES_URL", "http://elasticsearch:9200")
GROUP_ID = os.getenv("CONSUMER_GROUP", "quickbite-live")

ORDER_TOPIC = "order.events"
COURIER_TOPIC = "courier.locations"
DELIVERIES_INDEX = "deliveries-live"
COURIERS_INDEX = "courier-locations"

FINAL_STATUSES = {"delivered", "cancelled"}


# =====================================================================================
# Small pure helpers (no Kafka / Elasticsearch here -> easy to unit-test, see tests/)
# =====================================================================================
def parse_ts(value):
    """'2026-11-04T18:22:05.123Z' -> aware datetime (UTC). None stays None."""
    if not value:
        return None
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


class SeenEvents:
    """
    Remembers the last `max_size` event_ids we processed (Part 2.4: dedupe).

    Why bounded? Kafka topics never end; an unbounded set would eat all the memory.
    Duplicates from retries / replays arrive "close" to the original, so remembering
    the most recent 100k ids is enough in practice.
    Why an OrderedDict and not a set? It remembers insertion order, so we can throw
    away the OLDEST id when we are full (popitem(last=False)).
    """

    def __init__(self, max_size=100_000):
        self.max_size = max_size
        self._ids = OrderedDict()
        self.duplicates = 0

    def is_duplicate(self, event_id):
        """True if we saw this id before. Otherwise remember it and return False."""
        if event_id is None:            # no id -> we cannot dedupe; process it (upsert is still safe)
            return False
        if event_id in self._ids:
            self.duplicates += 1
            return True
        self._ids[event_id] = True
        if len(self._ids) > self.max_size:
            self._ids.popitem(last=False)
        return False


def normalize_status(event):
    """
    Part 5 (drift) fix: the 'v3 release' sends status in UPPER CASE ("DELIVERED").
    Elasticsearch `keyword` fields are case-sensitive, so status:delivered would miss them.
    Normalizing at ingestion keeps every downstream query simple.
    """
    status = event.get("status")
    return status.strip().lower() if isinstance(status, str) else None


def event_total(event):
    """
    Part 5 (drift) fix: v3 moved the money from `total` to `payment.amount`.
    Coalesce: take whichever exists. Never invent a 0 - missing stays None, so a missing
    value is visible as missing instead of silently lowering the revenue.
    """
    total = event.get("total")
    if total is None:
        total = (event.get("payment") or {}).get("amount")
    try:
        return round(float(total), 2) if total is not None else None
    except (TypeError, ValueError):
        return None


def compute_is_late(event, now):
    """
    Part 2.5: is this order late?
      - delivered:  late if delivered_at > promised_at          (a final answer)
      - cancelled:  never "late" (it did not get delivered at all - a different KPI)
      - still open: late if NOW is already past promised_at     (an answer that can change)
    Missing promised_at -> we cannot know -> False.
    """
    promised = parse_ts(event.get("promised_at"))
    if promised is None:
        return False
    status = normalize_status(event)
    if status == "cancelled":
        return False
    if status == "delivered":
        delivered = parse_ts(event.get("delivered_at")) or parse_ts(event.get("ts"))
        return delivered is not None and delivered > promised
    return now > promised


def order_event_to_doc(event, now):
    """One order event -> the fields we store in deliveries-live for that order."""
    status = normalize_status(event)
    doc = {
        "order_id": event["order_id"],
        "status": status,
        "last_event_type": event.get("event_type"),
        "last_event_ts": event.get("ts"),
        "restaurant_id": event.get("restaurant_id"),
        "restaurant_name": event.get("restaurant_name"),
        "city": event.get("city"),
        "courier_id": event.get("courier_id"),
        "total": event_total(event),
        "promised_at": event.get("promised_at"),
        "is_late": compute_is_late(event, now),
        "schema_v": event.get("schema_v"),
        "indexed_at": now.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z",
    }
    if event.get("delivered_at"):
        doc["delivered_at"] = event["delivered_at"]
    if event.get("cancel_reason"):
        doc["cancel_reason"] = event["cancel_reason"]
    promised = parse_ts(event.get("promised_at"))
    if status == "delivered" and event.get("delivered_at") and promised:
        # minutes late (negative = early). Handy for a Kibana "average lateness" metric.
        doc["minutes_vs_promise"] = round((parse_ts(event["delivered_at"]) - promised).total_seconds() / 60, 2)
    # Do not overwrite good values with None (e.g. an event without courier_id must not
    # erase the courier we already know). Upsert merges, so leaving a field out keeps it.
    return {k: v for k, v in doc.items() if v is not None}


def delivery_action(event, now):
    """
    Part 2.6: the Elasticsearch bulk action for one order event.
      _id = order_id      -> one document per order (not one per event)
      update + doc_as_upsert -> insert the first time, merge fields after that
    Re-sending the same event produces the same document: the write is IDEMPOTENT.
    """
    return {
        "_op_type": "update",
        "_index": DELIVERIES_INDEX,
        "_id": event["order_id"],
        "doc": order_event_to_doc(event, now),
        "doc_as_upsert": True,
    }


def courier_ping_to_doc(ping):
    """
    Part 2.7: a GPS ping -> a courier-locations document.
    Elasticsearch geo_point accepts {"lat": .., "lon": ..} - the mapping (Part 3) must say
    `location` is a geo_point, otherwise Kibana Maps cannot use it.
    """
    return {
        "courier_id": ping["courier_id"],
        "location": {"lat": ping["lat"], "lon": ping["lon"]},
        "state": ping.get("state"),
        "speed_kmh": ping.get("speed_kmh"),
        "ts": ping.get("ts"),
    }


def courier_action(ping):
    # _id = courier_id -> the index always holds the LATEST position of each courier (30 docs).
    # (If you wanted the full GPS history you would let Elasticsearch generate the _id.)
    return {"_op_type": "index", "_index": COURIERS_INDEX, "_id": ping["courier_id"],
            "_source": courier_ping_to_doc(ping)}


def build_actions(records, seen, now):
    """
    Turn a batch of Kafka records into Elasticsearch bulk actions.
    `records` is a list of objects with .topic and .value (already JSON-decoded dicts).
    Returns (actions, stats).
    """
    actions, stats = [], {"orders": 0, "couriers": 0, "duplicates": 0, "bad": 0}
    for rec in records:
        value = rec.value
        if not isinstance(value, dict):
            stats["bad"] += 1          # not JSON -> skip it (a real system would send it to a DLQ topic)
            continue
        if rec.topic == ORDER_TOPIC:
            if not value.get("order_id"):
                stats["bad"] += 1
                continue
            if seen.is_duplicate(value.get("event_id")):
                stats["duplicates"] += 1
                continue
            actions.append(delivery_action(value, now))
            stats["orders"] += 1
        elif rec.topic == COURIER_TOPIC:
            # GPS pings are not deduped: a repeated ping just overwrites the same doc (idempotent).
            if value.get("courier_id") is None or value.get("lat") is None:
                stats["bad"] += 1
                continue
            actions.append(courier_action(value))
            stats["couriers"] += 1
    return actions, stats


def refresh_stale_late_flags(es):
    """
    Part 5 (late) fix. is_late is computed when an EVENT arrives. An order that is stuck in
    "picked_up" sends no new event, so its stored is_late stays false even after promised_at
    has passed - the dashboard under-reports exactly during the incident we care about.

    Fix: every 30 s ask Elasticsearch to flip is_late for open orders whose promise is in the past.
    (The other fix is to not trust the stored flag for open orders at all and filter in Kibana with
    `promised_at < now` - computed at QUERY time. Both are fine; know why the flag goes stale.)
    """
    return es.update_by_query(
        index=DELIVERIES_INDEX,
        query={"bool": {
            "filter": [{"terms": {"status": ["created", "accepted", "picked_up"]}},
                       {"range": {"promised_at": {"lt": "now"}}}],
            "must_not": [{"term": {"is_late": True}}],
        }},
        script={"source": "ctx._source.is_late = true", "lang": "painless"},
        conflicts="proceed",          # an event updated the doc at the same moment -> skip it, next round
        refresh=True,
    )


def safe_json(raw):
    """value_deserializer that never crashes the consumer on a bad message."""
    try:
        return json.loads(raw.decode("utf-8"))
    except (ValueError, UnicodeDecodeError, AttributeError):
        return None


# =====================================================================================
# The main loop
# =====================================================================================
def main():
    from elasticsearch import Elasticsearch, NotFoundError, helpers
    from kafka import KafkaConsumer

    es = Elasticsearch(ES_URL, request_timeout=30)
    consumer = KafkaConsumer(
        ORDER_TOPIC, COURIER_TOPIC,              # one consumer can read several topics
        bootstrap_servers=KAFKA_BOOTSTRAP,
        group_id=GROUP_ID,                       # Kafka stores OUR position (offset) under this name
        auto_offset_reset="earliest",            # first start of the group -> read from the beginning
        enable_auto_commit=False,                # WE decide when an offset is "done" (after the ES write)
        value_deserializer=safe_json,
        key_deserializer=lambda k: k.decode("utf-8") if k else None,
        max_poll_records=500,
    )
    seen = SeenEvents()
    totals = {"orders": 0, "couriers": 0, "duplicates": 0, "bad": 0}
    print(f"[consumer] group={GROUP_ID} topics={ORDER_TOPIC},{COURIER_TOPIC} -> {ES_URL}. Ctrl+C to stop.")
    last_print = 0.0
    last_refresh = 0.0
    try:
        while True:
            if time.time() - last_refresh > 30:
                last_refresh = time.time()
                try:
                    res = refresh_stale_late_flags(es)
                    if res.get("updated"):
                        print(f"[consumer] marked {res['updated']} open orders as late (promise passed)")
                except NotFoundError:
                    pass  # normal on the very first start: deliveries-live is created by the first write
                except Exception as e:  # noqa: BLE001 - never let the refresh kill the consumer
                    print(f"[consumer] late-flag refresh skipped: {e}")

            # poll() returns {TopicPartition: [records]} - flatten it into one list
            batch = consumer.poll(timeout_ms=1000)
            records = [r for part in batch.values() for r in part]
            if not records:
                continue
            now = datetime.now(timezone.utc)
            actions, stats = build_actions(records, seen, now)
            if actions:
                # raise_on_error=True -> if ANY action fails we raise BEFORE committing,
                # so Kafka will give us this batch again (at-least-once).
                for attempt in range(1, 6):
                    try:
                        helpers.bulk(es, actions, raise_on_error=True)
                        break
                    except Exception as e:  # noqa: BLE001
                        print(f"[consumer] bulk failed (attempt {attempt}/5): {e}")
                        if attempt == 5:
                            raise
                        time.sleep(2 * attempt)
            consumer.commit()                    # only now: "everything up to here is safely in ES"
            for k in totals:
                totals[k] += stats[k]
            if time.time() - last_print > 5:
                print(f"[consumer] order events={totals['orders']} courier pings={totals['couriers']}"
                      f" duplicates skipped={totals['duplicates']} bad={totals['bad']}")
                last_print = time.time()
    except KeyboardInterrupt:
        print("[consumer] stopping")
    finally:
        consumer.close()


if __name__ == "__main__":
    main()

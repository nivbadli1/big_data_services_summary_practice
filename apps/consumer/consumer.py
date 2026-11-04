"""
Part 2 - QuickBite live consumer   (YOU write the parts marked TODO)
====================================================================
Kafka (order.events + courier.locations)  ->  Elasticsearch (deliveries-live + courier-locations)

RUN ORDER: after Part 0 (simulators running) and Part 2.0 (index templates created).
    docker compose exec apps python /apps/consumer/consumer.py
Stop it with Ctrl+C. Run it again after every change.

The file already RUNS as it is: it connects, reads messages and prints how many it saw,
but it writes NOTHING to Elasticsearch until you finish the TODOs.

What the finished consumer must do (handout Part 2.4 - 2.8):
  - read BOTH topics with ONE consumer in the consumer group "quickbite-live"
  - skip events it already processed (dedupe by event_id) - Kafka is "at-least-once"
  - write one document PER ORDER to deliveries-live (doc _id = order_id -> upsert)
    with an is_late flag
  - write the latest position of each courier to courier-locations (a geo_point)
  - commit the Kafka offsets only AFTER Elasticsearch accepted the writes

Self-check your functions (no Kafka needed):
    docker compose exec apps pytest -q /tests/test_consumer.py --target=student

Stuck? See solution/apps/consumer/consumer.py
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


# =====================================================================================
# Helpers - pure Python, no Kafka / Elasticsearch (so they are easy to test)
# =====================================================================================
def parse_ts(value):
    """'2026-11-04T18:22:05.123Z' -> aware datetime (UTC). None stays None.  (PROVIDED)"""
    if not value:
        return None
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


class SeenEvents:
    """Remembers event_ids we already processed (at most max_size of them)."""

    def __init__(self, max_size=100_000):
        self.max_size = max_size
        self._ids = OrderedDict()
        self.duplicates = 0

    def is_duplicate(self, event_id):
        """
        Return True if event_id was seen before (and count it in self.duplicates).
        Otherwise remember it and return False.

        TODO (Part 2.4): implement the dedupe.
          - an event without an id (None) can not be deduped -> return False
          - remember the id in self._ids
          - if more than self.max_size ids are stored, forget the OLDEST one
            hint: OrderedDict.popitem(last=False)
        """
        return False


def normalize_status(event):
    """
    The event's status as a clean lower-case string, or None.
    TODO (Part 5 - drift): for now this just returns the value as it is.
    """
    return event.get("status")


def event_total(event):
    """
    The order amount from the event as a float (or None if it is missing).
    TODO (Part 5 - drift): for now this only knows the `total` field.
    """
    total = event.get("total")
    return float(total) if total is not None else None


def compute_is_late(event, now):
    """
    Is this order late?

    TODO (Part 2.5): implement the rule:
      - no promised_at                    -> False
      - status "cancelled"                -> False (not delivered at all - a different KPI)
      - status "delivered"                -> delivered_at > promised_at
      - any other status (still open)     -> now > promised_at
    Use parse_ts() to turn the strings into datetimes and normalize_status(event) for the status.
    """
    return False


def order_event_to_doc(event, now):
    """One order event -> the fields we store for that order in deliveries-live.  (PROVIDED)"""
    doc = {
        "order_id": event["order_id"],
        "status": normalize_status(event),
        "last_event_type": event.get("event_type"),
        "last_event_ts": event.get("ts"),
        "restaurant_id": event.get("restaurant_id"),
        "restaurant_name": event.get("restaurant_name"),
        "city": event.get("city"),
        "courier_id": event.get("courier_id"),
        "total": event_total(event),
        "promised_at": event.get("promised_at"),
        "delivered_at": event.get("delivered_at"),
        "cancel_reason": event.get("cancel_reason"),
        "is_late": compute_is_late(event, now),
        "schema_v": event.get("schema_v"),
        "indexed_at": now.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z",
    }
    # Leave out empty fields: an upsert MERGES, so a missing field keeps the value we
    # already stored (e.g. an event without courier_id does not erase the courier).
    return {k: v for k, v in doc.items() if v is not None}


def delivery_action(event, now):
    """
    The Elasticsearch bulk action for one order event.

    TODO (Part 2.6): return a dict for elasticsearch.helpers.bulk that UPSERTS
    one document per order into DELIVERIES_INDEX:
        "_op_type": "update", "_index": ..., "_id": <the order_id>,
        "doc": order_event_to_doc(event, now), "doc_as_upsert": True
    Why _id = order_id?  -> handout Part 2.6
    """
    return None


def courier_action(ping):
    """
    The Elasticsearch bulk action for one GPS ping.

    TODO (Part 2.7): return an "index" action into COURIERS_INDEX with
        _id = courier_id  (the index keeps only the LATEST position per courier)
        _source = {"courier_id", "location": {"lat": .., "lon": ..}, "state", "speed_kmh", "ts"}
    """
    return None


def build_actions(records, seen, now):
    """
    Turn a batch of Kafka records (each has .topic and .value = a dict) into bulk actions.
    Returns (actions, stats).  (PROVIDED - it calls your functions above)
    """
    actions, stats = [], {"orders": 0, "couriers": 0, "duplicates": 0, "bad": 0}
    for rec in records:
        value = rec.value
        if not isinstance(value, dict):
            stats["bad"] += 1
            continue
        if rec.topic == ORDER_TOPIC:
            if not value.get("order_id"):
                stats["bad"] += 1
                continue
            if seen.is_duplicate(value.get("event_id")):
                stats["duplicates"] += 1
                continue
            action = delivery_action(value, now)
            stats["orders"] += 1
        elif rec.topic == COURIER_TOPIC:
            if value.get("courier_id") is None or value.get("lat") is None:
                stats["bad"] += 1
                continue
            action = courier_action(value)
            stats["couriers"] += 1
        else:
            continue
        if action is not None:          # None = that TODO is not done yet
            actions.append(action)
    return actions, stats


def safe_json(raw):
    """value_deserializer that never crashes on a bad message.  (PROVIDED)"""
    try:
        return json.loads(raw.decode("utf-8"))
    except (ValueError, UnicodeDecodeError, AttributeError):
        return None


# =====================================================================================
# The main loop
# =====================================================================================
def main():
    from elasticsearch import Elasticsearch, helpers
    from kafka import KafkaConsumer

    es = Elasticsearch(ES_URL, request_timeout=30)

    # TODO (Part 2.3): configure the consumer
    #   - subscribe to BOTH topics (ORDER_TOPIC and COURIER_TOPIC)
    #   - group_id=GROUP_ID               (why? handout Part 2.3)
    #   - auto_offset_reset="earliest"    (first start of the group: read from the beginning)
    #   - enable_auto_commit=False        (we commit ourselves, AFTER the Elasticsearch write)
    # For now it reads only order.events, with no group, from the end of the topic:
    consumer = KafkaConsumer(
        ORDER_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP,
        value_deserializer=safe_json,
        key_deserializer=lambda k: k.decode("utf-8") if k else None,
        max_poll_records=500,
    )
    seen = SeenEvents()
    totals = {"orders": 0, "couriers": 0, "duplicates": 0, "bad": 0, "written": 0}
    print(f"[consumer] reading from {KAFKA_BOOTSTRAP} -> writing to {ES_URL}. Ctrl+C to stop.")
    last_print = 0.0
    try:
        while True:
            batch = consumer.poll(timeout_ms=1000)          # {TopicPartition: [records]}
            records = [r for part in batch.values() for r in part]
            if not records:
                continue
            now = datetime.now(timezone.utc)
            actions, stats = build_actions(records, seen, now)

            if actions:
                # TODO (Part 2.8): write the actions to Elasticsearch with
                #   helpers.bulk(es, actions, raise_on_error=True)
                # and THEN commit the offsets with consumer.commit().
                # Why this order? handout Part 2.8 ("at-least-once").
                pass

            for k in stats:
                totals[k] += stats[k]
            if time.time() - last_print > 5:
                print(f"[consumer] order events={totals['orders']} courier pings={totals['couriers']}"
                      f" duplicates skipped={totals['duplicates']} bad={totals['bad']}"
                      f" | actions built this batch={len(actions)}")
                if not actions:
                    print("[consumer]   (nothing is written to Elasticsearch yet - finish the TODOs)")
                last_print = time.time()
    except KeyboardInterrupt:
        print("[consumer] stopping")
    finally:
        consumer.close()


if __name__ == "__main__":
    main()

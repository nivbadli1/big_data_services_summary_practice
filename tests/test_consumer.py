"""
Tests for the live consumer (Part 2) - apps/consumer/consumer.py
No Kafka or Elasticsearch needed: we test the pure functions with small fake records.

    docker compose exec apps pytest -q /tests/test_consumer.py --target=student   # your version
    docker compose exec apps pytest -q /tests/test_consumer.py                    # the solution
"""
from collections import namedtuple
from datetime import datetime, timedelta, timezone

import pytest

Record = namedtuple("Record", "topic value")
NOW = datetime(2026, 11, 6, 18, 0, 0, tzinfo=timezone.utc)


def iso(dt):
    return dt.strftime("%Y-%m-%dT%H:%M:%S.") + f"{dt.microsecond // 1000:03d}Z"


def order_event(order_id="QB-200001", event_id="e-1", status="picked_up", promised=NOW + timedelta(minutes=1),
                delivered=None, **extra):
    e = {"event_id": event_id, "order_id": order_id, "event_type": f"order_{status}", "status": status,
         "ts": iso(NOW - timedelta(seconds=30)), "restaurant_id": "R-01", "restaurant_name": "Hummus Abu Dani",
         "city": "Tel Aviv", "courier_id": "C-07", "promised_at": iso(promised), "total": 84.0, "schema_v": 2}
    if delivered:
        e["delivered_at"] = iso(delivered)
    e.update(extra)
    return e


def ping(cid="C-07", lat=32.0809, lon=34.7806):
    return {"courier_id": cid, "lat": lat, "lon": lon, "ts": iso(NOW), "state": "delivering", "speed_kmh": 18.2}


# --------------------------------------------------------------------- dedupe (Part 2.4)
def test_seen_events_dedupes(consumer):
    seen = consumer.SeenEvents()
    assert seen.is_duplicate("a") is False
    assert seen.is_duplicate("b") is False
    assert seen.is_duplicate("a") is True
    assert seen.duplicates == 1
    assert seen.is_duplicate(None) is False and seen.is_duplicate(None) is False   # no id -> cannot dedupe


def test_seen_events_is_bounded(consumer):
    seen = consumer.SeenEvents(max_size=3)
    for i in range(5):
        seen.is_duplicate(f"id-{i}")
    assert len(seen._ids) == 3
    assert seen.is_duplicate("id-0") is False          # the oldest ids were forgotten
    assert seen.is_duplicate("id-4") is True


# --------------------------------------------------------------------- is_late (Part 2.5)
@pytest.mark.parametrize("status, promised, delivered, expected", [
    ("delivered", NOW, NOW - timedelta(minutes=1), False),      # early
    ("delivered", NOW, NOW + timedelta(seconds=1), True),       # 1 second late is late
    ("delivered", NOW, NOW, False),                             # exactly on time
    ("picked_up", NOW + timedelta(minutes=1), None, False),     # open, still has time
    ("picked_up", NOW - timedelta(minutes=1), None, True),      # open, already late
    ("accepted", NOW - timedelta(minutes=1), None, True),
    ("cancelled", NOW - timedelta(hours=1), None, False),       # never "late"
])
def test_compute_is_late(consumer, status, promised, delivered, expected):
    assert consumer.compute_is_late(order_event(status=status, promised=promised, delivered=delivered), NOW) is expected


def test_compute_is_late_without_promise(consumer):
    e = order_event()
    e.pop("promised_at")
    assert consumer.compute_is_late(e, NOW) is False


# --------------------------------------------------------------------- ES actions (2.6, 2.7)
def test_delivery_action_is_an_upsert_by_order_id(consumer):
    a = consumer.delivery_action(order_event(), NOW)
    assert a["_op_type"] == "update"
    assert a["_index"] == "deliveries-live"
    assert a["_id"] == "QB-200001"
    assert a["doc_as_upsert"] is True
    assert a["doc"]["order_id"] == "QB-200001" and a["doc"]["is_late"] is False
    assert "delivered_at" not in a["doc"]            # None values are left out (upsert keeps old values)


def test_same_event_twice_gives_the_same_action(consumer):
    e = order_event()
    assert consumer.delivery_action(e, NOW) == consumer.delivery_action(dict(e), NOW)   # idempotent


def test_courier_action_is_geo_point_latest_position(consumer):
    a = consumer.courier_action(ping())
    assert a["_op_type"] == "index" and a["_index"] == "courier-locations" and a["_id"] == "C-07"
    assert a["_source"]["location"] == {"lat": 32.0809, "lon": 34.7806}
    assert a["_source"]["courier_id"] == "C-07"


# --------------------------------------------------------------------- the batch
def test_build_actions_mixed_batch(consumer):
    seen = consumer.SeenEvents()
    e1 = order_event(event_id="e-1", status="accepted")
    e2 = order_event(event_id="e-2", status="picked_up")
    records = [Record("order.events", e1), Record("courier.locations", ping("C-01")),
               Record("order.events", e2), Record("order.events", dict(e1)),       # replayed duplicate
               Record("order.events", None), Record("courier.locations", {"courier_id": "C-02"})]
    actions, stats = consumer.build_actions(records, seen, NOW)
    assert stats == {"orders": 2, "couriers": 1, "duplicates": 1, "bad": 2}
    assert [a["_id"] for a in actions] == ["QB-200001", "C-01", "QB-200001"]


def test_safe_json(consumer):
    assert consumer.safe_json(b'{"a": 1}') == {"a": 1}
    assert consumer.safe_json(b"not json") is None
    assert consumer.safe_json(None) is None


# --------------------------------------------------------------------- Part 5 fixes (drift)
@pytest.mark.part5
def test_drift_v3_status_and_payment_amount(consumer):
    e = order_event(status="DELIVERED", promised=NOW, delivered=NOW + timedelta(minutes=2), schema_v=3)
    e.pop("total")
    e["payment"] = {"amount": 112.0}
    doc = consumer.delivery_action(e, NOW)["doc"]
    assert doc["status"] == "delivered"
    assert doc["total"] == 112.0
    assert doc["is_late"] is True

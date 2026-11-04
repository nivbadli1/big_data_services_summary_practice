"""
order-app simulator - pretends to be the QuickBite ordering backend.
=====================================================================
PROVIDED - you do not need to change this file (but reading it is a good idea).

Every second it creates new orders and moves existing ones through their lifecycle:

    created -> accepted -> picked_up -> delivered        (or -> cancelled)

For EVERY change it does three things, like a real backend:
  1. MongoDB   quickbite.orders   insert / update the order document (always sets updated_at)
  2. Kafka     order.events       produce one JSON event, key = order_id
  3. Elastic   app-logs           write application log lines directly (no Logstash)

Time is compressed so you can watch things happen: the "promise" to the customer
is PROMISE_SECONDS (default 120 s) instead of 35 minutes. About 1 in 7 deliveries is late.

Run (inside the apps container - see handout Part 0):
    bash /apps/start_simulators.sh          # starts this file + courier_sim.py in the background
    python /apps/simulators/order_app.py    # or run it in the foreground to watch it

Environment variables (all optional):
    KAFKA_BOOTSTRAP   default course-kafka:9092
    MONGO_URI         default mongodb://mongo:27017
    ES_URL            default http://elasticsearch:9200
    ORDERS_PER_MIN    default 20     new orders per minute
    PROMISE_SECONDS   default 120    delivery promise shown to the customer
    SIM_SEED          default 7      random seed (same seed -> same sequence of orders)
"""
import json
import os
import random
import sys
import time
import uuid
from datetime import datetime, timedelta, timezone

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))
from common.catalog import (CITY_SPELLINGS, COURIERS, FIRST_NAMES, LAST_NAMES,  # noqa: E402
                            MENU, RESTAURANTS, STREETS)
from common import chaos_flags  # noqa: E402

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "course-kafka:9092")
MONGO_URI = os.getenv("MONGO_URI", "mongodb://mongo:27017")
ES_URL = os.getenv("ES_URL", "http://elasticsearch:9200")
ORDERS_PER_MIN = float(os.getenv("ORDERS_PER_MIN", "20"))
PROMISE_SECONDS = int(os.getenv("PROMISE_SECONDS", "120"))
SIM_SEED = int(os.getenv("SIM_SEED", "7"))

ORDER_TOPIC = "order.events"
COURIER_TOPIC = "courier.locations"
LOG_INDEX = "app-logs"
FIRST_LIVE_ORDER = 200000          # live order ids start at QB-200000 (history is QB-1xxxxx)

EVENT_TYPE = {"created": "order_created", "accepted": "order_accepted",
              "picked_up": "order_picked_up", "delivered": "order_delivered",
              "cancelled": "order_cancelled"}


def utcnow():
    return datetime.now(timezone.utc)


def iso(dt):
    """2026-11-04T18:22:05.123Z - the timestamp format used in every event and log line."""
    return dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.") + f"{dt.microsecond // 1000:03d}Z"


# ---------------------------------------------------------------------------------
# Connections (with retries: the containers may still be starting)
# ---------------------------------------------------------------------------------
def connect_with_retry(name, fn, attempts=30, wait=5):
    for i in range(1, attempts + 1):
        try:
            return fn()
        except Exception as e:  # noqa: BLE001 - we really want to retry on anything here
            print(f"[order-app] waiting for {name} ({i}/{attempts}): {type(e).__name__}: {e}")
            time.sleep(wait)
    raise SystemExit(f"[order-app] could not connect to {name} - is `docker compose ps` all healthy?")


def make_clients():
    from elasticsearch import Elasticsearch
    from kafka import KafkaProducer
    from kafka.admin import KafkaAdminClient, NewTopic
    from kafka.errors import TopicAlreadyExistsError
    from pymongo import MongoClient

    def kafka():
        admin = KafkaAdminClient(bootstrap_servers=KAFKA_BOOTSTRAP, client_id="order-app-admin")
        # Create the two topics explicitly (instead of relying on auto-create) so that
        # order.events has 3 partitions. Same key (order_id) -> same partition -> ordered events.
        for topic, parts in ((ORDER_TOPIC, 3), (COURIER_TOPIC, 1)):
            try:
                admin.create_topics([NewTopic(topic, num_partitions=parts, replication_factor=1)])
                print(f"[order-app] created topic {topic} ({parts} partitions)")
            except TopicAlreadyExistsError:
                pass
        admin.close()
        return KafkaProducer(
            bootstrap_servers=KAFKA_BOOTSTRAP,
            client_id="order-app",
            acks="all",                      # wait until the broker really stored the event
            retries=5,
            key_serializer=lambda k: k.encode("utf-8"),
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        )

    def mongo():
        c = MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)
        c.admin.command("ping")
        return c["quickbite"]["orders"]

    def es():
        c = Elasticsearch(ES_URL, request_timeout=10)
        c.info()
        return c

    es_client = connect_with_retry("Elasticsearch", es)
    # Index templates only apply to indices created AFTER them. If app-logs is created
    # without the template, Elasticsearch guesses the types ("dynamic mapping") and e.g.
    # `level` becomes text instead of keyword. So we refuse to start without it.
    if os.getenv("SKIP_TEMPLATE_CHECK") != "1" and not es_client.indices.exists_index_template(name=LOG_INDEX):
        raise SystemExit("[order-app] index template 'app-logs' not found in Elasticsearch.\n"
                         "            Do handout Part 0.5 first (section A of part3_elk/mappings.http in Kibana Dev Tools).")

    return connect_with_retry("Kafka", kafka), connect_with_retry("MongoDB", mongo), es_client


# ---------------------------------------------------------------------------------
# The simulation
# ---------------------------------------------------------------------------------
class OrderApp:
    def __init__(self, producer, orders, es, rnd):
        self.producer, self.orders, self.es, self.rnd = producer, orders, es, rnd
        self.active = {}                 # order_id -> in-memory state of open orders
        self.log_buffer = []
        self.sent = 0
        self.next_no = self._next_order_no()
        self.last_error_burst = 0.0

    def _next_order_no(self):
        """Continue numbering after the last live order (so a restart does not reuse ids)."""
        last = self.orders.find_one({"order_id": {"$gte": f"QB-{FIRST_LIVE_ORDER}"}},
                                    sort=[("order_id", -1)], projection={"order_id": 1})
        return int(last["order_id"][3:]) + 1 if last else FIRST_LIVE_ORDER

    # ----- logging to Elasticsearch ------------------------------------------------
    def log(self, level, service, message, order_id=None, latency_ms=None):
        doc = {"@timestamp": iso(utcnow()), "level": level, "service": service,
               "message": message, "host": "order-app-1"}
        if order_id:
            doc["order_id"] = order_id
        if latency_ms is not None:
            doc["latency_ms"] = int(latency_ms)
        self.log_buffer.append(doc)

    def flush_logs(self):
        if not self.log_buffer:
            return
        from elasticsearch import helpers
        actions = [{"_index": LOG_INDEX, "_source": d} for d in self.log_buffer]
        try:
            helpers.bulk(self.es, actions)
            self.log_buffer = []
        except Exception as e:  # noqa: BLE001 - logs are best-effort, keep the app running
            print(f"[order-app] could not write logs to Elasticsearch: {e}")
            self.log_buffer = self.log_buffer[-2000:]

    def normal_logs(self, state, order_id):
        """A few log lines per lifecycle step, with a realistic small error rate."""
        r = self.rnd.random()
        if state == "created":
            svc, msg, lat = "payment-service", "payment authorized", self.rnd.uniform(120, 650)
        elif state == "accepted":
            svc, msg, lat = "order-service", "restaurant accepted order", self.rnd.uniform(20, 180)
        elif state == "picked_up":
            svc, msg, lat = "dispatch-service", "courier picked up order", self.rnd.uniform(30, 250)
        else:
            svc, msg, lat = "order-service", f"order {state}", self.rnd.uniform(15, 120)
        if r < 0.01:
            self.log("ERROR", svc, f"{msg} failed: upstream timeout", order_id, lat * 8)
        elif r < 0.05:
            self.log("WARN", svc, f"{msg} slowly", order_id, lat * 3)
        else:
            self.log("INFO", svc, msg, order_id, lat)

    # ----- one order -------------------------------------------------------------
    def new_order(self, now, flags):
        rest_id, rest_name, city, cuisine, _ = self.rnd.choice(RESTAURANTS)
        items = []
        for _ in range(self.rnd.choice([1, 1, 2, 2, 3])):
            dish, price = self.rnd.choice(MENU[cuisine])
            items.append({"dish": dish, "qty": self.rnd.randint(1, 2), "unit_price": price})
        total = round(sum(i["qty"] * i["unit_price"] for i in items), 2)
        order_id = f"QB-{self.next_no}"
        self.next_no += 1
        first, last = self.rnd.choice(FIRST_NAMES), self.rnd.choice(LAST_NAMES)
        cust_no = self.rnd.randint(1, 400)
        v3 = chaos_flags.schema_v3_on(flags)

        doc = {
            "order_id": order_id,
            "schema_v": 3 if v3 else 2,
            "customer": {"customer_id": f"CU-{cust_no:04d}", "name": f"{first} {last}",
                         "contact": {"email": f"{first}.{last}{cust_no}@example.com".lower(),
                                     "phone": f"+972-5{self.rnd.randint(0, 9)}-{self.rnd.randint(1000000, 9999999)}"}},
            "restaurant_id": rest_id,
            "restaurant_name": rest_name,
            "delivery_address": {"street": f"{self.rnd.choice(STREETS)} {self.rnd.randint(1, 180)}",
                                 "city": CITY_SPELLINGS[city][0]},
            "items": items,
            "created_at": now,
            "promised_at": now + timedelta(seconds=PROMISE_SECONDS),
            "payment": {"method": self.rnd.choice(["credit_card", "apple_pay", "google_pay", "bit"])},
            "status": "CREATED" if v3 else "created",
            "updated_at": now,
        }
        if v3:
            # "v3 release": the money moved into the payment sub-document
            doc["payment"]["amount"] = total
        else:
            doc["total"] = total
        self.orders.insert_one(doc)

        o = {"order_id": order_id, "doc": doc, "state": "created", "courier_id": None,
             "next_at": time.time() + self.rnd.uniform(5, 15), "total": total, "v3": v3,
             "city": city}
        self.active[order_id] = o
        self.emit(o, now, flags)

    def advance(self, o, now, flags):
        late = chaos_flags.late_mode_on(flags)
        errors = time.time() < float(flags.get("errors_until", 0))
        s = o["state"]
        if s == "created":
            # payment problems cancel orders (much more often during an "errors" incident)
            if self.rnd.random() < (0.35 if errors else 0.03):
                o["state"], o["cancel_reason"] = "cancelled", "payment_failed"
            elif self.rnd.random() < 0.04:
                o["state"], o["cancel_reason"] = "cancelled", "restaurant_rejected"
            else:
                o["state"] = "accepted"
                o["courier_id"] = self.rnd.choice(COURIERS)
                o["next_at"] = time.time() + self.rnd.uniform(15, 40)
        elif s == "accepted":
            if self.rnd.random() < 0.03:
                o["state"], o["cancel_reason"] = "cancelled", "customer_cancelled"
            else:
                o["state"] = "picked_up"
                # normal: 30-95 s on the road; "late" incident: couriers are stuck in traffic
                o["next_at"] = time.time() + (self.rnd.uniform(140, 260) if late else self.rnd.uniform(30, 90))
        elif s == "picked_up":
            o["state"] = "delivered"
        self.emit(o, now, flags)
        if o["state"] in ("delivered", "cancelled"):
            del self.active[o["order_id"]]

    def emit(self, o, now, flags):
        """Write the change to MongoDB, Kafka and the logs - like a real backend would."""
        v3 = chaos_flags.schema_v3_on(flags)
        state = o["state"]
        status_value = state.upper() if v3 else state      # v3 release also changed the enum casing
        update = {"status": status_value, "updated_at": now}
        if o.get("courier_id"):
            update["courier_id"] = o["courier_id"]
        if state == "delivered":
            update["delivered_at"] = now
        if o.get("cancel_reason"):
            update["cancel_reason"] = o["cancel_reason"]
        self.orders.update_one({"order_id": o["order_id"]}, {"$set": update})

        event = {
            "event_id": str(uuid.UUID(int=self.rnd.getrandbits(128), version=4)),
            "order_id": o["order_id"],
            "event_type": EVENT_TYPE[state],
            "status": status_value,
            "ts": iso(now),
            "restaurant_id": o["doc"]["restaurant_id"],
            "restaurant_name": o["doc"]["restaurant_name"],
            "city": o["city"],
            "courier_id": o.get("courier_id"),
            "promised_at": iso(o["doc"]["promised_at"]),
            "schema_v": 3 if v3 else 2,
        }
        if v3:
            event["payment"] = {"amount": o["total"]}       # v3: total -> payment.amount
        else:
            event["total"] = o["total"]
        if state == "delivered":
            event["delivered_at"] = iso(now)
        if o.get("cancel_reason"):
            event["cancel_reason"] = o["cancel_reason"]
        self.producer.send(ORDER_TOPIC, key=o["order_id"], value=event)
        self.sent += 1

        if o.get("cancel_reason") == "payment_failed":
            self.log("ERROR", "payment-service", "payment declined by provider", o["order_id"],
                     self.rnd.uniform(900, 2500))
        else:
            self.normal_logs(state, o["order_id"])
        marker = {"created": "+", "delivered": "v", "cancelled": "x"}.get(state, ">")
        print(f"[order-app] {marker} {o['order_id']:<10} {state:<10} {o['doc']['restaurant_name']:<22}"
              f" active={len(self.active):<3} events_sent={self.sent}")

    def error_burst(self, flags):
        """During an 'errors' incident the payment provider times out a lot."""
        if time.time() >= float(flags.get("errors_until", 0)):
            return
        for _ in range(self.rnd.randint(3, 7)):
            self.log("ERROR", "payment-service", "payment gateway timeout (provider=PayFast)",
                     None, self.rnd.uniform(3000, 9000))

    def tick(self):
        now = utcnow()
        flags = chaos_flags.read_flags()
        # new orders: on average ORDERS_PER_MIN per minute
        if self.rnd.random() < ORDERS_PER_MIN / 60.0:
            self.new_order(now, flags)
        for o in list(self.active.values()):
            if time.time() >= o["next_at"]:
                self.advance(o, now, flags)
        self.error_burst(flags)
        self.producer.flush()
        self.flush_logs()


def main():
    print(f"[order-app] starting: {ORDERS_PER_MIN:g} orders/min, promise={PROMISE_SECONDS}s, seed={SIM_SEED}")
    producer, orders, es = make_clients()
    app = OrderApp(producer, orders, es, random.Random(SIM_SEED))
    print(f"[order-app] connected. First live order id: QB-{app.next_no}. Ctrl+C to stop.")
    try:
        while True:
            app.tick()
            time.sleep(1)
    except KeyboardInterrupt:
        print("[order-app] stopping")
    finally:
        producer.flush()
        app.flush_logs()


if __name__ == "__main__":
    main()

"""
courier-sim - pretends to be the QuickBite courier app (GPS pings from 30 couriers).
====================================================================================
PROVIDED - you do not need to change this file.

Every PING_SECONDS each courier sends its position to Kafka topic `courier.locations`:

    {"courier_id": "C-07", "lat": 32.0809, "lon": 34.7806, "ts": "2026-11-04T18:22:05.123Z",
     "state": "delivering", "speed_kmh": 21.4}

state is one of: idle / to_restaurant / delivering / stuck
key = courier_id  ->  all pings of one courier land in the same partition, in order.

Couriers move around Tel Aviv / Gush Dan (Ramat Gan, Givatayim, Holon, Herzliya, Petah Tikva).
During the "late" chaos incident many couriers get stuck in traffic (speed ~0).

Environment variables (all optional):
    KAFKA_BOOTSTRAP   default course-kafka:9092
    PING_SECONDS      default 5
    SIM_SEED          default 7
"""
import json
import os
import random
import sys
import time
from datetime import datetime, timezone

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))
from common.catalog import COURIERS, GUSH_DAN  # noqa: E402
from common import chaos_flags  # noqa: E402

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "course-kafka:9092")
PING_SECONDS = float(os.getenv("PING_SECONDS", "5"))
SIM_SEED = int(os.getenv("SIM_SEED", "7"))
TOPIC = "courier.locations"


def iso(dt):
    return dt.strftime("%Y-%m-%dT%H:%M:%S.") + f"{dt.microsecond // 1000:03d}Z"


def clamp(v, lo, hi):
    return max(lo, min(hi, v))


def make_producer():
    from kafka import KafkaProducer
    for i in range(1, 31):
        try:
            return KafkaProducer(bootstrap_servers=KAFKA_BOOTSTRAP, client_id="courier-sim",
                                 key_serializer=lambda k: k.encode("utf-8"),
                                 value_serializer=lambda v: json.dumps(v).encode("utf-8"))
        except Exception as e:  # noqa: BLE001
            print(f"[courier-sim] waiting for Kafka ({i}/30): {e}")
            time.sleep(5)
    raise SystemExit("[courier-sim] could not connect to Kafka")


def main():
    rnd = random.Random(SIM_SEED)
    couriers = {}
    for cid in COURIERS:
        couriers[cid] = {
            "lat": rnd.uniform(GUSH_DAN["lat_min"], GUSH_DAN["lat_max"]),
            "lon": rnd.uniform(GUSH_DAN["lon_min"], GUSH_DAN["lon_max"]),
            "state": rnd.choice(["idle", "to_restaurant", "delivering"]),
            "heading": (rnd.uniform(-1, 1), rnd.uniform(-1, 1)),
        }
    producer = make_producer()
    print(f"[courier-sim] {len(couriers)} couriers, one ping every {PING_SECONDS:g}s. Ctrl+C to stop.")
    rounds = 0
    try:
        while True:
            late = chaos_flags.late_mode_on()
            now = datetime.now(timezone.utc)
            stuck = 0
            for i, (cid, c) in enumerate(couriers.items()):
                if rnd.random() < 0.05:                       # sometimes change job / direction
                    c["state"] = rnd.choice(["idle", "to_restaurant", "delivering"])
                    c["heading"] = (rnd.uniform(-1, 1), rnd.uniform(-1, 1))
                if late and i % 3 != 0:                       # 2 of every 3 couriers stuck in traffic
                    state, speed = "stuck", rnd.uniform(0, 2)
                    stuck += 1
                else:
                    state = c["state"]
                    speed = 0.0 if state == "idle" else rnd.uniform(12, 28)
                    step = speed / 3600 * PING_SECONDS / 111      # km/h -> degrees per ping (rough)
                    c["lat"] = clamp(c["lat"] + c["heading"][0] * step, GUSH_DAN["lat_min"], GUSH_DAN["lat_max"])
                    c["lon"] = clamp(c["lon"] + c["heading"][1] * step, GUSH_DAN["lon_min"], GUSH_DAN["lon_max"])
                ping = {"courier_id": cid, "lat": round(c["lat"], 6), "lon": round(c["lon"], 6),
                        "ts": iso(now), "state": state, "speed_kmh": round(speed, 1)}
                producer.send(TOPIC, key=cid, value=ping)
            producer.flush()
            rounds += 1
            if rounds % 6 == 1:
                print(f"[courier-sim] round {rounds}: sent {len(couriers)} pings"
                      + (f"  ({stuck} couriers reporting state=stuck)" if stuck else ""))
            time.sleep(PING_SECONDS)
    except KeyboardInterrupt:
        print("[courier-sim] stopping")
    finally:
        producer.flush()


if __name__ == "__main__":
    main()

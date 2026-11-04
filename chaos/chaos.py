"""
chaos.py - production incidents on demand (handout Part 5).
=============================================================
Run ONE mode at a time, then play detective: what broke, where did you SEE it,
and how do you fix it? This script does not tell you what it really did - on purpose.

    docker compose exec apps python /chaos/chaos.py drift        # a new app release goes out
    docker compose exec apps python /chaos/chaos.py duplicates   # a network blip on the producer side
    docker compose exec apps python /chaos/chaos.py errors       # a partner has a bad afternoon
    docker compose exec apps python /chaos/chaos.py late         # Friday evening traffic
    docker compose exec apps python /chaos/chaos.py status       # what is active right now
    docker compose exec apps python /chaos/chaos.py reset        # back to normal (also ends "drift")

The simulators must be running (Part 0).
Answers (only after you tried!): solution/part5_chaos_answers.md
"""
import json
import os
import sys
import time

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "apps"))
sys.path.insert(0, "/apps")
from common import chaos_flags  # noqa: E402

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "course-kafka:9092")
ORDER_TOPIC = "order.events"


def drift(flags):
    flags["schema_v3"] = True
    chaos_flags.write_flags(flags)
    print("v3 release deployed to production.")
    print("Release notes: \"performance improvements and a cleaner payment model\".")


def duplicates(flags, how_many=300):
    """Re-send the most recent order events (same event_id!) - like a producer retry storm."""
    from kafka import KafkaConsumer, KafkaProducer, TopicPartition

    consumer = KafkaConsumer(bootstrap_servers=KAFKA_BOOTSTRAP, enable_auto_commit=False,
                             consumer_timeout_ms=5000)
    partitions = [TopicPartition(ORDER_TOPIC, p) for p in sorted(consumer.partitions_for_topic(ORDER_TOPIC) or [])]
    if not partitions:
        raise SystemExit("topic order.events not found - are the simulators running?")
    consumer.assign(partitions)
    end = consumer.end_offsets(partitions)
    per_part = max(1, how_many // len(partitions))
    for tp in partitions:
        consumer.seek(tp, max(0, end[tp] - per_part))
    replay = []
    for msg in consumer:
        replay.append((msg.key, msg.value))
        if all(consumer.position(tp) >= end[tp] for tp in partitions):
            break
    consumer.close()

    producer = KafkaProducer(bootstrap_servers=KAFKA_BOOTSTRAP)
    for key, value in replay:
        producer.send(ORDER_TOPIC, key=key, value=value)
    producer.flush()
    print("Network blip on the order-app producer. It has recovered.")


def errors(flags, minutes=3):
    flags["errors_until"] = time.time() + minutes * 60
    chaos_flags.write_flags(flags)
    print("Our payment provider reported \"degraded performance\". Monitoring the situation.")


def late(flags, minutes=5):
    flags["late_until"] = time.time() + minutes * 60
    chaos_flags.write_flags(flags)
    print("Friday evening. It is raining in Tel Aviv.")


def status(flags):
    now = time.time()
    print(json.dumps({
        "schema_v3 (drift)": bool(flags.get("schema_v3")),
        "errors active, seconds left": max(0, round(float(flags.get("errors_until", 0)) - now)),
        "late active, seconds left": max(0, round(float(flags.get("late_until", 0)) - now)),
    }, indent=2))


def reset(_flags):
    chaos_flags.write_flags({})
    print("All incidents closed. (The data they produced is still there - that is real life.)")


MODES = {"drift": drift, "duplicates": duplicates, "errors": errors, "late": late,
         "status": status, "reset": reset}


def main(argv):
    if len(argv) != 2 or argv[1] not in MODES:
        print(__doc__)
        print("usage: python chaos.py {" + "|".join(MODES) + "}")
        return 2
    MODES[argv[1]](chaos_flags.read_flags())
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))

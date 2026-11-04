"""
Tiny "feature flag" file shared by chaos/chaos.py and the two simulators.

chaos.py writes a JSON file, the simulators re-read it every loop.
Everything runs inside the same `apps` container, so a file in /tmp is enough.
"""
import json
import os
import time

FLAGS_FILE = os.getenv("CHAOS_FILE", "/tmp/quickbite_chaos.json")


def read_flags():
    try:
        with open(FLAGS_FILE, encoding="utf-8") as f:
            return json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        return {}


def write_flags(flags):
    tmp = FLAGS_FILE + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(flags, f)
    os.replace(tmp, FLAGS_FILE)      # atomic: readers never see a half-written file


def schema_v3_on(flags=None):
    return bool((flags if flags is not None else read_flags()).get("schema_v3"))


def late_mode_on(flags=None):
    flags = flags if flags is not None else read_flags()
    return time.time() < float(flags.get("late_until", 0))

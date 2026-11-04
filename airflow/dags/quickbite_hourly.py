"""
Part 4 - the QuickBite hourly batch pipeline   (YOU write the parts marked TODO)
=================================================================================
MongoDB (quickbite.orders)  --incremental-->  pandas (Silver + data quality)  -->  Gold CSVs
                                                                               -->  Elasticsearch daily-summary

    extract_orders -> transform_to_silver -> quality_gate --+--> publish_gold -> index_daily_summary -> update_watermark
                                                            |
                                                            +--> quarantine_and_alert

Variables (Admin -> Variables). All have defaults, so the DAG works without creating them:
    quickbite_watermark        default 1970-01-01T00:00:00.000Z   last updated_at we fully processed
    quickbite_max_bad_pct      default 10                          max % of rows with missing status / bad total
    quickbite_max_duplicates   default 5                           max duplicated order_id rows per run

Why incremental? A full extract re-reads every order every hour. With a WATERMARK we only read
documents whose updated_at is NEWER than the last successful run. The watermark is written ONLY
in the very last task, so a failed run is simply repeated next time (nothing is lost).

    In the Spark module you will replace this pandas transform with PySpark.
    (The Airflow part - extract, branch, watermark - stays almost the same.)

The file already loads in Airflow as it is (no import errors), but the tasks marked TODO
do not do their job yet. The pandas functions live in quickbite_transform.py (also TODOs).

Stuck? See solution/airflow/dags/quickbite_hourly.py
"""
import json
import os
import sys
from datetime import datetime, timedelta, timezone

from airflow import DAG
from airflow.models import Variable
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import BranchPythonOperator, PythonOperator

# Make "import quickbite_transform" work no matter how Airflow loads this file.
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import quickbite_transform as qt  # noqa: E402

# readPreference=secondaryPreferred: a batch job must not compete with the app on the primary.
# In production this points at a dedicated analytics secondary (or you use CDC instead - Appendix A).
MONGO_URI = os.getenv("MONGO_URI", "mongodb://mongo:27017/?directConnection=true&readPreference=secondaryPreferred")
ES_URL = os.getenv("ES_URL", "http://elasticsearch:9200")
OUTPUT_DIR = os.getenv("QUICKBITE_OUTPUT", "/opt/airflow/output")

WATERMARK_VAR = "quickbite_watermark"
DEFAULT_WATERMARK = "1970-01-01T00:00:00.000Z"
SUMMARY_INDEX = "daily-summary"
ALERTS_INDEX = "pipeline-alerts"

default_args = {
    "owner": "data_team",
    # TODO (Part 4.0): retry every task 2 times, 30 seconds apart, before failing the run
}


def _dirs(run_id):
    """Every run gets its own staging folder, so two runs never overwrite each other's files."""
    safe = run_id.replace(":", "_").replace("+", "_")
    paths = {
        "staging": os.path.join(OUTPUT_DIR, "staging", safe),
        "silver": os.path.join(OUTPUT_DIR, "silver"),
        "gold": os.path.join(OUTPUT_DIR, "gold"),
        "quarantine": os.path.join(OUTPUT_DIR, "quarantine"),
    }
    for p in paths.values():
        os.makedirs(p, exist_ok=True)
    return paths


def _parse_watermark(text):
    return datetime.strptime(text, "%Y-%m-%dT%H:%M:%S.%fZ").replace(tzinfo=timezone.utc)


def _format_watermark(dt):
    dt = dt.astimezone(timezone.utc) if dt.tzinfo else dt.replace(tzinfo=timezone.utc)
    return dt.strftime("%Y-%m-%dT%H:%M:%S.") + f"{dt.microsecond // 1000:03d}Z"


# ------------------------------------------------------------------------------------
# Task 1 - EXTRACT (incremental)
# ------------------------------------------------------------------------------------
def extract_orders(run_id, **_):
    from bson.json_util import JSONMode, JSONOptions, dumps
    from pymongo import MongoClient

    watermark = Variable.get(WATERMARK_VAR, default_var=DEFAULT_WATERMARK)
    since = _parse_watermark(watermark)

    coll = MongoClient(MONGO_URI, serverSelectionTimeoutMS=10000)["quickbite"]["orders"]

    # TODO (Part 4.1): an INCREMENTAL extract.
    #   - read only documents whose updated_at is GREATER THAN `since`  ($gt)
    #   - sort them by updated_at ascending (then _id) so the newest one is last
    #   - new_watermark = the updated_at of the LAST document you read (use _format_watermark),
    #     or the old watermark if nothing changed.  Why not datetime.now()?  -> handout Part 4.1
    # Right now it reads EVERYTHING every run and never moves the watermark:
    docs = list(coll.find({}))
    new_watermark = watermark

    paths = _dirs(run_id)
    raw_path = os.path.join(paths["staging"], "orders_raw.json")
    # Canonical Extended JSON keeps BSON types (Decimal128, Date) so the transform sees the
    # SAME messy types MongoDB has. Plain json.dumps would crash on Decimal128.
    with open(raw_path, "w", encoding="utf-8") as f:
        f.write(dumps(docs, json_options=JSONOptions(json_mode=JSONMode.CANONICAL)))

    print(f"watermark={watermark}  ->  extracted {len(docs)} changed orders  ->  new watermark={new_watermark}")
    # XCom: only SMALL things (a path and a few numbers), never the data itself.
    return {"raw_path": raw_path, "rows": len(docs), "old_watermark": watermark, "new_watermark": new_watermark}


# ------------------------------------------------------------------------------------
# Task 2 - TRANSFORM to Silver + data-quality report
# ------------------------------------------------------------------------------------
def transform_to_silver(run_id, ti, **_):
    from bson.json_util import loads

    info = ti.xcom_pull(task_ids="extract_orders")
    with open(info["raw_path"], encoding="utf-8") as f:
        docs = loads(f.read())

    df = qt.flatten_orders(docs)
    clean, quarantine, report = qt.split_quality(df)

    paths = _dirs(run_id)
    clean_path = os.path.join(paths["staging"], "silver_batch.csv")
    quarantine_path = os.path.join(paths["staging"], "quarantine_batch.csv")
    clean.to_csv(clean_path, index=False)
    quarantine.to_csv(quarantine_path, index=False)

    print("Data-quality report:\n" + json.dumps(report, indent=2))
    if not quarantine.empty:
        print("Quarantined rows (first 10):\n" + quarantine[["order_id", "reason"]].head(10).to_string(index=False))
    return {**report, "clean_path": clean_path, "quarantine_path": quarantine_path}


# ------------------------------------------------------------------------------------
# Task 3 - QUALITY GATE (BranchPythonOperator returns the task_id to run next)
# ------------------------------------------------------------------------------------
def quality_gate(ti, **_):
    report = ti.xcom_pull(task_ids="transform_to_silver")
    max_bad_pct = float(Variable.get("quickbite_max_bad_pct", default_var="10"))
    max_dups = int(Variable.get("quickbite_max_duplicates", default_var="5"))
    ok = qt.quality_decision(report, max_bad_pct, max_dups)
    print(f"bad_pct={report['bad_pct']} (max {max_bad_pct}), duplicate_rows={report['duplicate_rows']} "
          f"(max {max_dups}) -> ok={ok}")
    # TODO (Part 4.3): a BranchPythonOperator must RETURN THE task_id TO RUN NEXT.
    #   ok -> "publish_gold",  not ok -> "quarantine_and_alert"
    # Right now it always publishes, even when the data is bad:
    return "publish_gold"


# ------------------------------------------------------------------------------------
# Task 4a - GOLD (only when the gate passed)
# ------------------------------------------------------------------------------------
def _load_restaurants():
    import pandas as pd
    from pymongo import MongoClient
    coll = MongoClient(MONGO_URI, serverSelectionTimeoutMS=10000)["quickbite"]["restaurants"]
    return pd.DataFrame(list(coll.find({}, {"_id": 0, "restaurant_id": 1, "name": 1, "commission_pct": 1})))


def publish_gold(run_id, ti, **_):
    import pandas as pd

    report = ti.xcom_pull(task_ids="transform_to_silver")
    paths = _dirs(run_id)
    silver_path = os.path.join(paths["silver"], "orders_silver.csv")

    batch = qt.read_silver_csv(report["clean_path"])
    existing = qt.read_silver_csv(silver_path) if os.path.exists(silver_path) else None
    silver = qt.merge_silver(existing, batch)               # UPSERT by order_id
    silver.to_csv(silver_path, index=False)

    payouts = qt.courier_payouts(silver)
    fees = qt.restaurant_fees(silver, _load_restaurants())
    summary = qt.daily_summary(silver, payouts, fees)

    payouts.to_csv(os.path.join(paths["gold"], "courier_payouts.csv"), index=False)
    fees.to_csv(os.path.join(paths["gold"], "restaurant_fees.csv"), index=False)
    summary_path = os.path.join(paths["staging"], "daily_summary.csv")
    summary.to_csv(summary_path, index=False)

    # keep the (good-enough) batch's rejected rows too, so nobody loses them
    q = pd.read_csv(report["quarantine_path"])
    if not q.empty:
        q.to_csv(os.path.join(paths["quarantine"], f"quarantine_{run_id.replace(':', '_')}.csv"), index=False)

    print(f"silver={len(silver)} orders | courier_payouts={len(payouts)} rows | "
          f"restaurant_fees={len(fees)} rows | days={len(summary)}")
    return {"summary_path": summary_path, "silver_rows": int(len(silver))}


def index_daily_summary(run_id, ti, **_):
    import pandas as pd
    from elasticsearch import Elasticsearch, helpers

    info = ti.xcom_pull(task_ids="publish_gold")
    report = ti.xcom_pull(task_ids="transform_to_silver")
    summary = pd.read_csv(info["summary_path"], dtype={"date": str})
    run_info = {"run_id": run_id, "indexed_at": qt.utc_now_iso(),
                "quarantined_rows_last_run": report["quarantined_rows"]}
    docs = qt.summary_docs(summary, run_info)
    es = Elasticsearch(ES_URL, request_timeout=30)
    # TODO (Part 4.4): build one "index" bulk action per doc into SUMMARY_INDEX and send them
    # with helpers.bulk(es, actions, raise_on_error=True).
    # Choose the _id so that re-running the DAG does NOT create a second copy of the same day.
    actions = []
    print(f"built {len(actions)} daily-summary actions for {len(docs)} days (TODO: send them)")


def update_watermark(ti, **_):
    info = ti.xcom_pull(task_ids="extract_orders")
    # TODO (Part 4.5): save info["new_watermark"] in the Airflow Variable WATERMARK_VAR.
    # Why is this the LAST task and not part of extract_orders?  -> handout Part 4.5
    print(f"TODO: watermark should move {info['old_watermark']} -> {info['new_watermark']}")


# ------------------------------------------------------------------------------------
# Task 4b - QUARANTINE (gate failed): keep the evidence, raise an alert, do NOT move the watermark
# ------------------------------------------------------------------------------------
def quarantine_and_alert(run_id, ti, **_):
    import pandas as pd
    from elasticsearch import Elasticsearch

    report = ti.xcom_pull(task_ids="transform_to_silver")
    paths = _dirs(run_id)
    target = os.path.join(paths["quarantine"], f"quarantine_{run_id.replace(':', '_')}.csv")
    pd.read_csv(report["quarantine_path"]).to_csv(target, index=False)

    alert = {k: v for k, v in report.items() if not k.endswith("_path")}
    alert.update({"@timestamp": qt.utc_now_iso(), "run_id": run_id, "level": "ALERT",
                  "message": "quickbite_hourly: data-quality gate FAILED - Gold not published, watermark not moved"})
    try:
        Elasticsearch(ES_URL, request_timeout=30).index(index=ALERTS_INDEX, document=alert)
    except Exception as e:  # noqa: BLE001 - the alert must not hide the original problem
        print(f"could not index alert: {e}")
    print("!" * 70)
    print("DATA-QUALITY GATE FAILED. Gold tables were NOT updated and the watermark was NOT moved,")
    print("so the next run re-reads the same documents. Look at:", target)
    print(json.dumps(alert, indent=2, default=str))
    print("!" * 70)


with DAG(
    dag_id="quickbite_hourly",
    default_args=default_args,
    description="QuickBite: incremental Mongo extract -> pandas Silver + DQ -> Gold CSV + daily-summary",
    schedule_interval="@hourly",
    start_date=datetime(2026, 11, 1),
    catchup=False,                 # do not create a run for every hour since start_date
    max_active_runs=1,             # two runs at once would race on the watermark
    tags=["quickbite", "capstone"],
) as dag:

    t_extract = PythonOperator(task_id="extract_orders", python_callable=extract_orders)
    t_transform = PythonOperator(task_id="transform_to_silver", python_callable=transform_to_silver)
    t_gate = BranchPythonOperator(task_id="quality_gate", python_callable=quality_gate)
    t_gold = PythonOperator(task_id="publish_gold", python_callable=publish_gold)
    t_summary = PythonOperator(task_id="index_daily_summary", python_callable=index_daily_summary)
    t_watermark = PythonOperator(task_id="update_watermark", python_callable=update_watermark)
    t_quarantine = PythonOperator(task_id="quarantine_and_alert", python_callable=quarantine_and_alert)
    # TODO (Part 4.3): after a branch, ONE of the two paths is always "skipped".
    #   With the default trigger_rule ("all_success") `done` is then skipped too.
    #   Give `done` a trigger_rule that means "nothing failed and at least one upstream succeeded".
    #   (look up "none_failed_min_one_success" in the Airflow docs)
    t_done = EmptyOperator(task_id="done")

    t_extract >> t_transform >> t_gate
    t_gate >> t_gold >> t_summary >> t_watermark >> t_done
    t_gate >> t_quarantine >> t_done

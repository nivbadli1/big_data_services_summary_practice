# TESTING.md - what was verified, and what was not

Docker image registries were **blocked** in the build sandbox (`docker pull` -> Forbidden), so **the docker-compose stack was never started**. Everything below was tested outside Docker.

## Verified

| What | How | Result |
|---|---|---|
| `docker-compose.yml` syntax, service graph, profiles | `docker compose config -q` (Compose v5.6) | valid; 6 default services + `kafdrop` behind profile `ui` |
| All Python compiles | `python -m py_compile` on every `.py` (3.8 and 3.11) | OK |
| Seed generator determinism + **every Part 1 expected number** | `seed_orders.py` loaded into a **real MongoDB 8.0.23** (conda-forge binary) and the **full** `solution/part1_mongo/explore.js` run with **real mongosh 2.3.8**. Numbers also recomputed in plain Python and with mongomock (`tests/test_seed_expected.py`, incl. `-m mongo` against the real server) | all match the handout (table in the PR) |
| Student `part1_mongo/explore.js` | run with mongosh against the real seed | runs without errors and does not modify data until the student writes 1.3 |
| Pandas transform (normalize, DQ, Silver upsert, Gold, daily summary) | `tests/test_transform.py` on **Python 3.8 + pandas 2.0.3** (what `apache/airflow:2.8.4` ships) and Python 3.11 | 46/46 pass (solution) |
| Consumer logic (dedupe, `is_late`, upsert actions, geo_point, batch stats, drift fix) | `tests/test_consumer.py` | 16/16 pass (solution) |
| Consumer main loop order (bulk **then** commit, group / earliest / no auto-commit) | `main()` driven with a fake `KafkaConsumer` + a minimal fake ES HTTP server, real `elasticsearch==8.15.1` client | calls: poll -> bulk -> commit; correct `_bulk` bodies |
| **DAG on Airflow 2.8.4** | `apache-airflow==2.8.4` installed with the official constraints (Python 3.8), `airflow dags list-import-errors` (none), `airflow dags test quickbite_hourly` against the **real MongoDB** + fake ES | **solution**: all tasks success; 500 rows extracted, 462 clean / 38 quarantined, 290 payout rows, 274 fee rows, 30 days, watermark `2026-10-30T22:47:07.976Z`. Second run: 0 rows. After the 1.3 status fix: exactly 57 rows. With `quickbite_max_bad_pct=5`: quarantine branch, watermark not moved. **student** skeleton: imports, runs, writes Gold, does not crash |
| order-app simulator logic | `OrderApp` driven with a fake clock (30 sim-minutes) + a recording producer, writing to the real MongoDB | ids start at `QB-200000`, key = order_id, unique event_ids, `updated_at` on every Mongo write, ~15% late; drift/late/errors flags change events, Mongo docs and logs as designed |
| chaos.py | all modes run against a fake Kafka | `duplicates` re-sends 300 events; flags written / reset |
| Student skeletons | tests with `--target=student` | they fail as expected (TODOs), they do not crash |

## Verified on a real machine (Niv, MacBook, Docker Desktop, 4 Nov 2026)

- `docker compose up -d --build`: all services healthy (after removing leftover containers from earlier lessons and freeing port 8080).
- Airflow image build (after dropping the remote constraints URL), `airflow standalone`, `./output` writable on Mac.
- Seed: 500 orders + 20 restaurants. `solution/part1_mongo/explore.js` output matches every expected count (late 117, bonus index 88, top dishes).
- `pytest /tests --target=solution` in the apps container: 65 passed, 1 skipped.
- Index templates (also via `solution/part3_elk/apply_templates.py`), `courier-locations.location` = `geo_point`.
- Order-app refuses to start without the `app-logs` template, then starts once it exists.
- Consumer: reads both topics, upserts `deliveries-live` and `courier-locations`, late refresh marks open orders, resumes from committed offsets after being stopped (LAG back to 0), `chaos.py duplicates` -> 23 duplicates skipped.
- DAG (`airflow dags test`): first run 500 -> 462 clean / 38 quarantined, payouts 290, fees 274, 30 daily-summary docs; student skeleton runs end to end without crashing; incremental run after `chaos.py drift` picks up only changed orders (177) and the solution handles v3 (`bad_total` 0).
- `chaos.py errors`, `late`, `drift` run; v3 orders appear in Mongo (`payment.amount`).

## Still UNTESTED

1. Kibana click paths in the handout and `solution/part3_dashboard_steps.md` (Lens, Maps layer, menu names) - walk through them once in the browser.
2. Windows / WSL2 and Linux hosts (Linux may need `chmod 777 output`).
3. Live-rate numbers in the handout (events per minute, error rate) are estimates.

## How to re-run the automated checks

```bash
docker compose exec apps pytest -q /tests/                                  # solution
MONGO_URI=mongodb://mongo:27017/?directConnection=true docker compose exec apps pytest -q /tests/ -m mongo   # real Mongo
```

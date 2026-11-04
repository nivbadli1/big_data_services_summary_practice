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

## UNTESTED - run once on a real machine before giving it to students

1. **`docker compose up -d --build` end to end** on an 8 GB laptop (Mac + Windows/WSL2 ideally): all healthchecks go healthy, the memory limits are enough, and the start order works.
2. **Kafka KRaft config** of `apache/kafka:3.8.1` (env vars, `CLUSTER_ID`, `/opt/kafka/bin` paths in the handout, healthcheck command), and topic auto-creation by the order-app admin client.
3. **Airflow container**: the Dockerfile build (pip + constraints URL), `airflow standalone` with the SQLite volume `/opt/airflow/db` (permissions for UID 50000), the `airflow/airflow` user creation, `/health` healthcheck, the `./output` bind-mount being writable by UID 50000 (on Linux hosts you may need `chmod 777 output` or an `.env` with `AIRFLOW_UID=$(id -u)`).
4. **Real Elasticsearch / Kibana 8.15.3**: the index templates in `mappings.http`, `helpers.bulk` upserts, `update_by_query` (late refresh), the geo_point map, every DSL query, and all Kibana click paths in the handout and `solution/part3_dashboard_steps.md` (Lens formulas, Maps layer, panel custom time range). Kibana menu names were written from memory of 8.x.
5. **Live numbers** in the handout (doc counts per minute, ~1-2% error rate, LAG, console-consumer output shape) are estimates from the simulation, not measured on the real stack.
6. Real kafka-python 2.2.15 against the broker: `consumer.commit()`, the `chaos.py duplicates` seek/replay, consumer-group describe output.
7. The order-app's refusal to start without the `app-logs` template (`exists_index_template`).
8. The Mongo healthcheck replica-set init (copied from the cde_mongodb lesson, which was tested there).

## How to re-run the automated checks

```bash
docker compose exec apps pytest -q /tests/                                  # solution
MONGO_URI=mongodb://mongo:27017/?directConnection=true docker compose exec apps pytest -q /tests/ -m mongo   # real Mongo
```

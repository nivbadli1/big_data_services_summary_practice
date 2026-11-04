# Part 5 - Incident drill: answers

For each mode: **what broke / where you see it / how to fix it**. The code fixes are in the solution files; search them for `Part 5`.

## 5.1 `drift` - "v3 release deployed"

**What broke.** The app's v3 release changed the event and document contract without telling the data team:
- `status` is now UPPER CASE (`"DELIVERED"`), the same problem you fixed in Mongo in Part 1.3, now coming in live.
- `total` was **moved** to `payment.amount` (in the Kafka events and in the Mongo documents). `schema_v` is 3.

**Where you see it.**
- Kibana: **Late deliveries** stops growing and **Late % by city** drops, because the panels filter `status:delivered` and `keyword` is case-sensitive. Discover on `deliveries-live`: new docs have `status: DELIVERED` and **no `total`**.
- The consumer does not crash. That is the dangerous part: the data is wrong without any error.
- Airflow: `transform_to_silver` shows `bad_total` jumping (every v3 order has no `total`). The **quality gate** fails -> `quarantine_and_alert` runs and Gold is not published. **The DQ gate did its job**: wrong revenue numbers never reached Finance.

**Fix.**
- `consumer.py`: `normalize_status()` lower-cases; `event_total()` coalesces `total` / `payment.amount`.
- `quickbite_transform.py`: `order_total()` coalesces too (`normalize_status` already lower-cases since Part 4).
- Restart the consumer. Old wrong docs fix themselves when their order's next event arrives (upsert). For a full fix, reset the group offsets to earliest (Appendix B.6) and replay: the index is rebuilt with correct values.
- Trigger the DAG again. The watermark did not move during the failed runs, so the same window is re-processed and nothing is lost.
- The real lesson: a **data contract** (schema registry, `$jsonSchema` validation in Mongo, a release checklist that includes the data team) would have caught this before production.

## 5.2 `duplicates` - "network blip on the producer"

**What broke.** The last ~300 events were re-sent with the **same `event_id`** (what a producer retry storm or a replay looks like).

**Where you see it.** The consumer line `duplicates skipped=` jumps by ~300. The `deliveries-live` doc count does **not** change, and Late deliveries does not double. **Nothing broke**: this is the at-least-once design working.

**Why it holds.** Two independent safety nets:
1. **Dedupe** by `event_id` (`SeenEvents`) skips them.
2. **Idempotent write**: even if a duplicate gets through, `_id = order_id` + upsert writes the same values into the same document.

**Second run after a consumer restart.** `SeenEvents` lives **in memory**, so after a restart it is empty and the replayed events are **not** skipped (`duplicates skipped` stays 0). The counts are still correct, because net #2 (upsert by `order_id`) absorbs them. That is why you never rely on in-memory dedupe alone. Without both nets (`_op_type: index` with auto ids), every replayed event would be a new doc and every count on the dashboard would be inflated.

## 5.3 `errors` - "payment provider degraded"

**What broke.** For 3 minutes `payment-service` logs bursts of `ERROR payment gateway timeout (provider=PayFast)` with 3-9 s latency, and ~35% of new orders are cancelled with `cancel_reason: payment_failed`.

**Where you see it.** **Error rate** jumps from ~1-2% to a large share, all in `payment-service`. **Slowest services** shows p95 `latency_ms` for payment-service in the thousands. Discover: `level:ERROR and service:"payment-service"`. In `deliveries-live`: `cancel_reason:payment_failed`.

**Fix.** There is no code bug: it is an **external** incident. The data team's job is to make it **visible** and **countable**:
- Ops: a Kibana alert rule on the error rate (Stack Management -> Rules) would page someone instead of waiting for a person to look.
- Finance: `daily_summary()` now has `payment_failed` per day (see the solution `quickbite_transform.py`) -> visible in `daily-summary`.
- Notice: the logs come straight from the app into Elasticsearch, so the logs keep flowing even if Kafka has a problem. This is a different path with different failure modes.

## 5.4 `late` - "Friday evening, raining in Tel Aviv"

**What broke.** For 5 minutes 2 of every 3 couriers are `stuck` (speed ~0), and picked-up orders take 140-260 s instead of 30-90 s, longer than the 120 s promise.

**Where you see it.** The **map** turns into one colour (`state: stuck`). Late deliveries jumps, but **only after** the stuck orders are finally delivered, minutes later.

**The hidden bug.** During the incident, `status:picked_up and promised_at < now` returns many orders, but `status:picked_up and is_late:true` returns **few or none**. `is_late` is computed when an **event** arrives. A stuck order produces **no new event** between `picked_up` and `delivered`, so its stored flag stays `false`. The dashboard under-reports exactly while the incident is happening.

**Fix (two valid options).**
1. **Query-time:** don't trust the stored flag for open orders. In Kibana use `is_late:true or (status:(created or accepted or picked_up) and promised_at < now)`.
2. **Refresh:** the solution consumer runs `refresh_stale_late_flags()` every 30 s, an `update_by_query` that sets `is_late=true` on open orders whose promise has passed. Stored flags are then at most 30 s stale.

The general lesson: a value derived from **time** ("late") goes stale even when no data arrives. Either compute it when you read it, or have a job that re-computes it.

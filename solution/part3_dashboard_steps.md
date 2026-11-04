# Part 3.4 - Building the "QuickBite Ops" dashboard, step by step

Written for **Kibana 8.15**. Menu names can move slightly between 8.x versions; the ideas are the same.
Before you start: the 4 data views from handout Part 3.3 exist, the consumer is running.

## Setup
1. ☰ -> **Analytics -> Dashboards** -> **Create dashboard**.
2. Time picker (top right) -> **Last 30 minutes**. Click the calendar icon -> **Refresh every 10 seconds** -> Start.

## Panel 1 - Couriers now (map)
1. **Add panel** (or "Add from library" / the Maps icon in the toolbar) -> **Maps**. In older 8.x: ☰ -> Maps -> Create map, then add the saved map to the dashboard later.
2. **Add layer -> Documents**. Data view `courier-locations`. Geospatial field `location` (only offered if it is a `geo_point`; if you don't see it, the template was missing - see handout 3.1).
3. Layer style -> **Fill color -> By value -> `state`**. Optional: tooltip fields `courier_id`, `state`, `speed_kmh`.
4. Zoom to Tel Aviv. **Save & return**. Title: *Couriers now*.

Why it works without a time filter problem: `courier-locations` keeps one doc per courier (`_id = courier_id`), and its `ts` is refreshed every ping, so all 30 are inside "Last 30 minutes".

## Panel 2 - Late deliveries (metric)
1. **Create visualization** (opens Lens). Data view `deliveries-live`.
2. Chart type **Metric**. Primary metric **Count of records**.
3. KQL bar of the panel: `status:delivered and is_late:true`. Title *Late deliveries*. **Save and return**.
Optional: duplicate the panel with the KQL `status:(created or accepted or picked_up) and promised_at < now`, titled *Late right now (open)* (the lesson of Part 5.4).

## Panel 3 - Late % by city
1. Lens, data view `deliveries-live`, chart type **Bar vertical**.
2. Horizontal axis: `city`, **Top 8 values**.
3. Vertical axis: **Formula** -> `count(kql='is_late:true') / count() * 100`. Value format: Number, 1 decimal. Name: *Late %*.
4. KQL bar: `status:delivered`. Title *Late % by city*.

## Panel 4 - Error rate
1. Lens, data view `app-logs`, chart type **Line**.
2. Horizontal axis: `@timestamp`, Date histogram, minimum interval **30s**.
3. Vertical axis: **Formula** `count(kql='level:ERROR') / count() * 100`, name *Error %*.
4. **Breakdown**: `service`, top 3. Title *Error rate (%)*.

## Panel 5 - Slowest services
1. Lens, data view `app-logs`, chart type **Table**.
2. Rows: `service`. Metrics: **Count**, **Percentile 95** of `latency_ms`, **Average** of `latency_ms`.
3. Sort by p95 descending. Title *Slowest services*.

## Panel 6 - Revenue per day (after Part 4)
1. Lens, data view `daily-summary`, chart type **Bar vertical**.
2. Horizontal axis `date`, Date histogram, interval **1 day**.
3. Vertical axis **Sum** of `revenue_ils`. Optional second layer: line of `late_pct` (Average).
4. **Save and return**, then panel ⋯ -> **Panel settings -> Apply a custom time range -> Last 60 days** (the history is from October, so the dashboard's "Last 30 minutes" would hide it).

## Save
Top right **Save** -> title **QuickBite Ops**, "Store time with dashboard" on.

## Export (optional)
☰ -> Stack Management -> **Saved objects** -> select the dashboard -> **Export** (include related objects). The `.ndjson` file can be imported again after `docker compose down -v`.

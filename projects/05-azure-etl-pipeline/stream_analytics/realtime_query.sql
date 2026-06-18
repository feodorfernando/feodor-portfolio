-- realtime_query.sql
-- Azure Stream Analytics job: Event Hub -> (hot path) Power BI + (cold path) ADLS gen2.
-- Tumbling-window aggregation for the live dashboard, raw passthrough for the lake.
-- Author: Feodor Fernando

-- 1) HOT PATH: 1-minute revenue + event rate per region, pushed to Power BI streaming dataset.
SELECT
    System.Timestamp()              AS window_end,
    region,
    COUNT(*)                        AS event_count,
    SUM(amount)                     AS revenue,
    AVG(amount)                     AS avg_order_value
INTO
    [powerbi-live]
FROM
    [eventhub-input] TIMESTAMP BY event_time
GROUP BY
    region,
    TumblingWindow(minute, 1);

-- 2) COLD PATH: land every raw event in ADLS gen2 partitioned by date for batch/Synapse.
SELECT
    event_id,
    user_id,
    region,
    amount,
    event_time
INTO
    [adls-raw]            -- output configured with path pattern: raw/events/{date}/{time}
FROM
    [eventhub-input] TIMESTAMP BY event_time;

-- 3) ANOMALY PATH: flag spikes (>3x the 10-min average) onto a Service Bus alert queue.
WITH baseline AS (
    SELECT region, AVG(amount) AS avg_amt
    FROM [eventhub-input] TIMESTAMP BY event_time
    GROUP BY region, SlidingWindow(minute, 10)
)
SELECT e.event_id, e.region, e.amount, b.avg_amt
INTO [servicebus-alerts]
FROM [eventhub-input] e TIMESTAMP BY event_time
JOIN baseline b ON e.region = b.region AND DATEDIFF(minute, e, b) BETWEEN 0 AND 10
WHERE e.amount > 3 * b.avg_amt;

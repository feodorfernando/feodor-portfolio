-- create_external_tables.sql
-- Synapse Serverless SQL: expose ADLS gen2 Parquet as external tables for BI/ad-hoc.
-- Author: Feodor Fernando

-- 1) Data source pointing at the gold container in ADLS gen2.
CREATE EXTERNAL DATA SOURCE gold_lake
WITH (LOCATION = 'abfss://gold@feodorlake.dfs.core.windows.net');

CREATE EXTERNAL FILE FORMAT parquet_format
WITH (FORMAT_TYPE = PARQUET);

-- 2) External table over the curated daily revenue (written by Databricks gold layer).
CREATE EXTERNAL TABLE dbo.revenue_daily (
    [date]      DATE,
    region      VARCHAR(40),
    segment     VARCHAR(40),
    revenue     DECIMAL(18,2),
    txn_count   BIGINT
)
WITH (
    LOCATION   = 'revenue_daily/',
    DATA_SOURCE = gold_lake,
    FILE_FORMAT = parquet_format
);

-- 3) Serverless view used directly by the Power BI model (DirectQuery-friendly).
CREATE OR ALTER VIEW dbo.vw_revenue_trend AS
SELECT
    [date],
    region,
    SUM(revenue)    AS revenue,
    SUM(txn_count)  AS txn_count
FROM dbo.revenue_daily
GROUP BY [date], region;

-- 4) Example consumption query the dashboard runs.
SELECT TOP (30) [date], region, revenue
FROM dbo.vw_revenue_trend
ORDER BY [date] DESC;

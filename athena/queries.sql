-- =====================================================================
-- Athena queries over the curated orders zone (S3 Parquet, partitioned
-- by year/month). Partition filters are always included so scans stay
-- small and cheap.
-- =====================================================================

-- 1. Monthly KPIs from curated data (mirrors the T-SQL version in
--    sql-analytics-library, for the AWS reporting path)
SELECT
    year,
    month,
    COUNT(*)                          AS order_count,
    COUNT(DISTINCT customer_id)       AS active_customers,
    SUM(gross_amount)                 AS gross_revenue,
    SUM(gross_amount) / COUNT(*)      AS avg_order_value
FROM curated.orders
WHERE year = 2026 AND month BETWEEN 1 AND 9
  AND status IN ('DELIVERED', 'SHIPPED')
GROUP BY year, month
ORDER BY year, month;

-- 2. Freshness check: did yesterday's batch land?
SELECT COUNT(*) AS rows_yesterday
FROM curated.orders
WHERE year = YEAR(current_date - interval '1' day)
  AND month = MONTH(current_date - interval '1' day)
  AND order_date = current_date - interval '1' day;

-- 3. Batch verification: published counts by batch id (compare with the
--    manifest before the BI refresh runs)
SELECT publish_batch_id, COUNT(*) AS published_rows, SUM(gross_amount) AS total_amount
FROM curated.orders
WHERE publish_batch_id = 'BATCH-20260901000000'
GROUP BY publish_batch_id;

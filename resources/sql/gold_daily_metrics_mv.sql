CREATE OR REFRESH MATERIALIZED VIEW {{gold_materialized_view_name}}
AS
SELECT
  order_date,
  COUNT(orderId) AS totalOrders,
  SUM(amount) AS totalRevenue,
  MAX(ingest_timestamp) AS source_max_ingest_timestamp,
  CURRENT_TIMESTAMP() AS gold_updated_at
FROM {{silver_table_name}}
GROUP BY order_date;

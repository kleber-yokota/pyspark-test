-- Flow validation: the silver table exists with the transformed data.
-- Runs on the SQL warehouse (serverless) — no Spark cluster.
-- Unqualified: resolves against the default catalog (dev on Databricks,
-- spark_catalog locally) and the default schema — runs as written on both.
SELECT name, name_cap, qty
FROM orders_silver
ORDER BY qty;

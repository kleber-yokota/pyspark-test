-- join task: joins names_bronze and surnames_bronze on id (inner join).
-- Names without a surname (and surnames without a name) are left out of
-- the result.
-- Bronze lives in the default catalog (dev on Databricks, spark_catalog
-- locally); silver lives in the silver catalog, people schema (both sides).
-- ``USING delta`` is the default on Databricks — explicit for the local UC.
-- Runs as written on both.
CREATE OR REPLACE TABLE silver.people.names_silver USING delta AS
SELECT n.id, n.name, s.surname
FROM names_bronze AS n
JOIN surnames_bronze AS s
ON n.id = s.id;

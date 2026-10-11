-- save task: materializes the final target of the job (Delta) in the gold
-- catalog. ``gold`` exists on both sides (UC catalog in the local Spark;
-- UC catalog on Databricks) — qualified name, no remapping.
CREATE OR REPLACE TABLE gold.people.names USING delta AS
SELECT id, name, surname
FROM silver.people.names_silver;

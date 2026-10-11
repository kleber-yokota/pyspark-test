"""Simple ETL flow, 100% PySpark — runs on Databricks (serverless).

- ``transform``: pure transformation (DataFrame -> DataFrame);
- ``run_etl``: reads the raw data from ``source`` (a CSV file on
  ``s3a://`` / ``volume://`` / ``dbfs:``) and creates the ``orders_bronze``
  (raw) and ``orders_silver`` (transformed) tables — each layer reads the
  previous one.

When the file runs as a ``spark_python`` task of a Databricks job, the
runtime provides the session (``getOrCreate``) and the source via the task
parameter (``sys.argv[1]``); the local test (``tests/test_job_bundle.py``)
calls ``main`` directly with the local source.
"""

from __future__ import annotations

import sys

import pyspark.sql.functions as F
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import IntegerType, StringType, StructField, StructType

# Raw schema (stable): do not trust CSV type inference.
ORDERS_SCHEMA = StructType(
    [StructField("name", StringType()), StructField("qty", IntegerType())]
)


def transform(df: DataFrame) -> DataFrame:
    """Trims ``name`` and adds ``name_cap`` (initcap)."""
    return df.withColumn("name", F.trim(F.col("name"))).withColumn(
        "name_cap", F.initcap(F.col("name"))
    )


def run_etl(session: SparkSession, source: str, catalog: str, schema: str) -> str:
    """Creates ``orders_bronze`` (raw, from ``source``) and ``orders_silver``.

    Bronze persists the file as it came (with spaces); silver reads the
    bronze table and applies ``transform``. Returns the full silver table
    name.
    """
    bronze = f"{catalog}.{schema}.orders_bronze"
    silver = f"{catalog}.{schema}.orders_silver"
    raw = session.read.schema(ORDERS_SCHEMA).option("header", "true").csv(source)
    raw.write.mode("overwrite").saveAsTable(bronze)
    transform(session.table(bronze)).write.mode("overwrite").saveAsTable(silver)
    return silver


def main(
    session: SparkSession,
    source: str,
    catalog: str = "dev",
    schema: str = "default",
) -> str:
    """Entrypoint for the ``spark_python`` task (and the local integration test).

    ``source`` is the raw CSV path (the task parameter in the job;
    ``s3a://...`` / ``volume://...`` / ``dbfs:...``).
    """
    return run_etl(session, source, catalog, schema)


if __name__ == "__main__":
    if len(sys.argv) < 2:
        raise SystemExit(
            "usage: python flows/etl.py <source>  "
            "(e.g. volume://dev.default.orders/orders.csv)"
        )
    main(SparkSession.builder.getOrCreate(), sys.argv[1])

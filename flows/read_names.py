"""Reads the names file (``id,name``) from the source and creates
``{catalog}.{schema}.names_bronze``.

Cleaning: trims ``name`` and drops rows with an empty name.

When the file runs as a ``spark_python`` task of a Databricks job, the
runtime provides the session (``getOrCreate``) and the source via the task
parameter (``sys.argv[1]``); the local test (``tests/test_job_bundle.py``)
calls ``main`` directly with the local source.
"""

from __future__ import annotations

import sys

import pyspark.sql.functions as F
from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType, StringType, StructField, StructType

# Raw schema (stable): do not trust CSV type inference.
NAMES_SCHEMA = StructType(
    [StructField("id", IntegerType()), StructField("name", StringType())]
)


def main(
    session: SparkSession,
    source: str,
    catalog: str = "dev",
    schema: str = "default",
) -> None:
    """Reads ``id,name`` from ``source`` (CSV with header) and creates
    ``names_bronze``."""
    names = (
        session.read.schema(NAMES_SCHEMA).option("header", "true")
        .csv(source)
        .withColumn("name", F.trim(F.col("name")))
        .filter(F.col("name").isNotNull() & (F.col("name") != ""))
    )
    # ``delta`` explicit: the default format on Databricks; the local UC
    # requires a provider for managed tables.
    names.write.format("delta").mode("overwrite").saveAsTable(
        f"{catalog}.{schema}.names_bronze"
    )


if __name__ == "__main__":
    main(SparkSession.builder.getOrCreate(), sys.argv[1])

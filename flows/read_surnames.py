"""Reads the surnames file (``id,surname``) from the source and creates
``{catalog}.{schema}.surnames_bronze``.

The ``id`` is the join key with ``names_bronze`` (the ``join`` task).

Cleaning: trims ``surname`` and drops empty rows.

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
SURNAMES_SCHEMA = StructType(
    [StructField("id", IntegerType()), StructField("surname", StringType())]
)


def main(
    session: SparkSession,
    source: str,
    catalog: str = "dev",
    schema: str = "default",
) -> None:
    """Reads ``id,surname`` from ``source`` (CSV with header) and creates
    ``surnames_bronze``."""
    surnames = (
        session.read.schema(SURNAMES_SCHEMA).option("header", "true")
        .csv(source)
        .withColumn("surname", F.trim(F.col("surname")))
        .filter(F.col("surname").isNotNull() & (F.col("surname") != ""))
    )
    # ``delta`` explicit: the default format on Databricks; the local UC
    # requires a provider for managed tables.
    surnames.write.format("delta").mode("overwrite").saveAsTable(
        f"{catalog}.{schema}.surnames_bronze"
    )


if __name__ == "__main__":
    main(SparkSession.builder.getOrCreate(), sys.argv[1])

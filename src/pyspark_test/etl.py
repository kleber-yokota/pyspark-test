"""ETL: CSV -> ``trim`` on the column -> catalog (schema ``silver``, table ``names``).

The table is EXTERNAL; ``location`` receives the Delta files.
"""

from __future__ import annotations

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

__all__ = ["save_csv_to_silver"]


def save_csv_to_silver(session: SparkSession, csv_path: str, location: str) -> None:
    """Reads the CSV (column ``name``), trims it and saves to ``silver.names``.

    The table is EXTERNAL in ``location``, in the session's default catalog
    (Unity Catalog); the ``silver`` schema is created. ``location`` receives
    the Delta files — the UC server keeps only the metadata.
    """
    df = session.read.csv(csv_path, header=True)
    df = df.withColumn("name", F.trim(F.col("name")))
    session.sql("CREATE SCHEMA silver")
    session.sql(
        f"CREATE TABLE silver.names (name STRING) USING delta LOCATION '{location}'"
    )
    df.createOrReplaceTempView("people")
    session.sql("INSERT INTO silver.names SELECT name FROM people")

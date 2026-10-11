"""PySpark text transformations (DataFrame API)."""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

__all__ = ["capitalize_names"]


def capitalize_names(df: DataFrame) -> DataFrame:
    """Creates the ``name_cap`` column with ``name`` capitalized (``initcap``).

    Uses Spark's ``initcap``: the first letter of each word goes to
    uppercase and the rest to lowercase (``"maria da silva" -> "Maria Da
    Silva"``). The original ``name`` column is kept.
    """
    return df.withColumn("name_cap", F.initcap(F.col("name")))

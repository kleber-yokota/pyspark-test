"""Unit tests for the PySpark flow (100% PySpark, no bucket/S3).

Uses the ``spark`` fixture (Spark Connect in Docker — the same
SparkSession API as Databricks). The functions tested are the same ones
the Databricks workflow executes (``flows/etl.py``), so the tests are
compatible with Databricks: they depend on no Docker, S3, Floci, or UC
local — only on a ``SparkSession``.
"""

from __future__ import annotations

from flows.etl import transform


def test_transform_trims_and_capitalizes(spark) -> None:
    df = spark.createDataFrame(
        [("  ana", 1), ("bruno ", 2), ("carla", 3)],
        schema="name: string, qty: int",
    )
    out = transform(df).collect()
    assert [r["name"] for r in out] == ["ana", "bruno", "carla"]
    assert [r["name_cap"] for r in out] == ["Ana", "Bruno", "Carla"]
    assert [r["qty"] for r in out] == [1, 2, 3]


def test_transform_keeps_other_columns(spark) -> None:
    df = spark.createDataFrame([("  ana", 1)], schema="name: string, qty: int")
    out = transform(df)
    assert set(out.columns) == {"name", "qty", "name_cap"}
    assert out.count() == 1

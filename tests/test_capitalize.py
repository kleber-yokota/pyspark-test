"""Tests: ``capitalize_names`` creates the ``name_cap`` column on a real Spark.

Spark Connect in a Docker container via **testcontainers** (the ``spark``
fixture in ``tests/conftest.py``) — requires Docker.
"""

from pyspark_test import text


def test_capitalize_creates_new_column(spark) -> None:
    df = spark.createDataFrame(
        [("maria da silva",), ("joao santos",), ("ANA LIMA",)],
        schema="name STRING",
    )
    result = text.capitalize_names(df)
    rows = result.collect()
    assert [row["name"] for row in rows] == [
        "maria da silva",
        "joao santos",
        "ANA LIMA",
    ]
    assert [row["name_cap"] for row in rows] == [
        "Maria Da Silva",
        "Joao Santos",
        "Ana Lima",
    ]


def test_capitalize_keeps_original_columns(spark) -> None:
    df = spark.createDataFrame([("maria da silva",)], schema="name STRING")
    assert text.capitalize_names(df).columns == ["name", "name_cap"]


def test_capitalize_null_name_becomes_null(spark) -> None:
    df = spark.createDataFrame([(None,)], schema="name STRING")
    assert text.capitalize_names(df).collect()[0]["name_cap"] is None

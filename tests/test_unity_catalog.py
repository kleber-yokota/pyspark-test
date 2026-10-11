"""Tests with Unity Catalog (requires Docker).

- Fixture ``uc`` (``tests/conftest.py``): a ``SparkSession`` (Spark Connect)
  with the UC connector + Delta Lake, on a dedicated Docker network with
  the UC server; the session's default catalog is ``demo``.
- The ``uc_clean`` decorator: drops the test's tables from the schema
  after each test (Django-style isolation) — even when the test fails.
- Only EXTERNAL tables (``USING delta LOCATION``): managed tables are
  experimental in UC OSS.
- The ``LOCATION``s are local paths inside the Spark container (the UC
  server keeps only the metadata); the test CSV is copied into the
  container by the ``uc`` fixture (path ``UC_CSV_DEST`` in ``infra.py``).
"""

from __future__ import annotations

import pytest
from conftest import uc_clean

from pyspark_test import etl, text

# 3-minute limit per UC test (includes the fixture setup on first use).
pytestmark = pytest.mark.timeout(180)


def test_uc_default_catalog(uc) -> None:
    row = uc.sql("SELECT current_catalog() AS c").collect()[0]
    assert row["c"] == "demo"


@uc_clean
def test_uc_delta_insert_select_roundtrip(uc) -> None:
    """EXTERNAL Delta table in UC: INSERT + SELECT (roundtrip via SQL)."""
    table = "demo.default.roundtrip"
    uc.sql(
        f"CREATE TABLE {table} (id INT, name STRING) "
        "USING delta LOCATION '/opt/spark/data/roundtrip'"
    )
    uc.sql(f"INSERT INTO {table} VALUES (1, 'maria'), (2, 'joao')")
    rows = uc.sql(f"SELECT id, name FROM {table} ORDER BY id").collect()
    assert [(r["id"], r["name"]) for r in rows] == [(1, "maria"), (2, "joao")]


@uc_clean
def test_uc_table_appears_in_catalog(uc) -> None:
    """The created table is registered in the UC catalog (visible via SQL)."""

    def tables() -> set[str]:
        rows = uc.sql("SHOW TABLES IN demo.default").collect()
        return {r["tableName"] for r in rows}

    # Isolation (Django-style): the schema is clean — the ``roundtrip``
    # table created by the previous test was dropped by ``uc_clean``.
    assert "roundtrip" not in tables()

    table = "demo.default.catalog_check"
    uc.sql(
        f"CREATE TABLE {table} (id INT) "
        "USING delta LOCATION '/opt/spark/data/catalog_check'"
    )
    uc.sql(f"INSERT INTO {table} VALUES (7)")
    assert "catalog_check" in tables()
    # ...and the Spark can read the data from it
    rows = uc.sql(f"SELECT id FROM {table}").collect()
    assert [r["id"] for r in rows] == [7]


@uc_clean
def test_uc_reads_csv_trims_and_saves_to_silver(uc) -> None:
    """The ETL reads the CSV, trims the column and saves to ``silver.names`` (UC).

    The CSV is a repo file copied into the Spark container by the ``uc``
    fixture (Spark does not see the host filesystem); the LOCATION is in
    the container — the UC server keeps only the metadata.
    """
    etl.save_csv_to_silver(
        uc,
        "/opt/spark/data/names_padded.csv",
        "/opt/spark/data/silver/names",
    )

    # The data is in the UC catalog, with the trim applied:
    rows = uc.sql("SELECT name FROM demo.silver.names ORDER BY name").collect()
    assert [r["name"] for r in rows] == [
        "ANA LIMA",
        "joao santos",
        "maria da silva",
    ]


def test_uc_schema_silver_does_not_persist(uc) -> None:
    """Neither the ``silver`` schema nor the ``names`` table persist.

    The cleanup is done by ``@uc_clean``: it drops the created tables and
    schemas; the only schema that remains is the ``default`` one from the
    bootstrap.
    """
    schemas = {r["namespace"] for r in uc.sql("SHOW NAMESPACES IN demo").collect()}
    assert schemas == {"default"}


@uc_clean
def test_uc_capitalize_over_uc_table(uc) -> None:
    table = "demo.default.names"
    uc.sql(
        f"CREATE TABLE {table} (name STRING) "
        "USING delta LOCATION '/opt/spark/data/names_uc'"
    )
    uc.sql(f"INSERT INTO {table} VALUES ('maria da silva'), ('ANA LIMA')")
    result = text.capitalize_names(uc.table(table))
    caps = sorted(r["name_cap"] for r in result.collect())
    assert caps == ["Ana Lima", "Maria Da Silva"]

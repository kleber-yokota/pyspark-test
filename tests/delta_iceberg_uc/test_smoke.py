"""Smoke for the ``spark-delta-iceberg-uc`` image (Spark 4.1 + Delta + Iceberg + UC).

Validates coexistence in the same session:
  - the Spark Connect server starts (pre-baked jars, no download);
  - the UC catalog (``demo``) answers via the connector;
  - Delta: create (LOCATION) + insert + select in the UC catalog;
  - Iceberg: create + insert + select in the hadoop catalog.
"""

import pytest

pytestmark = pytest.mark.timeout(180)


def test_spark_starts(delta_iceberg_uc) -> None:
    assert delta_iceberg_uc.version == "4.1.2"
    assert delta_iceberg_uc.sql("SELECT current_catalog()").collect()[0][0] == "demo"


def test_uc_reachable_via_connector(delta_iceberg_uc) -> None:
    namespaces = [
        r["namespace"]
        for r in delta_iceberg_uc.sql("SHOW NAMESPACES IN demo").collect()
    ]
    assert "default" in namespaces


def test_delta_in_uc_catalog(delta_iceberg_uc) -> None:
    session = delta_iceberg_uc
    session.sql(
        "CREATE TABLE demo.default.smoke_delta (id INT, name STRING) "
        "USING delta LOCATION '/tmp/smoke_delta'"
    )
    try:
        session.sql("INSERT INTO demo.default.smoke_delta VALUES (1, 'a')")
        rows = [
            (r["id"], r["name"])
            for r in session.sql(
                "SELECT id, name FROM demo.default.smoke_delta ORDER BY id"
            ).collect()
        ]
        assert rows == [(1, "a")]
    finally:
        session.sql("DROP TABLE IF EXISTS demo.default.smoke_delta")


def test_iceberg_in_same_session(delta_iceberg_uc) -> None:
    session = delta_iceberg_uc
    session.sql(
        "CREATE TABLE iceberg.main.smoke_ib (id INT, name STRING) USING iceberg"
    )
    try:
        session.sql("INSERT INTO iceberg.main.smoke_ib VALUES (1, 'x')")
        rows = [
            (r["id"], r["name"])
            for r in session.sql("SELECT id, name FROM iceberg.main.smoke_ib").collect()
        ]
        assert rows == [(1, "x")]
    finally:
        session.sql("DROP TABLE IF EXISTS iceberg.main.smoke_ib")

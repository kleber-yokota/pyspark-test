"""Smoke for the ``spark-iceberg-uc`` image (Spark 4.1 + Iceberg + UC).

Validates the minimum for the image to be ready:
  - the Spark Connect server starts (pre-baked jars, no download);
  - the UC catalog (``demo``) answers via the connector;
  - Iceberg: create + insert + select in the hadoop catalog (``iceberg``).
"""

import pytest

pytestmark = pytest.mark.timeout(180)


def test_spark_starts(iceberg_uc) -> None:
    assert iceberg_uc.version == "4.1.2"
    assert iceberg_uc.sql("SELECT current_catalog()").collect()[0][0] == "demo"


def test_uc_reachable_via_connector(iceberg_uc) -> None:
    namespaces = [
        r["namespace"] for r in iceberg_uc.sql("SHOW NAMESPACES IN demo").collect()
    ]
    assert "default" in namespaces


def test_iceberg_insert_select(iceberg_uc) -> None:
    session = iceberg_uc
    session.sql("CREATE TABLE iceberg.main.smoke (id INT, name STRING) USING iceberg")
    try:
        session.sql("INSERT INTO iceberg.main.smoke VALUES (1, 'a'), (2, 'b')")
        rows = [
            (r["id"], r["name"])
            for r in session.sql(
                "SELECT id, name FROM iceberg.main.smoke ORDER BY id"
            ).collect()
        ]
        assert rows == [(1, "a"), (2, "b")]
    finally:
        session.sql("DROP TABLE IF EXISTS iceberg.main.smoke")

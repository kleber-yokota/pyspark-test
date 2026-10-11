"""SDP pipeline (Spark Declarative Pipelines / ``spark-pipelines``): sales.

The pipeline project lives in ``pipelines/sales`` (committed spec
``spark-pipeline.yml`` + ``transformations/``); the test uses the committed
spec as-is (rewriting only ``configuration.pipeline.sources.path``, the
parquet prefix in the bucket) and runs ``pyspark.pipelines.cli.run`` against
the fixture ``pipeline_env`` server (Spark Connect + Unity Catalog — the
same emulation as the ``names`` job).

Flow: parquet in S3 (product_id/price, store_id/product_id/quantity,
store_id/address) -> clean (cast/trim/region extraction) -> data quality
(not null, unique, range, values) -> two tables:

- ``gold.sales.sales_by_region`` (region, quantity);
- ``gold.sales.sales_by_store`` (store_id, quantity, amount).

``clean``: the pipeline delivers both tables;
``dirty``: data quality fails the run before any flow.
"""

from __future__ import annotations

import os
from copy import deepcopy
from pathlib import Path

import pyspark.pipelines.cli as sdp_cli
import pytest
import yaml
from infra import S3A_BUCKET
from pyspark.sql import SparkSession
from pyspark.sql.connect.session import SparkSession as ConnectSparkSession

PIPELINE_DIR = Path(__file__).resolve().parent.parent / "pipelines" / "sales"
SPEC_FILE = PIPELINE_DIR / "spark-pipeline.yml"


def _connect(pipeline_env: str) -> SparkSession:
    return SparkSession.builder.remote(pipeline_env).create()


def _run(source: str, pipeline_env: str) -> None:
    """Runs the pipeline CLI: full refresh, no dry-run.

    The CLI (``getOrCreate``) reuses the process's active session and
    stops it at the end — so the session is created here and forced as the
    active one (the ``SPARK_REMOTE`` env var is what the
    ``spark-pipelines run --remote <endpoint>`` launcher injects; a fixture
    session from another test must not leak into the run).
    """
    session = ConnectSparkSession.builder.remote(pipeline_env).create()
    # The CLI's ``getOrCreate`` takes the active session (thread-local) —
    # force it here (and clean it up afterwards): a fixture session from
    # another test must not leak into the run.
    ConnectSparkSession._active_session.session = session
    os.environ["SPARK_REMOTE"] = pipeline_env
    spec = _write_spec(source)
    try:
        sdp_cli.run(spec, False, False, None, False)
    finally:
        os.environ.pop("SPARK_REMOTE", None)
        ConnectSparkSession._active_session.session = None
        if spec != SPEC_FILE:
            spec.unlink(missing_ok=True)


def _write_spec(source: str) -> Path:
    """Returns the spec to run, based on the project's committed spec.

    The committed ``spark-pipeline.yml`` is the source of truth: if the
    sources prefix already matches ``source``, it is used as-is; otherwise,
    a sibling spec is written (the ``transformations/**`` glob is relative
    to the project directory) with only
    ``configuration.pipeline.sources.path`` changed. The sibling is removed
    by the caller.
    """
    spec = yaml.safe_load(SPEC_FILE.read_text(encoding="utf-8"))
    if spec["configuration"]["pipeline.sources.path"] == source:
        return SPEC_FILE
    spec["configuration"]["pipeline.sources.path"] = source
    sibling = SPEC_FILE.with_name("spark-pipeline.run.yml")
    sibling.write_text(yaml.safe_dump(spec, sort_keys=False), encoding="utf-8")
    return sibling


def _seed(session: SparkSession, prefix: str, dirty: bool) -> None:
    """Seeds the three parquets into the bucket (one dataset per directory)."""
    if dirty:
        # Violations: unique (P1, S1 duplicated), not_null (price of P2),
        # range (negative price, zero quantity) and values (P99/S99 do not
        # exist).
        products = [("P1", "9.90"), ("P1", "9.90"), ("P2", None), ("P3", "-1.00")]
        stores = [
            ("S1", "123 Main St - South"),
            ("S1", "123 Main St - South"),
            ("S3", "789 Third St - Northeast"),
        ]
        sales = [
            ("S1", "P1", "2"),
            ("S2", "P99", "5"),
            ("S99", "P2", "3"),
            ("S1", "P2", "0"),
        ]
    else:
        products = [("P1", "9.90"), ("P2", "19.90"), ("P3", "5.00")]
        stores = [
            ("S1", "123 Main St - South"),
            ("S2", "456 Side St - Southeast"),
            ("S3", "789 Third St - Northeast"),
        ]
        sales = [
            ("S1", "P1", "2"),
            ("S1", "P2", "1"),
            ("S2", "P1", "5"),
            ("S2", "P3", "4"),
            ("S3", "P2", "7"),
            ("S3", "P1", "3"),
        ]
    datasets = {
        "products": (products, "product_id string, price string"),
        "stores": (stores, "store_id string, address string"),
        "sales": (sales, "store_id string, product_id string, quantity string"),
    }
    for name, (rows, schema) in datasets.items():
        session.createDataFrame(rows, schema=schema).write.mode(
            "overwrite"
        ).parquet(f"s3a://{S3A_BUCKET}/pipeline/{prefix}/{name}")


def test_spec_keeps_the_committed_spec() -> None:
    """The run uses the committed spec; the sources prefix is the only diff."""
    original = SPEC_FILE.read_text(encoding="utf-8")
    before = yaml.safe_load(original)
    spec = _write_spec("s3a://pyspark-test/pipeline/dirty")
    try:
        assert SPEC_FILE.read_text(encoding="utf-8") == original
        expected = deepcopy(before)
        expected["configuration"]["pipeline.sources.path"] = (
            "s3a://pyspark-test/pipeline/dirty"
        )
        assert yaml.safe_load(spec.read_text(encoding="utf-8")) == expected
    finally:
        if spec != SPEC_FILE:
            spec.unlink(missing_ok=True)


def test_pipeline_delivers_both_tables(pipeline_env: str) -> None:
    session = _connect(pipeline_env)
    _seed(session, "clean", dirty=False)
    session.stop()

    _run(f"s3a://{S3A_BUCKET}/pipeline/clean", pipeline_env)

    session2 = _connect(pipeline_env)
    try:
        region = [
            (r.region, r.quantity)
            for r in session2.sql(
                "SELECT region, quantity "
                "FROM gold.sales.sales_by_region "
                "ORDER BY region"
            ).collect()
        ]
        assert region == [("Northeast", 10), ("South", 3), ("Southeast", 9)]

        store = [
            (r.store_id, r.quantity, r.amount)
            for r in session2.sql(
                "SELECT store_id, quantity, amount "
                "FROM gold.sales.sales_by_store "
                "ORDER BY store_id"
            ).collect()
        ]
        assert store == [
            ("S1", 3, pytest.approx(39.70)),
            ("S2", 9, pytest.approx(69.50)),
            ("S3", 10, pytest.approx(169.00)),
        ]
    finally:
        session2.stop()


def test_pipeline_fails_with_dirty_data(pipeline_env: str) -> None:
    session = _connect(pipeline_env)
    _seed(session, "dirty", dirty=True)
    session.stop()

    with pytest.raises(Exception, match="data quality check failed") as excinfo:
        _run(f"s3a://{S3A_BUCKET}/pipeline/dirty", pipeline_env)

    # The message lists the four check types violated:
    # not_null, unique, range and values.
    message = str(excinfo.value)
    for check in ("not_null", "unique", "range", "values"):
        assert check in message

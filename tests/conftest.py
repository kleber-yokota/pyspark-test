"""Fixtures: ``spark`` (real Spark) and ``s3_client`` (Floci emulating S3).

The Docker infra (testcontainers: containers, network, UC bootstrap)
lives in ``infra.py``.
"""

from __future__ import annotations

import functools

import boto3
import pytest
from floci import FlociContainer
from infra import (
    FLOCI_ALIAS,
    FLOCI_IMAGE,
    S3A_BUCKET,
    UC_CATALOG,
    UC_SCHEMA,
    delta_iceberg_session,
    gold_session,
    iceberg_session,
    pipeline_server,
    s3a_session,
    start_spark,
    uc_session,
)
from testcontainers.core.network import Network


@pytest.fixture(scope="session")
def spark():
    """``SparkSession`` (Spark Connect in a Docker container, via testcontainers)."""
    session, container = start_spark()
    try:
        yield session
    finally:
        session.stop()
        container.stop()


@pytest.fixture(scope="session")
def floci_network():
    """Docker network shared by Floci (S3) and Spark (S3A), once per session."""
    net = Network().create()
    try:
        yield net
    finally:
        net.remove()


@pytest.fixture(scope="session")
def floci(floci_network):
    """Floci container (emulated S3) via testcontainers, once per session.

    On the shared network so that the Spark container reaches it by the
    alias ``floci`` (containers do not see the host's ``127.0.0.1``).
    """
    with FlociContainer(image=FLOCI_IMAGE) \
            .with_network(floci_network) \
            .with_network_aliases(FLOCI_ALIAS) as container:
        yield container


@pytest.fixture(scope="session")
def s3_client(floci):
    """boto3 client (``s3`` service) pointed at Floci."""
    return boto3.client(
        "s3",
        endpoint_url=floci.get_endpoint(),
        region_name=floci.get_region(),
        aws_access_key_id=floci.get_access_key(),
        aws_secret_access_key=floci.get_secret_key(),
        config=boto3.session.Config(s3={"addressing_style": "path"}),
    )


@pytest.fixture(scope="session")
def spark_s3a(floci, floci_network):
    """``SparkSession`` (Spark Connect) with S3A — reads ``s3a://`` directly from Floci.

    The image reuses ``spark-uc`` (``hadoop-aws`` is already in the Ivy
    cache — no download). Once per session.
    """
    yield from s3a_session(floci, floci_network)


@pytest.fixture(scope="session")
def spark_gold(s3_bucket, floci, floci_network):
    """``SparkSession`` S3A + ``gold`` catalog (Unity Catalog, Delta on S3).

    The ``names`` job writes ``silver.people.names_silver`` and
    ``gold.people.names`` — on Databricks, ``silver`` and ``gold`` are UC
    catalogs. The fixture starts the UC server on the Floci network and
    creates ``spark_catalog``, ``silver`` and ``gold`` via REST
    (``storage_root`` in the bucket — depends on ``s3_bucket``). Once per
    session.
    """
    yield from gold_session(floci, floci_network)


@pytest.fixture(scope="session")
def pipeline_env(s3_bucket, floci, floci_network) -> str:
    """Connect endpoint of the SDP pipeline server (spark-pipelines).

    The ``sales`` pipeline (project in ``pipelines/sales``) reads parquet
    from the Floci bucket and materializes the final tables in the
    ``gold`` catalog (Unity Catalog, ``sales`` schema) — like on
    Databricks. The fixture starts the UC server on the Floci network and
    creates ``gold`` + ``sales`` via REST (``storage_root`` in the bucket —
    depends on ``s3_bucket``). Once per session; the consumers create and
    stop their own sessions (the pipeline CLI stops the session it
    reuses).
    """
    yield from pipeline_server(floci, floci_network)


@pytest.fixture(scope="session")
def s3_bucket(s3_client) -> str:
    """Bucket created once per session (Floci starts empty on every run)."""
    s3_client.create_bucket(Bucket=S3A_BUCKET)
    yield S3A_BUCKET


@pytest.fixture(scope="session")
def uc():
    """Unity Catalog: UC server + Spark Connect (UC + Delta), once per session.

    A dedicated Docker network: the UC server (the ``demo`` catalog and the
    ``default`` schema are created via the REST API) and the Spark Connect
    server (pre-baked jars), with the UC connector registered as
    ``spark.sql.catalog.demo`` and Delta as ``spark_catalog`` (a Delta 4.4.x
    requirement). The session already has ``demo`` as the default catalog.
    Yields a ``SparkSession``.
    """
    yield from uc_session()


@pytest.fixture(scope="session")
def iceberg_uc():
    """Spark 4.1 + Iceberg + UC (no Delta), once per session.

    Catalogs: ``demo`` (UC) and ``iceberg`` (hadoop, warehouse inside the
    container); ``defaultCatalog=demo``. Yields a ``SparkSession``.
    """
    yield from iceberg_session()


@pytest.fixture(scope="session")
def delta_iceberg_uc():
    """Spark 4.1 + Delta + Iceberg + UC (coexistence), once per session.

    Catalogs: ``spark_catalog`` (DeltaCatalog — a Delta 4.4.x requirement),
    ``demo`` (UC) and ``iceberg`` (hadoop); ``defaultCatalog=demo``.
    Yields a ``SparkSession``.
    """
    yield from delta_iceberg_session()


def _session_catalogs(session) -> list[str]:
    """Catalogs registered in the session: ``spark_catalog`` + ``spark.sql.catalog.*``.

    ``SHOW CATALOGS`` only returns ``spark_catalog`` and does not list the
    catalogs registered via conf (e.g. ``demo``), so the session conf is the
    source of truth for discovery.
    """
    prefix = "spark.sql.catalog."
    catalogs = {"spark_catalog"}
    for key in session.conf.getAll:
        name = key.removeprefix(prefix)
        if key.startswith(prefix) and "." not in name:
            catalogs.add(name)
    return sorted(catalogs)


def _cleanup_catalogs(session) -> None:
    """Dynamically clean ALL catalogs in the session: drop every table and
    every schema created by the tests (e.g. ``demo.silver``).

    Only these remain: ``demo.default`` (the fixture bootstrap) and
    ``spark_catalog.default`` (Spark does not allow dropping the default
    database); the tables inside them are dropped too. Catalogs without
    namespace enumeration support (e.g. a hadoop catalog) are skipped: they
    hold no persistent metadata (the container is ephemeral).
    """
    keep = {(UC_CATALOG, UC_SCHEMA), ("spark_catalog", "default")}
    for catalog in _session_catalogs(session):
        try:
            namespaces = [
                r["namespace"]
                for r in session.sql(f"SHOW NAMESPACES IN {catalog}").collect()
            ]
        except Exception:  # noqa: BLE001 - e.g. a hadoop catalog
            continue
        for namespace in namespaces:
            for row in session.sql(f"SHOW TABLES IN {catalog}.{namespace}").collect():
                session.sql(
                    f"DROP TABLE IF EXISTS {catalog}.{namespace}.{row['tableName']}"
                )
            if (catalog, namespace) not in keep:
                session.sql(f"DROP SCHEMA {catalog}.{namespace} CASCADE")


def uc_clean(func):
    """Decorator: guarantees cleanup of the UC catalogs after the test, even on failure.

    The "database" (``demo`` catalog + ``default`` schema, started once in
    the ``uc`` fixture) stays; the tables and schemas created by the test
    are dropped across all catalogs of the session. The ``try/finally``
    guarantees cleanup even when the test fails or raises.

    Usage::

        @uc_clean
        def test_x(uc) -> None: ...
    """

    @functools.wraps(func)
    def wrapper(uc, *args, **kwargs):
        try:
            return func(uc, *args, **kwargs)
        finally:
            _cleanup_catalogs(uc)

    return wrapper

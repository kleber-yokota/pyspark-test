"""Docker infra (testcontainers) for the test suite.

Everything that starts containers, used by the fixtures in ``conftest.py``:
- ``start_spark`` — the base Spark Connect server (``SPARK_IMAGE``),
- ``s3a_session`` — Spark Connect (S3A) on the Floci network,
- ``uc_session`` / ``gold_session`` / ``pipeline_server`` — a network +
  Unity Catalog server + Spark Connect server (pre-baked jar images),
  with the catalogs bootstrapped via the REST API.
"""

from __future__ import annotations

import contextlib
import json
import os
import socket
import time
import urllib.request
from pathlib import Path

from floci import FlociContainer
from pyspark.sql import SparkSession
from testcontainers.core.container import DockerContainer
from testcontainers.core.network import Network
from testcontainers.core.wait_strategies import HttpWaitStrategy, WaitStrategy

SPARK_IMAGE = os.environ.get("SPARK_IMAGE", "apache/spark:4.2.0-python3")
CONNECT_PORT = 15002
SPARK_STARTUP_TIMEOUT_S = int(os.environ.get("SPARK_STARTUP_TIMEOUT_S", "60"))

# Floci: local AWS (S3 and other services) emulator — https://floci.io
FLOCI_IMAGE = os.environ.get("FLOCI_IMAGE", "floci/floci:2.2.0")


class _FailFastPortWait(WaitStrategy):
    """Port wait strategy that fails fast if the container dies at startup.

    The native ``PortWaitStrategy`` only checks the port: if the container
    process dies (e.g. ``--packages`` with a nonexistent artifact), the wait
    only fails at the timeout. Here the container status is also checked on
    every poll.
    """

    def __init__(self, port: int) -> None:
        super().__init__()
        self._port = port

    def wait_until_ready(self, container) -> None:
        host = container.get_container_host_ip()
        port = int(container.get_exposed_port(self._port))
        start = time.monotonic()
        while True:
            if time.monotonic() - start > self._startup_timeout:
                raise TimeoutError(
                    f"Port {self._port} did not open within {self._startup_timeout}s"
                )
            container.reload()
            if container.status not in ("running", "created"):
                log = container.get_logs()[1][-800:].decode(errors="replace")
                raise RuntimeError(
                    f"Container died at startup (status={container.status}):\n{log}"
                )
            try:
                with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                    s.settimeout(1)
                    s.connect((host, port))
                return
            except OSError:
                time.sleep(self._poll_interval)


# --- Unity Catalog (Docker: UC server + Spark Connect on a dedicated network) ---
UC_IMAGE = os.environ.get("UC_IMAGE", "unitycatalog/unitycatalog:v0.6.0")
# Image with pre-baked Delta + UC + S3A (hadoop-aws) jars (see Dockerfile.uc):
UC_SPARK_IMAGE = os.environ.get("UC_SPARK_IMAGE", "spark-uc:0.3.0")
UC_REST_PORT = 8080
UC_CATALOG = "demo"
UC_SCHEMA = "default"
# Pinned. Delta for Spark 4.2 uses the SUFFIXED artifact ``delta-spark_4.2_2.13``
# (``delta-spark_2.13:4.3.1`` breaks at runtime on Spark 4.2.0).
# ``hadoop-aws`` enables ``s3a://`` (S3A) — the official Spark image does not
# include it; the version matches the Hadoop bundled with Spark (3.5.0 in 4.2).
DELTA_PKG_42 = "io.delta:delta-spark_4.2_2.13:4.4.1"
UC_PACKAGES = (
    f"{DELTA_PKG_42},"
    "io.unitycatalog:unitycatalog-spark_4.2_2.13:0.6.0,"
    "org.apache.hadoop:hadoop-aws:3.5.0"
)
UC_STARTUP_TIMEOUT_S = int(os.environ.get("UC_STARTUP_TIMEOUT_S", "180"))

# Spark 4.1 images (Iceberg does not yet publish a runtime for Spark 4.2 —
# see spark.md). Jars are pre-baked; at runtime Ivy is a cache hit.
ICEBERG_UC_SPARK_IMAGE = os.environ.get(
    "ICEBERG_UC_SPARK_IMAGE", "spark-iceberg-uc:0.2.0"
)
DELTA_ICEBERG_UC_SPARK_IMAGE = os.environ.get(
    "DELTA_ICEBERG_UC_SPARK_IMAGE", "spark-delta-iceberg-uc:0.2.0"
)
# Spark 4.1 bundles Hadoop 3.4.2 — hadoop-aws at the same version (S3A).
UC_PKGS_41 = (
    "io.unitycatalog:unitycatalog-spark_4.1_2.13:0.6.0,"
    "org.apache.hadoop:hadoop-aws:3.4.2"
)
ICEBERG_PKG_41 = "org.apache.iceberg:iceberg-spark-runtime-4.1_2.13:1.12.0"
DELTA_PKG_41 = "io.delta:delta-spark_4.1_2.13:4.4.0"


def start_spark() -> tuple[SparkSession, DockerContainer]:
    """Start the Spark Connect container and return ``(session, container)``.

    The session is created with ``create`` (not ``getOrCreate``): the suite
    also creates the ``uc`` fixture session; ``getOrCreate`` would return the
    previous global one.
    """
    container = (
        DockerContainer(
            image=SPARK_IMAGE,
            # ``--wait``: without it the script daemonizes the server and the
            # container dies right after (exit 0); with it the server runs in
            # the foreground.
            command="/opt/spark/sbin/start-connect-server.sh --wait",
            ports=[CONNECT_PORT],
        ).waiting_for(
            _FailFastPortWait(CONNECT_PORT).with_startup_timeout(SPARK_STARTUP_TIMEOUT_S)
        )
    )
    container.start()
    session = SparkSession.builder.remote(
        f"sc://127.0.0.1:{container.get_exposed_port(CONNECT_PORT)}"
    ).create()
    return session, container


def _rest(uc_port: int, path: str, body: dict) -> None:
    """POST to the UC REST API (the ``bin/uc`` CLI of the image is broken)."""
    req = urllib.request.Request(
        f"http://127.0.0.1:{uc_port}/{path}",
        data=json.dumps(body).encode(),
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    urllib.request.urlopen(req, timeout=30).read()


def _bootstrap_catalogs(uc_port: int, catalogs: list[tuple[str, str, str]]) -> None:
    """Bootstrap: catalog + schema via REST (same flow as the UC ``uc`` CLI)."""
    for name, storage_root, schema in catalogs:
        _rest(
            uc_port,
            "api/2.1/unity-catalog/catalogs",
            {"name": name, "storage_root": storage_root},
        )
        _rest(
            uc_port,
            "api/2.1/unity-catalog/schemas",
            {"catalog_name": name, "name": schema},
        )


def _uc_server(
    network: Network, uc_alias: str, s3_env: dict[str, str] | None = None
) -> DockerContainer:
    """UC server container on ``network`` (alias ``uc_alias``), unstarted.

    Port 8080 accepts TCP before the REST API is ready (initial requests
    are reset): the wait probes the real API. ``s3_env`` passes the UC
    server's per-bucket S3 config as env vars (see ``_s3_env``).
    """
    server = DockerContainer(image=UC_IMAGE)
    server = (
        server.with_network(network)
        .with_network_aliases(uc_alias)
        .with_exposed_ports(UC_REST_PORT)
        .waiting_for(
            HttpWaitStrategy(
                UC_REST_PORT,
                path="/api/2.1/unity-catalog/catalogs",
            ).with_startup_timeout(SPARK_STARTUP_TIMEOUT_S)
        )
    )
    for key, value in (s3_env or {}).items():
        server = server.with_env(key, value)
    return server


def _spark_connect_server(
    spark_image: str,
    packages: str,
    confs: dict[str, str],
    network: Network,
    files: list[tuple[Path, str]] | None = None,
) -> DockerContainer:
    """Spark Connect server container on ``network`` (unstarted).

    ``--packages`` hits the pre-baked image (Ivy cache hit, no download);
    ``files`` are repo paths copied into the container
    (``(source, destination)``) — the Spark JVM does not see the host
    filesystem, so test data must be copied in.
    """
    command = " ".join(
        [
            "/opt/spark/sbin/start-connect-server.sh",
            "--wait",
            "--packages",
            packages,
        ]
        + [f"--conf {k}={v}" for k, v in confs.items()]
    )
    container = DockerContainer(
        image=spark_image, command=command, ports=[CONNECT_PORT]
    ).with_network(network)
    for source, destination in files or []:
        container = container.with_copy_into_container(source, destination)
    return container.waiting_for(
        _FailFastPortWait(CONNECT_PORT).with_startup_timeout(UC_STARTUP_TIMEOUT_S)
    )


def _uc_spark_session(
    spark_image: str,
    packages: str,
    confs: dict[str, str],
    files: list[tuple[Path, str]] | None = None,
    network: Network | None = None,
    catalogs: list[tuple[str, str, str]] | None = None,
    s3_env: dict[str, str] | None = None,
):
    """Start a network + UC + Spark Connect server and yield the session.

    The UC server is started first and the ``catalogs`` bootstrapped via
    REST (default: ``demo``); then the Spark Connect server (``spark_image``
    with pre-baked jars — ``--packages`` is a cache hit in Ivy). The
    ``ExitStack`` tears down everything that was created, in reverse order
    — nothing leaks if the setup fails partway. ``network`` reuses an
    external network (not started here); ``s3_env`` is the UC server's
    per-bucket S3 config (``s3.bucketPath.0`` et al.).
    """
    with contextlib.ExitStack() as stack:
        net = stack.enter_context(Network()) if network is None else network
        uc_server = stack.enter_context(_uc_server(net, UC_ALIAS, s3_env))
        _bootstrap_catalogs(
            uc_server.get_exposed_port(UC_REST_PORT),
            catalogs
            or [
                (UC_CATALOG, f"/home/unitycatalog/etc/data/{UC_CATALOG}", UC_SCHEMA)
            ],
        )
        spark_c = stack.enter_context(
            _spark_connect_server(spark_image, packages, confs, net, files)
        )
        session = SparkSession.builder.remote(
            f"sc://127.0.0.1:{spark_c.get_exposed_port(CONNECT_PORT)}"
        ).create()
        stack.callback(session.stop)

        yield session


def _uc_demo_confs() -> dict[str, str]:
    """Confs for the UC ``demo`` catalog (connector) + session default catalog."""
    return {
        f"spark.sql.catalog.{UC_CATALOG}": "io.unitycatalog.spark.UCSingleCatalog",
        f"spark.sql.catalog.{UC_CATALOG}.uri": f"http://uc:{UC_REST_PORT}",
        f"spark.sql.catalog.{UC_CATALOG}.type": "static",
        f"spark.sql.catalog.{UC_CATALOG}.token": "",
        "spark.sql.defaultCatalog": UC_CATALOG,
    }


def _iceberg_catalog_confs() -> dict[str, str]:
    """Iceberg (hadoop) catalog with the warehouse inside the container."""
    return {
        "spark.sql.catalog.iceberg": "org.apache.iceberg.spark.SparkCatalog",
        "spark.sql.catalog.iceberg.type": "hadoop",
        "spark.sql.catalog.iceberg.warehouse": "/opt/spark/data/iceberg_warehouse",
    }


# Test CSV from the repo, copied into the Spark container: the ETL reads
# the path via Spark, which does not see the host filesystem.
NAMES_PADDED_CSV = Path(__file__).resolve().parent / "data" / "names_padded.csv"
UC_CSV_DEST = "/opt/spark/data/names_padded.csv"


def uc_session():
    """Unity Catalog env (Spark 4.2 + UC + Delta, pre-baked image). Yields a session.

    Catalogs: ``spark_catalog`` (DeltaCatalog — a Delta 4.4.x requirement)
    and ``demo`` (UC); ``defaultCatalog=demo``. The ``names_padded.csv``
    CSV is copied into the container (path ``UC_CSV_DEST``).
    """
    confs = {
        "spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension",
        # Delta 4.4.x: the session needs a DeltaCatalog registered.
        "spark.sql.catalog.spark_catalog": (
            "org.apache.spark.sql.delta.catalog.DeltaCatalog"
        ),
        **_uc_demo_confs(),
    }
    yield from _uc_spark_session(
        UC_SPARK_IMAGE,
        UC_PACKAGES,
        confs,
        files=[(NAMES_PADDED_CSV, UC_CSV_DEST)],
    )


def iceberg_session():
    """Spark 4.1 + Iceberg + UC (no Delta) env. Yields a ``SparkSession``.

    Catalogs: ``demo`` (UC) and ``iceberg`` (hadoop, warehouse inside the
    container); ``defaultCatalog=demo``.
    """
    confs = {
        "spark.sql.extensions": (
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions"
        ),
        **_iceberg_catalog_confs(),
        **_uc_demo_confs(),
    }
    yield from _uc_spark_session(
        ICEBERG_UC_SPARK_IMAGE,
        f"{ICEBERG_PKG_41},{UC_PKGS_41}",
        confs,
    )


# --- Object storage (S3A): Spark reads s3a:// directly from Floci ---
# Reuses the ``spark-uc`` image: ``hadoop-aws`` is already in the Ivy
# cache (cache hit — no download).
S3A_SPARK_IMAGE = os.environ.get("S3A_SPARK_IMAGE", UC_SPARK_IMAGE)
S3A_PACKAGES = "org.apache.hadoop:hadoop-aws:3.5.0"
FLOCI_ALIAS = "floci"
# Fixed name: Floci starts empty on every run (no cross-run collisions).
S3A_BUCKET = "pyspark-test"
UC_ALIAS = "uc"


def _floci_fs_confs(floci: FlociContainer, scheme: str) -> dict[str, str]:
    """Hadoop confs for Floci in a scheme (``s3a`` or ``s3``).

    Hadoop confs need the ``spark.hadoop.`` prefix to reach the
    ``Configuration`` used in file reads; the endpoint uses the network
    alias (the Spark container does not see the host's ``127.0.0.1``).
    """
    return {
        f"spark.hadoop.fs.{scheme}.endpoint": (
            f"http://{FLOCI_ALIAS}:{FlociContainer.PORT}"
        ),
        f"spark.hadoop.fs.{scheme}.access.key": floci.get_access_key(),
        f"spark.hadoop.fs.{scheme}.secret.key": floci.get_secret_key(),
        f"spark.hadoop.fs.{scheme}.path.style.access": "true",
    }


def _s3_env(floci: FlociContainer) -> dict[str, str]:
    """UC server per-bucket S3 config (env vars).

    The server vends static credentials for the Floci bucket (managed
    tables on ``s3://`` locations). The dummy session token selects the
    static (test-only) credential path — no real STS is involved.
    """
    return {
        "s3.bucketPath.0": f"s3://{S3A_BUCKET}",
        "s3.region.0": floci.get_region(),
        "s3.accessKey.0": floci.get_access_key(),
        "s3.secretKey.0": floci.get_secret_key(),
        "s3.sessionToken.0": "local-test",
    }


def _uc_catalog_confs(
    floci: FlociContainer,
    catalogs: list[tuple[str, str, str]],
    uc_alias: str,
) -> dict[str, str]:
    """Confs: S3A/S3 (Floci) + Delta extension + ``UCSingleCatalog`` per catalog.

    The UC ``storage_root`` uses the ``s3`` scheme, which Spark resolves
    via S3A (``fs.s3.impl``).
    """
    confs = {
        **_floci_fs_confs(floci, "s3a"),
        **_floci_fs_confs(floci, "s3"),
        "spark.hadoop.fs.s3.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
        "spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension",
    }
    for catalog, _storage, _schema in catalogs:
        confs[f"spark.sql.catalog.{catalog}"] = (
            "io.unitycatalog.spark.UCSingleCatalog"
        )
        confs[f"spark.sql.catalog.{catalog}.uri"] = f"http://{uc_alias}:{UC_REST_PORT}"
        confs[f"spark.sql.catalog.{catalog}.type"] = "static"
        confs[f"spark.sql.catalog.{catalog}.token"] = ""
    return confs


def s3a_session(floci: FlociContainer, network: Network):
    """Spark Connect (S3A) on the Floci network. Yields a ``SparkSession``.

    The S3A endpoint points to Floci via the network alias (the Spark
    container does not see the host's ``127.0.0.1``).
    """
    confs = _floci_fs_confs(floci, "s3a")
    with contextlib.ExitStack() as stack:
        spark_c = stack.enter_context(
            _spark_connect_server(S3A_SPARK_IMAGE, S3A_PACKAGES, confs, network)
        )
        session = SparkSession.builder.remote(
            f"sc://127.0.0.1:{spark_c.get_exposed_port(CONNECT_PORT)}"
        ).create()
        stack.callback(session.stop)

        yield session


# Catalogs of the ``names`` job (UC): bronze in ``default``, silver and
# gold with the ``people`` schema — storage in the Floci bucket.
GOLD_CATALOGS = [
    ("spark_catalog", f"s3://{S3A_BUCKET}/spark-warehouse", "default"),
    ("silver", f"s3://{S3A_BUCKET}/silver-warehouse", "people"),
    ("gold", f"s3://{S3A_BUCKET}/gold-warehouse", "people"),
]


def gold_session(floci: FlociContainer, network: Network):
    """Spark Connect (S3A) + Unity Catalog: the ``gold`` catalog (Delta on S3).

    The ``names`` job writes ``gold.people.names`` — on Databricks, ``gold``
    is a UC catalog. Emulation: a UC server on the Floci network (alias
    ``uc``) with ``gold`` + ``people`` created via REST (``storage_root`` in
    the Floci bucket — the UC server v0.6.0 does not touch the local
    filesystem for a cloud scheme) and Spark with the ``UCSingleCatalog``
    connector.

    On Databricks the default catalog (``dev``) and the ``silver`` and
    ``gold`` catalogs are UC — here they are all UC catalogs on the same
    server: ``spark_catalog`` (bronze — the default InMemory does not
    support the CTAS of the ``join`` task), ``silver`` (the job's
    intermediate) and ``gold`` (the job's target), with ``storage_root`` in
    the Floci bucket.
    """
    yield from _uc_spark_session(
        S3A_SPARK_IMAGE,
        UC_PACKAGES,
        _uc_catalog_confs(floci, GOLD_CATALOGS, UC_ALIAS),
        network=network,
        catalogs=GOLD_CATALOGS,
        s3_env=_s3_env(floci),
    )


# Catalogs of the ``sales`` pipeline (UC). ``spark_catalog`` (default) is
# required by the Delta 4.4 configuration check
# (``checkRequiredConfigurations``) — even without use.
SALES_CATALOGS = [
    ("spark_catalog", f"s3://{S3A_BUCKET}/pipeline-warehouse", "default"),
    ("gold", f"s3://{S3A_BUCKET}/sales-gold-warehouse", "sales"),
]


def pipeline_server(floci: FlociContainer, network: Network):
    """Spark Connect server (S3A + UC) for the SDP pipeline (spark-pipelines).

    Same emulation as ``gold_session`` (a UC server on the Floci network,
    ``gold`` catalog + ``sales`` schema created via REST with
    ``storage_root`` in the bucket, ``UCSingleCatalog`` connector + Delta
    extension), but it yields the **Connect endpoint**
    (``sc://127.0.0.1:<port>``) instead of a session: the pipeline
    (``pyspark.pipelines.cli.run``) creates and stops its own session
    against that server.
    """
    # Alias distinct from ``gold_session`` (same network): with the same
    # alias, the network DNS becomes ambiguous and Spark may talk to the
    # wrong UC server.
    uc_alias = "uc-sales"
    with contextlib.ExitStack() as stack:
        uc_server = stack.enter_context(
            _uc_server(network, uc_alias, _s3_env(floci))
        )
        _bootstrap_catalogs(
            uc_server.get_exposed_port(UC_REST_PORT), SALES_CATALOGS
        )
        spark_c = stack.enter_context(
            _spark_connect_server(
                UC_SPARK_IMAGE,
                UC_PACKAGES,
                _uc_catalog_confs(floci, SALES_CATALOGS, uc_alias),
                network,
            )
        )
        yield f"sc://127.0.0.1:{spark_c.get_exposed_port(CONNECT_PORT)}"


def delta_iceberg_session():
    """Spark 4.1 + Delta + Iceberg + UC (coexistence) env. Yields a ``SparkSession``.

    Catalogs: ``spark_catalog`` (DeltaCatalog), ``demo`` (UC) and
    ``iceberg`` (hadoop); ``defaultCatalog=demo``.
    """
    confs = {
        "spark.sql.extensions": (
            "io.delta.sql.DeltaSparkSessionExtension,"
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions"
        ),
        "spark.sql.catalog.spark_catalog": (
            "org.apache.spark.sql.delta.catalog.DeltaCatalog"
        ),
        **_iceberg_catalog_confs(),
        **_uc_demo_confs(),
    }
    yield from _uc_spark_session(
        DELTA_ICEBERG_UC_SPARK_IMAGE,
        f"{DELTA_PKG_41},{ICEBERG_PKG_41},{UC_PKGS_41}",
        confs,
    )


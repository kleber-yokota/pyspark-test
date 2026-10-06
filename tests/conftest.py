"""Fixture ``spark``: Spark real para os testes de integracao.

- Padrao: sobe um servidor Spark Connect dentro de um container Docker
  (via testcontainers) UMA vez por sessao de teste. Requer Docker (CI).
- ``SPARK_LOCAL=1``: usa Spark em modo local no host (requer JVM) —
  para desenvolver sem Docker. A sessao e a mesma API (SparkSession).
"""

from __future__ import annotations

import os

import pytest

SPARK_IMAGE = os.environ.get("SPARK_IMAGE", "apache/spark:4.2.0-python3")
CONNECT_PORT = 15002
SPARK_STARTUP_TIMEOUT_S = int(os.environ.get("SPARK_STARTUP_TIMEOUT_S", "240"))


def _docker_spark():
    """Servidor Spark Connect no container ``apache/spark`` (testcontainers)."""
    from pyspark.sql import SparkSession
    from testcontainers.core.container import DockerContainer
    from testcontainers.core.wait_strategies import PortWaitStrategy

    container = (
        DockerContainer(
            image=SPARK_IMAGE,
            command="/opt/spark/sbin/start-connect-server.sh",
            ports=[CONNECT_PORT],
        )
        .waiting_for(
            PortWaitStrategy(CONNECT_PORT).with_startup_timeout(SPARK_STARTUP_TIMEOUT_S)
        )
    )
    container.start()
    port = container.get_exposed_port(CONNECT_PORT)
    session = SparkSession.builder.remote(f"sc://127.0.0.1:{port}").getOrCreate()
    try:
        yield session
    finally:
        session.stop()
        container.stop()


def _local_spark():
    """Spark em modo local no host (SPARK_LOCAL=1; requer JVM)."""
    from pyspark.sql import SparkSession

    session = (
        SparkSession.builder.master("local[2]")
        .appName("pyspark-test")
        .getOrCreate()
    )
    try:
        yield session
    finally:
        session.stop()


@pytest.fixture(scope="session")
def spark():
    """SparkSession pronta: Docker (padrao) ou local (``SPARK_LOCAL=1``)."""
    if os.environ.get("SPARK_LOCAL") == "1":
        yield from _local_spark()
        return
    yield from _docker_spark()

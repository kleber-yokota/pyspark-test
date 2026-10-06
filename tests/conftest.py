"""Fixtures e perfis compartilhados por toda a suite de testes.

- ``spark``: sobe um servidor Spark Connect dentro de um container Docker
  (via testcontainers) UMA vez por sessao de teste (scope="session").
- Perfis do Hypothesis: ``ci`` (padrao) e ``fast`` (local/depuracao).
"""

from __future__ import annotations

import os

import pytest
from hypothesis import settings

SPARK_IMAGE = os.environ.get("SPARK_IMAGE", "apache/spark:4.2.0-python3")
CONNECT_PORT = 15002
SPARK_STARTUP_TIMEOUT_S = int(os.environ.get("SPARK_STARTUP_TIMEOUT_S", "240"))

settings.register_profile("ci", max_examples=100, deadline=None)
settings.register_profile("fast", max_examples=25, deadline=None)
settings.load_profile(os.environ.get("HYPOTHESIS_PROFILE", "ci"))


@pytest.fixture(scope="session")
def spark():
    """Servidor Spark Connect em Docker (testcontainers), pronto para conectar.

    Uso em um teste::

        def test_algo(spark):
            df = spark.range(10)

    A fixture so e acionada quando algum teste a declara como dependencia,
    entao a suite unitaria/fuzz nao paga o custo do container.
    """
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

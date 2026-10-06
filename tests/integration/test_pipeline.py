"""Testes de integracao: pipeline completa contra um Spark real (em Docker).

Rode com::

    uv run pytest tests/integration -v

Requer acesso ao Docker (o container ``apache/spark`` e subido pelo
testcontainers na fixture ``spark`` de ``tests/conftest.py``).
"""

import pytest
from pyspark.sql import functions as F

from pyspark_test import pipeline, rows

RAW_ROWS = [
    {"id": "1", "amount": "1.234,56", "customer": "ACME"},
    {"id": "2", "amount": "56,90", "customer": "ACME"},
    {"id": "3", "amount": "10,00", "customer": "GLOBEX"},
    {"id": "", "amount": "999,99", "customer": "DROP"},  # id vazio -> descartada
    {"id": "4", "amount": "abc", "customer": "DROP"},  # amount invalida -> descartada
]

# Calculado a mao: ACME = 1234.56 + 56.90, GLOBEX = 10.00
EXPECTED_TOTALS = {"ACME": 1291.46, "GLOBEX": 10.00}


def test_pipeline_agregado_por_cliente(spark) -> None:
    result = pipeline.run_pipeline(spark, RAW_ROWS)
    got = {row["customer"]: row["total"] for row in result.collect()}
    assert got == pytest.approx(EXPECTED_TOTALS)


def test_pipeline_descarta_linhas_invalidas(spark) -> None:
    df = pipeline.load_sales(spark, RAW_ROWS)
    assert df.count() == 3


def test_pipeline_entrada_vazia(spark) -> None:
    result = pipeline.run_pipeline(spark, [])
    assert result.count() == 0


def test_total_do_mundo_e_conservado(spark) -> None:
    """Agregado no Spark bate com o somatorio feito pela logica pura.

    Checa a colagem: o que sai do Spark (group by/sum) deve bater com o que a
    logica pura de ``rows.py`` produz para as mesmas linhas validas.
    """
    df = pipeline.load_sales(spark, RAW_ROWS)
    spark_total_value = df.agg(F.sum("amount").alias("t")).collect()[0]["t"]
    pure_total = sum(
        rows.parse_amount(row["amount"])
        for row in RAW_ROWS
        if rows.is_valid_row(row)
    )
    assert spark_total_value == pytest.approx(pure_total)

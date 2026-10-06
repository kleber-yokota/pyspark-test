"""Colagem da pipeline no Spark (Spark Connect).

A logica pesada vive em :mod:`pyspark_test.rows` (pura, coberta por testes
unitarios/fuzz/mutacao). Este modulo so orquestra carga, filtro e agregacao
no Spark e e coberto pelos testes de integracao.
"""

from __future__ import annotations

from collections.abc import Iterable

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from pyspark_test import rows

__all__ = ["SALES_SCHEMA", "load_sales", "run_pipeline", "total_by_customer"]

SALES_SCHEMA = "id STRING, customer STRING, amount DOUBLE"


def load_sales(spark: SparkSession, raw_rows: Iterable[dict[str, str]]) -> DataFrame:
    """Valida e normaliza linhas brutas (via :mod:`rows`) e carrega no Spark.

    Linhas com ``id`` vazio ou ``amount`` nao parseavel sao descartadas aqui.
    """
    cleaned = [
        {
            "id": str(row["id"]).strip(),
            "customer": str(row["customer"]),
            "amount": rows.parse_amount(str(row["amount"])),
        }
        for row in raw_rows
        if rows.is_valid_row(row)
    ]
    return spark.createDataFrame(cleaned, schema=SALES_SCHEMA)


def total_by_customer(df: DataFrame) -> DataFrame:
    """Agrega total e quantidade de linhas por cliente."""
    return df.groupBy("customer").agg(
        F.sum("amount").alias("total"),
        F.count_star().alias("row_count"),
    )


def run_pipeline(spark: SparkSession, raw_rows: Iterable[dict[str, str]]) -> DataFrame:
    """Pipeline completa: carga -> filtro de validacao -> agregacao por cliente."""
    return total_by_customer(load_sales(spark, raw_rows))

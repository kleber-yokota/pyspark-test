"""Transformacoes de texto em PySpark (API de DataFrame)."""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

__all__ = ["capitalize_names"]


def capitalize_names(df: DataFrame) -> DataFrame:
    """Cria a coluna ``name_cap`` com ``name`` capitalizada (``initcap``).

    Usa ``initcap`` do Spark: a primeira letra de cada palavra vai para
    maiuscula e o resto para minuscula (``"maria da silva" -> "Maria Da
    Silva"``). A coluna original ``name`` e mantida.
    """
    return df.withColumn("name_cap", F.initcap(F.col("name")))

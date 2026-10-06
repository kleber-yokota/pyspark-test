"""Testes: ``capitalize_names`` cria a coluna ``name_cap`` num Spark real.

- Padrao: Spark Connect em container Docker via **testcontainers** (fixture
  ``spark`` em ``tests/conftest.py``).
- Sem Docker: ``SPARK_LOCAL=1 uv run pytest tests -v`` (requer JVM).
"""

from pyspark_test import text


def test_capitalize_cria_nova_coluna(spark) -> None:
    df = spark.createDataFrame(
        [("maria da silva",), ("joao santos",), ("ANA LIMA",)],
        schema="name STRING",
    )
    result = text.capitalize_names(df)
    rows = result.collect()
    assert [row["name"] for row in rows] == [
        "maria da silva",
        "joao santos",
        "ANA LIMA",
    ]
    assert [row["name_cap"] for row in rows] == [
        "Maria Da Silva",
        "Joao Santos",
        "Ana Lima",
    ]


def test_capitalize_mantem_colunas_originais(spark) -> None:
    df = spark.createDataFrame([("maria da silva",)], schema="name STRING")
    assert text.capitalize_names(df).columns == ["name", "name_cap"]


def test_capitalize_nome_nulo_vira_nulo(spark) -> None:
    df = spark.createDataFrame([(None,)], schema="name STRING")
    assert text.capitalize_names(df).collect()[0]["name_cap"] is None

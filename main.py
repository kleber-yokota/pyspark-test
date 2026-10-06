"""Demo: cria um DataFrame com a coluna ``name`` e aplica ``capitalize_names``."""

from __future__ import annotations

from pyspark.sql import SparkSession

from pyspark_test import text


def main() -> None:
    spark = (
        SparkSession.builder.master("local[2]")
        .appName("pyspark-test")
        .getOrCreate()
    )
    try:
        df = spark.createDataFrame(
            [("maria da silva",), ("joao santos",), ("ANA LIMA",)],
            schema="name STRING",
        )
        text.capitalize_names(df).show()
    finally:
        spark.stop()


if __name__ == "__main__":
    main()

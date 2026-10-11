"""Final tables delivered by the pipeline (gold catalog, sales schema).

- ``sales_by_region``: quantity of products sold per region;
- ``sales_by_store``: quantity and amount (quantity * price) per store.

``format="delta"`` is the default on Databricks; explicit for the local UC
connector (which does not accept a managed table without a provider).
"""

from pyspark import pipelines as dp
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, sum

spark: SparkSession  # injected by spark-pipelines


@dp.materialized_view(format="delta")
def sales_by_region() -> DataFrame:
    return (
        spark.table("sales")
        .join(spark.table("stores"), "store_id")
        .groupBy("region")
        .agg(sum("quantity").alias("quantity"))
    )


@dp.materialized_view(format="delta")
def sales_by_store() -> DataFrame:
    return (
        spark.table("sales")
        .join(spark.table("products"), "product_id")
        .groupBy("store_id")
        .agg(
            sum("quantity").alias("quantity"),
            sum(col("quantity") * col("price")).alias("amount"),
        )
    )

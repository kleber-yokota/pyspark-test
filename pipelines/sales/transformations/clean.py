"""Clean: type casts, text trims and region extraction from the address."""

from pyspark import pipelines as dp
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, element_at, split, trim

spark: SparkSession  # injected by spark-pipelines


@dp.temporary_view
def products() -> DataFrame:
    return (
        spark.table("products_br")
        .withColumn("product_id", trim(col("product_id")))
        .withColumn("price", col("price").cast("double"))
    )


@dp.temporary_view
def sales() -> DataFrame:
    return (
        spark.table("sales_br")
        .withColumn("store_id", trim(col("store_id")))
        .withColumn("product_id", trim(col("product_id")))
        .withColumn("quantity", col("quantity").cast("int"))
    )


@dp.temporary_view
def stores() -> DataFrame:
    # Address in the ``<street and number>, <city> - <region>`` format:
    # the region is the second element of the split on `` - ``.
    return (
        spark.table("stores_br")
        .withColumn("store_id", trim(col("store_id")))
        .withColumn("region", element_at(split(col("address"), " - "), 2))
    )

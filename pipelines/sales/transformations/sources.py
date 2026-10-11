"""Raw sources: one parquet directory per entity in the S3 bucket.

- ``products``: product_id, price;
- ``sales``: store_id, product_id, quantity;
- ``stores``: store_id, address (the region is the `` - <region>`` suffix).
"""

from pyspark import pipelines as dp
from pyspark.sql import DataFrame, SparkSession

spark: SparkSession  # injected by spark-pipelines


@dp.temporary_view
def products_br() -> DataFrame:
    return spark.read.parquet(f"{spark.conf.get('pipeline.sources.path')}/products")


@dp.temporary_view
def sales_br() -> DataFrame:
    return spark.read.parquet(f"{spark.conf.get('pipeline.sources.path')}/sales")


@dp.temporary_view
def stores_br() -> DataFrame:
    return spark.read.parquet(f"{spark.conf.get('pipeline.sources.path')}/stores")

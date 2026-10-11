"""Data quality gate: validates the sources before any pipeline flow.

The module-level code runs at project import (the ``register_definitions``
of spark-pipelines injects the session into ``spark`` before
``exec_module``), so the gate validates the bucket parquets and fails the
run before processing when there is a violation.

Check types: ``not_null``, ``unique``, ``range`` (min/max) and ``values``
(value present in the reference table — referential integrity).
"""

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

spark: SparkSession  # injected by spark-pipelines


class DataQualityError(Exception):
    """Data quality violation in the pipeline sources."""


def _not_null(df: DataFrame, name: str, columns: tuple[str, ...]) -> list[str]:
    violations: list[str] = []
    for column in columns:
        n = df.filter(F.col(column).isNull()).count()
        if n:
            violations.append(f"{name}: not_null({column}): {n} row(s) with null")
    return violations


def _unique(df: DataFrame, name: str, columns: tuple[str, ...]) -> list[str]:
    n = (
        df.groupBy(*columns)
        .agg(F.count(F.lit(1)).alias("n"))
        .filter(F.col("n") > 1)
        .count()
    )
    if n:
        return [f"{name}: unique({'+'.join(columns)}): {n} duplicate group(s)"]
    return []


def _range(
    df: DataFrame, name: str, column: str, minimum: float, maximum: float | None
) -> list[str]:
    # The value is cast to double: null, non-numeric or out of range
    # (open on both ends) is a violation.
    value = F.col(column).cast("double")
    outside = value.isNull() | (value <= minimum)
    if maximum is not None:
        outside = outside | (value >= maximum)
    n = df.filter(outside).count()
    if n:
        return [f"{name}: range({column}): {n} row(s) out of range"]
    return []


def _values(
    df: DataFrame, name: str, column: str, reference: DataFrame, ref_name: str
) -> list[str]:
    n = df.join(reference, column, "left_anti").count()
    if n:
        return [
            f"{name}: values({column}): {n} row(s) with value not found in {ref_name}"
        ]
    return []


source = spark.conf.get("pipeline.sources.path")

products = spark.read.parquet(f"{source}/products")
sales = spark.read.parquet(f"{source}/sales")
stores = spark.read.parquet(f"{source}/stores")

violations: list[str] = []
violations += _not_null(products, "products", ("product_id", "price"))
violations += _unique(products, "products", ("product_id",))
violations += _range(products, "products", "price", 0.0, None)
violations += _not_null(sales, "sales", ("store_id", "product_id", "quantity"))
violations += _unique(sales, "sales", ("store_id", "product_id"))
violations += _range(sales, "sales", "quantity", 0.0, None)
violations += _not_null(stores, "stores", ("store_id", "address"))
violations += _unique(stores, "stores", ("store_id",))
violations += _values(sales, "sales", "product_id", products, "products")
violations += _values(sales, "sales", "store_id", stores, "stores")

if violations:
    raise DataQualityError("data quality check failed: " + "; ".join(violations))

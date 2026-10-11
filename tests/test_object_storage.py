"""Object storage tests: a CSV in an S3 bucket (Floci) + a real Spark.

- ``floci``/``s3_client`` (``tests/conftest.py``): the Floci container — a
  local AWS emulator — started via ``testcontainers-floci``.
- The test uploads the CSV to the bucket and Spark reads **directly from
  the bucket** (S3A, ``s3a://``) — no download to the local disk.
"""

from __future__ import annotations

from pathlib import Path

from pyspark_test import text

KEY = "names.csv"


def _csv_path() -> Path:
    return Path(__file__).parent / "data" / "names.csv"


def test_upload_csv_to_bucket(s3_client, s3_bucket) -> None:
    body = _csv_path().read_bytes()
    s3_client.put_object(Bucket=s3_bucket, Key=KEY, Body=body)

    obj = s3_client.get_object(Bucket=s3_bucket, Key=KEY)
    assert obj["Body"].read() == body

    keys = [o["Key"] for o in s3_client.list_objects_v2(Bucket=s3_bucket)["Contents"]]
    # The bucket is shared per session (jobs seed <job>/ prefixes);
    # here only the object's presence matters, not exclusivity.
    assert KEY in keys


def test_spark_reads_csv_from_bucket_and_applies_capitalize(
    spark_s3a, s3_client, s3_bucket
) -> None:
    """Spark reads the CSV directly from the bucket (S3A) and applies
    ``capitalize_names``."""
    body = _csv_path().read_bytes()
    s3_client.put_object(Bucket=s3_bucket, Key=KEY, Body=body)

    result = text.capitalize_names(
        spark_s3a.read.csv(f"s3a://{s3_bucket}/{KEY}", header=True)
    )
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

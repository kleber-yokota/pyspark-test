# pyspark-test

**How to test Spark locally — a working, end-to-end example.**

This repository is a local test suite that runs a production-like
**Databricks bundle** (`databricks.yml` + `flows/`) and a **Spark
Declarative Pipeline** (`pipelines/sales/`) against **real Spark in
Docker** — with **Unity Catalog** (REST), **Delta Lake**, **Iceberg** and
**S3** (emulated). No Databricks workspace, no cloud account, no secrets,
no Java on the host.

The point is not the toy transforms; it is the **recipe**: how to assemble
real engines, real metadata and reproducible containers so that the code
you ship to Databricks is exercised exactly as it will run there.

---

## What was built

| Piece | What it is |
|---|---|
| `databricks.yml` | A Free Edition bundle with two jobs: **`etl`** (`spark_python_task` → SQL validation) and **`names`** (4 tasks: read + clean two CSVs from the bucket, join on `id`, save a Delta gold table). Sensitive values (`workspace.host`, `warehouse_id`) are masked with `<PLACEHOLDER>` comments. |
| `flows/` | The task code, 100% PySpark + pure SQL: medallion (`orders_bronze`/`orders_silver`, `names_bronze`/`surnames_bronze`, `gold.people.names`). |
| `pipelines/sales/` | A **Spark Declarative Pipelines** project: Parquet sources in S3 (`products`, `sales`, `stores`) → cleaning → data quality (`not_null`, `unique`, `range`, FK `values`) → two outputs: `gold.sales.sales_by_region` and `gold.sales.sales_by_store`. |
| `tests/orchestrator.py` | A tiny orchestrator: parses the bundle YAML, builds the task graph (`depends_on`), runs tasks locally — so the **same YAML** drives Databricks and the local test. |
| `tests/conftest.py` + `tests/infra.py` | Session fixtures: Spark Connect, Unity Catalog server, Floci (S3), S3A, and per-image catalog setups. |
| `tests/` | **37 tests in ~3 minutes** (locally): unit (no Docker) + integration against real Spark. |

### Test matrix

| Group | Files | Engine |
|---|---|---|
| DataFrame transforms | `test_capitalize.py`, `test_etl.py` | Spark 4.2 |
| Object storage (S3A) | `test_object_storage.py` | Spark 4.2 + hadoop-aws + Floci |
| Bundle jobs (yml-driven) | `test_job_bundle.py`, `test_orchestrator.py` | Spark 4.2 + S3A |
| Unity Catalog (Delta) | `test_unity_catalog.py` | Spark 4.2 + Delta 4.4.1 + UC 0.6.0 |
| SDP pipeline (sales) | `test_pipeline_sales.py` | Spark 4.2 + Delta + UC |
| Iceberg on 4.1 | `iceberg_uc/test_smoke.py` | Spark 4.1 + Iceberg 1.12.0 + UC |
| Delta + Iceberg coexistence | `delta_iceberg_uc/test_smoke.py` | Spark 4.1 + Delta + Iceberg + UC |

---

## Why testcontainers (the short answer)

PySpark 4.x is **client-only** (Spark Connect, gRPC): there is no
`local` master and no JVM on the host. The engine has to live somewhere —
and with testcontainers it lives in a **pinned, disposable, reproducible
container** that is created and destroyed with the test session
(`ExitStack`). The suite is worth it because it gives **Databricks
parity**: the SQL in `flows/` runs exactly as written, against real
Delta/UC metadata and emulated S3. Mocking a `SparkSession` would hide
exactly the failures Databricks has (catalogs, URI schemes,
credentials). The measured cost — a cold Spark Connect session of ~6–19s
per environment — is analyzed in [`test-performance.md`](test-performance.md).

**testcontainers is what makes the difference**: the same code that
passes locally is the code that runs in CI and in Databricks, because
the engine, the metadata and the storage are real (or emulated) in all
three places.

## What was learned — the reference docs

| Doc | What you get from it |
|---|---|
| [`spark.md`](spark.md) | The complete setup recipe: containers, Docker networks and aliases, the S3A/UC/Delta confs, ports, pinned versions, and the gotchas (symptom → cause → solution). |
| [`spark-testing-lessons.md`](spark-testing-lessons.md) | The lessons: why UC must back named catalogs, the `DeltaCatalog` NPE, session lifecycle (never leak a session — gRPC threads keep the process alive), the committed-spec rule for pipeline tests, and a checklist for a new test environment. |
| [`test-performance.md`](test-performance.md) | Where the ~3 minutes go (7 cold starts), why 7 environments cannot be 1, and what actually reduces runtime. |

## How to run

```bash
uv sync
make lint        # ruff
make test        # the full suite — Spark in Docker (requires Docker)
```

Useful environment variables (defaults shown):

| Var | Default | What it controls |
|---|---|---|
| `SPARK_IMAGE` | `apache/spark:4.2.0-python3` | image of the plain Spark fixture |
| `UC_SPARK_IMAGE` | `spark-uc:0.3.0` | custom image with pre-baked Delta/UC/S3A jars |
| `FLOCI_IMAGE` | `floci/floci:2.2.0` | S3 emulator |
| `UC_IMAGE` | `unitycatalog/unitycatalog:v0.6.0` | UC server |
| `SPARK_STARTUP_TIMEOUT_S` | `60` | container wait timeout |

The custom images (`Dockerfile.uc`, `Dockerfile.iceberg-uc`,
`Dockerfile.delta-iceberg-uc`) pre-bake the jars at build time (Ivy cache
hit at runtime) and run as a non-root user.

## CI (`.github/workflows/ci.yml`)

1. **lint** — `ruff check`
2. **test** — warms the image cache, builds the custom images, runs
   `pytest` with coverage floor (`--cov-fail-under=90`) and uploads the
   coverage report.

## About this project

Built locally, with a local LLM
(`Qwen3.8-27B-UD-Q2_K_XL.gguf` quantized, running on an RTX 5070Ti) driving
the implementation — the same setup this repository demonstrates: real
engines, no cloud.

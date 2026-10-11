# Spark in Testcontainers + Unity Catalog

Reference for the test setup of this project: **real Spark in Docker via
testcontainers** and how to connect it to the **Unity Catalog** (UC) as the
metadata catalog (Delta tables).

**Status: recipe validated end to end** (2026-10) — Spark 4.2.0 +
Delta 4.4.1 + UC 0.6.0, with the custom image `spark-uc:0.3.0` (pre-baked
jars, non-root user) and the `uc` fixture in `tests/conftest.py`.

---

## 1. Current setup: Spark in testcontainers

### How it works

- The tests run against a **real Spark** (no mock): a **Spark Connect**
  server inside a Docker container (`apache/spark:4.2.0-python3`), started
  **once per test session** via
  [testcontainers](https://testcontainers.com/).
- The `spark` fixture (`tests/conftest.py`) exposes a `SparkSession`,
  connected with `SparkSession.builder.remote("sc://127.0.0.1:<port>")`.
- The `uc` fixture starts the UC server + a second Spark Connect server
  (custom image with Delta + UC) on the same Docker network, and exposes a
  session with `demo` as the default catalog.
- Files that Spark needs to read (e.g. the ETL CSV) are copied into the
  Spark container (the container does not see the host filesystem).

### Fixtures (`tests/conftest.py`)

| Fixture | Scope | Description |
|---|---|---|
| `spark` | session | `SparkSession` — Spark Connect in a Docker container (testcontainers) |
| `floci_network` | session | Docker network shared by Floci and Spark (S3A) |
| `floci` | session | Floci container (local AWS services emulator), on the `floci_network` network |
| `s3_client` | session | `boto3` client pointed at the emulated S3 (Floci) |
| `s3_bucket` | session | Bucket created once per session (Floci starts empty on every run) |
| `spark_s3a` | session | `SparkSession` — Spark Connect with S3A (hadoop-aws); reads `s3a://` directly from Floci |
| `uc` | session | `SparkSession` — Spark Connect with UC + Delta (default catalog `demo`), dedicated network with the UC server |

Decorator (same module):

| Decorator | Scope | Description |
|---|---|---|
| `@uc_clean` | function | Drops the tables and schemas created in the catalog after the test (even on failure) — Django-style isolation |

### Environment variables

| Var | Default | Use |
|---|---|---|
| `SPARK_IMAGE` | `apache/spark:4.2.0-python3` | Image of the Spark Connect server (`spark` fixture) |
| `FLOCI_IMAGE` | `floci/floci:2.2.0` | Image of Floci (emulated S3) |
| `SPARK_STARTUP_TIMEOUT_S` | `60` | Timeout of the Spark container wait |
| `UC_IMAGE` | `unitycatalog/unitycatalog:v0.6.0` | Image of the UC server |
| `UC_SPARK_IMAGE` | `spark-uc:0.3.0` | Custom image with the pre-baked jars (see §3) |
| `UC_STARTUP_TIMEOUT_S` | `180` | Timeout of the UC Spark container wait |
| `S3A_SPARK_IMAGE` | `spark-uc:0.3.0` | Image of the `spark_s3a` fixture (reuses the Ivy cache with hadoop-aws) |

The UC tests use `pytest.mark.timeout(180)` (3 minutes per test,
`pytest-timeout`) so the suite never hangs.

### Spark container details

- Command: `/opt/spark/sbin/start-connect-server.sh --wait`
  — `--wait` is mandatory: without it the script daemonizes the server
  and the container exits with exit 0.
- Exposed port: `15002` (Spark Connect).

### How to run

```bash
make test               # Spark in Docker (requires Docker on the host)
make lint               # ruff
```

### CI (`.github/workflows/ci.yml`)

- Runner `ubuntu-latest` (Docker available → testcontainers works).
- The job **warms the image cache** (`apache/spark`, `floci`,
  `unitycatalog/unitycatalog:v0.6.0`) and **builds the custom image**
  (`docker build -f Dockerfile.uc -t spark-uc:0.3.0 .`) before pytest.
- pytest with a coverage floor (`--cov-fail-under=90`) and a coverage
  artifact.

---

## 2. Unity Catalog (UC)

### What it is

**Unity Catalog** is the open-source (Apache 2.0) data and AI catalog,
open-sourced by Databricks (an LF AI & Data sandbox project). It serves as
a metastore: organizes assets (tables, schemas, catalogs, volumes, models)
and controls governance. What matters for this project:

- **Multi-engine**: Spark uses UC via the connector (REST), the same way
  DuckDB, Flink, Trino, etc. do;
- **Compatible with the Hive Metastore API** and with the Iceberg REST API;
- **Multi-format**: Delta Lake, Iceberg, Hudi (via UniForm), Parquet...
- **Storage-agnostic**: local (default), S3, GCS or Azure ADLS (via
  `server.properties`);
- **Credential vending**: Spark receives scoped credentials from the server
  to access the tables' storage.

### Components and ports

| Component | Port | Note |
|---|---|---|
| UC server (REST API) | `8080` | Java 17; image `unitycatalog/unitycatalog` |
| Backend (Armeria) | `8081` | Internal; do not expose |
| gRPC | `9091` | For gRPC clients (not used here) |
| Web UI | `3000` | Optional in tests |
| Spark connector | — | Maven JAR `io.unitycatalog:unitycatalog-spark_*` |

REST base: `http://<host>:8080/api/2.1/unity-catalog` (endpoints
`/catalogs`, `/schemas`, `/tables`...). The server keeps **metadata** in
its data directory (H2); the **table data files (Delta)** are written by
**Spark** to the table's `LOCATION`.

### Versions (pinned for this project — validated)

| Component | Version |
|---|---|
| UC server image | `unitycatalog/unitycatalog:v0.6.0` |
| Spark connector (Spark 4.2.x) | `io.unitycatalog:unitycatalog-spark_4.2_2.13:0.6.0` |
| Delta Lake (Spark 4.2) | `io.delta:delta-spark_4.2_2.13:4.4.1` |
| Spark | `4.2.0` (image `apache/spark:4.2.0-python3`) |

⚠️ **Delta for Spark 4.2 uses the SUFFIXED artifact `delta-spark_4.2_2.13`**
(available since Delta 4.4.0). The `delta-spark_2.13:4.3.1` resolves and
downloads without error, but breaks at runtime on Spark 4.2.0
(`NoSuchMethodError: CatalogStorageFormat.copy(...)` — Spark 4.2
refactored the method signature).

### Validated recipe (Spark 4.2.0 + Delta 4.4.1 + UC 0.6.0)

```bash
/opt/spark/sbin/start-connect-server.sh --wait \
  --packages 'io.delta:delta-spark_4.2_2.13:4.4.1,io.unitycatalog:unitycatalog-spark_4.2_2.13:0.6.0' \
  --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension \
  --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog \
  --conf spark.sql.catalog.demo=io.unitycatalog.spark.UCSingleCatalog \
  --conf spark.sql.catalog.demo.uri=http://uc:8080 \
  --conf spark.sql.catalog.demo.type=static \
  --conf spark.sql.catalog.demo.token= \
  --conf spark.sql.defaultCatalog=demo
```

- `UCSingleCatalog` is the connector's catalog plugin; **the catalog must
  be registered in Spark with the SAME name as in UC** (`demo`);
- `type=static` + empty `token=` is required by the connector (local UC
  without authentication);
- `spark.sql.catalog.spark_catalog=...DeltaCatalog` is **mandatory on
  Delta 4.4.x**: without a `DeltaCatalog` registered in the session,
  Delta writes fail with `DELTA_CONFIGURE_SPARK_SESSION_WITH_EXTENSION_AND_CATALOG`
  (a wrapper error for writes to V2 tables);
- `uri` points to the UC server (`http://uc:8080` = testcontainers network
  alias; outside the network, `http://127.0.0.1:<exposed port>`);
- `spark.sql.defaultCatalog=demo` makes name resolution use UC.

Verified: `CREATE TABLE ... USING delta LOCATION`, `INSERT INTO` and
`SELECT` working via Spark Connect, with the Delta files in the
`LOCATION` (a shared directory).

### Starting the UC server (reference)

Docker (official image):

```bash
docker run --rm -d -p 8080:8080 \
  -v uc_data:/opt/unitycatalog/etc/data \
  unitycatalog/unitycatalog:v0.6.0
```

The official `compose.yaml` starts server + UI (`docker compose up` → UI
on `localhost:3000`, server on `localhost:8080`), with a bind mount of
`./etc/conf` (config, e.g. S3) and a persistent volume in
`/opt/unitycatalog/etc/data`.

### Creating catalogs/schemas (REST — the `bin/uc` CLI is broken in the image)

The `bin/uc` of the official container **does not work** (it expects
`$ROOT_DIR/jars/classpath`, which does not exist in the image). Use the
REST API (the `uc` fixture does exactly this):

```bash
# catalog (storage_root is only used by managed tables — experimental)
curl -s -X POST http://localhost:8080/api/2.1/unity-catalog/catalogs \
  -H 'Content-Type: application/json' \
  -d '{"name":"demo","storage_root":"/opt/unitycatalog/etc/data/demo"}'

# schema
curl -s -X POST http://localhost:8080/api/2.1/unity-catalog/schemas \
  -H 'Content-Type: application/json' \
  -d '{"catalog_name":"demo","name":"default"}'
```

Alternative: the `unitycatalog-client` Python package (PyPI) to create
catalogs/schemas/tables via REST from the test.

### Usage example in a test (PySpark Connect)

```python
@uc_clean
def test_mine(uc) -> None:
    uc.sql("CREATE TABLE demo.default.people (name STRING) USING delta LOCATION '/opt/spark/data/people'")
    uc.sql("INSERT INTO demo.default.people VALUES ('maria da silva'), ('ANA LIMA')")
    rows = uc.sql("SELECT name FROM demo.default.people").collect()
    # or: df = uc.table("demo.default.people")
```

EXTERNAL table with a `LOCATION` local to the Spark container — the UC
server keeps only the metadata.

`@uc_clean` guarantees that the test's tables (and created schemas, e.g.
`demo.silver`) are dropped after the test, via the session itself (the
cleanup runs even when the test fails). The catalog and the `default`
schema remain — they are the session's "database setup" (analogy: Django's
rollback).

### The `uc` fixture (implemented in `tests/conftest.py`)

- Session-scoped;
- Yields a `SparkSession` (same shape as the `spark` fixture): the catalog
  and the schema are controlled by the session conf
  (`spark.sql.defaultCatalog=demo`), so the test uses qualified names
  (`demo.default.table`) in the SQL;
- Dedicated Docker network with the UC server (alias `uc`) + the Spark
  Connect server (`UC_SPARK_IMAGE`); the ETL CSV is copied into the
  container (Spark does not see the host filesystem);
- Bootstraps catalog `demo` + schema `default` via REST;
- The UC wait uses `HttpWaitStrategy` (port 8080 accepts TCP before the API
  is ready — initial requests are reset);
- The Spark wait uses a wait strategy that **fails fast if the container
  dies** (the native wait only checks the port and hangs forever);
- Cleanup: `@uc_clean` (decorator in `tests/conftest.py`) —
  `SHOW NAMESPACES` + `SHOW TABLES` + `DROP TABLE`/`DROP SCHEMA CASCADE`
  via the session itself (the connector issues the REST DELETEs with the
  **qualified** name — a short name gives 400 `INVALID_ARGUMENT`); the
  `default` schema remains, the others (e.g. `silver`) are dropped;
- The session uses `SparkSession.builder.remote(...).create()` (not
  `getOrCreate()`): the suite has two sessions (the `spark` + `uc`
  fixtures); `getOrCreate` would return the previous global one.

### Gotchas (learned during validation)

1. **`spark_catalog` on Delta 4.4.x**: the session needs a `DeltaCatalog`
   registered (`spark.sql.catalog.spark_catalog=
   org.apache.spark.sql.delta.catalog.DeltaCatalog`) — different from the
   old mode of pointing `spark_catalog` at the `UCSingleCatalog`, which was
   the workaround for the `Catalog not found: spark_catalog` quirk.
2. **Managed tables are experimental** in UC OSS: they require
   `server.managed-table.enabled=true` in `server.properties`
   (bind mount `./etc/conf` → `/opt/unitycatalog/etc/conf`) **and** the
   property `delta.feature.catalogManaged=supported`.
   **For unit tests, use EXTERNAL tables** with an explicit `LOCATION` —
   no experimental flag.
3. **Spark writes the data**: the table's `LOCATION` must be a path
   accessible by the **Spark container** (e.g. `/opt/spark/data/...`), not
   by the UC container.
4. **First use downloads jars from Maven**: `--packages` resolves at
   runtime (~105 MB of jars). The custom image (§3) pre-downloads them at
   build time.
5. **BuildKit pins `user.home=/root` at build time**: `ENV HOME` does not
   change the ivy cache during `docker build` — the cache lands in
   `/root/.ivy2.5.2` and must be copied to the final user's `HOME`.
6. **A nameless uid breaks Hadoop**: `USER 1001` (numeric) fails in
   `getpwuid` (`Invalid UID, could not determine effective user`) → create
   a named user (`useradd -u 1001 appuser`).
7. **`work-dir`/`data` need the right owner**: if the final user cannot
   write to `/opt/spark/work-dir`, the metastore Derby starts READ ONLY
   (a confusing JDO error that is NOT from the UC server).
8. **Command string assembly**: `" ".join(...)` concatenated without a
   space becomes `--packages A,B--conf ...` (a single token) — Spark tries
   to resolve the artifact `A,B--conf`, the server dies and the wait hangs.
9. **`getOrCreate()` vs `create()`**: with two sessions in the suite, use
   `.create()` for each one (see the fixture above).

### S3 (Floci) — S3A enabled in the custom images

The official Spark image **does not include `hadoop-aws`** (S3A): without
it, `s3a://` fails with `No FileSystem for scheme 's3a'`. The custom
images pre-bake `hadoop-aws` at build time (same pattern as the Ivy cache):

| Image | Spark | Hadoop | hadoop-aws (pinned) |
|---|---|---|---|
| `spark-uc:0.3.0` | 4.2.0 | 3.5.0 | `org.apache.hadoop:hadoop-aws:3.5.0` |
| `spark-iceberg-uc:0.2.0` | 4.1.2 | 3.4.2 | `org.apache.hadoop:hadoop-aws:3.4.2` |
| `spark-delta-iceberg-uc:0.2.0` | 4.1.2 | 3.4.2 | `org.apache.hadoop:hadoop-aws:3.4.2` |

Validated E2E (2026-10) in the suite: the `spark_s3a` fixture starts Spark
on the same Docker network as Floci (alias `floci`) and the test
`test_object_storage.py::test_spark_reads_csv_from_bucket_and_applies_capitalize`
does `spark.read.csv("s3a://<bucket>/<key>")` directly from the bucket:

```
--conf spark.hadoop.fs.s3a.endpoint=http://floci:4566
--conf spark.hadoop.fs.s3a.access.key=<key>
--conf spark.hadoop.fs.s3a.secret.key=<secret>
--conf spark.hadoop.fs.s3a.path.style.access=true
```

- Floci: S3 on port `4566`, **path-style** mandatory and HTTP (no SSL);
- Floci default credentials: `test`/`test`;
- the `spark.hadoop.` prefix is mandatory: without it the conf does not
  reach the Hadoop `Configuration` used in file reads.

For UC tables with data on the emulated S3 (not yet implemented):

1. Floci is on the same Docker network as Spark (`floci_network` fixture);
   `spark_s3a` reads `s3a://` directly from the bucket;
2. Configure the UC server in `server.properties`
   (bind mount `./etc/conf` → `/opt/unitycatalog/etc/conf`):

   ```properties
   s3.bucketPath.0=<bucket>
   s3.region.0=us-east-1
   s3.accessKey.0=<key>        # Floci credentials
   s3.secretKey.0=<secret>
   ```

---

## 3. Custom image (`Dockerfile.uc` → `spark-uc:0.3.0`)

Goal: **not download Delta/UC from Maven at runtime** (faster CI and
offline) and run as a **non-root user (uid 1001)**.

```dockerfile
FROM apache/spark:4.2.0-python3

ARG DELTA_VERSION=4.4.1
ARG UC_VERSION=0.6.0
# S3A: version of the Hadoop bundled with Spark 4.2.0 (hadoop-client-3.5.0).
ARG HADOOP_AWS_VERSION=3.5.0
ARG APP_UID=1001
ARG APP_HOME=/home/app

ENV HOME ${APP_HOME}

# Named user with a fixed uid: Hadoop (UgiLinuxImpl) calls getpwuid for the
# uid at runtime — a uid with no entry in /etc/passwd breaks Spark
# ("Invalid UID, could not determine effective user").
USER root
RUN useradd -u "${APP_UID}" -d "${APP_HOME}" -s /bin/false appuser

# Pre-fetch: resolves Delta + UC connector (and transitive dependencies).
# Runs as root. BuildKit detail: at build time the sandbox pins the JVM's
# user.home to /root (ignoring HOME), so the cache lands in
# /root/.ivy2.5.2 — we copy it to the final user's home.
USER root
RUN /opt/spark/bin/spark-sql \
      --master 'local[1]' \
      --packages "io.delta:delta-spark_4.2_2.13:${DELTA_VERSION},io.unitycatalog:unitycatalog-spark_4.2_2.13:${UC_VERSION},org.apache.hadoop:hadoop-aws:${HADOOP_AWS_VERSION}" \
      --conf "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension" \
      -e 'SELECT 1' \
    && mkdir -p "${APP_HOME}" \
    && cp -a /root/.ivy2.5.2 "${APP_HOME}/.ivy2.5.2" \
    && chown -R "${APP_UID}:${APP_UID}" "${APP_HOME}" \
      /opt/spark/work-dir /opt/spark/data

# Final user (non-root), home /home/app with the Ivy cache ready.
USER ${APP_UID}
```

Validations:

- **Offline**: `spark-sql --packages <full runtime string>` →
  `0 artifacts copied, N already retrieved` (cache hit; no download).
- **E2E**: `CREATE TABLE` + `INSERT` + `SELECT` against the UC server, as
  user 1001, on a network with UC.

Building the image:

```bash
docker build -f Dockerfile.uc -t spark-uc:0.3.0 .
```

### CI (`.github/workflows/ci.yml`)

- Warm the images: `docker pull unitycatalog/unitycatalog:v0.6.0`
- Build the custom image: `docker build -f Dockerfile.uc -t spark-uc:0.3.0 .`

---

## 4. Sources

- UC docs — Quickstart: <https://docs.unitycatalog.io/quickstart/>
- UC docs — Docker Compose: <https://docs.unitycatalog.io/docker_compose/>
- UC docs — Spark integration: <https://docs.unitycatalog.io/integrations/unity-catalog-spark/>
- GitHub: <https://github.com/unitycatalog/unitycatalog>
- Release 0.6.0 (artifacts/versions): <https://github.com/unitycatalog/unitycatalog/releases/tag/v0.6.0>
- Docker Hub: <https://hub.docker.com/r/unitycatalog/unitycatalog>
- Issue #1944 (delta-spark_4.2 only exists since Delta 4.4.0):
  <https://github.com/unitycatalog/unitycatalog/issues/1944>
- Spark Connect + UC demo (Spark 4.1.1/4.2.0, UC 0.4.1–0.6.0):
  <https://books.japila.pl/spark-declarative-pipelines/demo/unity-catalog/>

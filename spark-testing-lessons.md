# Lessons learned — testing with real Spark

Reference for **how to test** (and why) — complements
[`spark.md`](spark.md) (setup reference) and
[`test-performance.md`](test-performance.md) (fixture cost).

Central question: **why testcontainers, and what has to come together for
it to work?**

---

## 1. Why testcontainers (and not local Spark nor mocks)

- The PySpark 4.2 used here is **client-only (Spark Connect)**: there is no
  `master=local` and no JVM on the host. `SparkSession.builder.remote("sc://...")`
  speaks gRPC with a real Spark server — which has to live somewhere.
- The suite is worth it for **parity with Databricks**: the SQL in
  `flows/` runs exactly as written, real Delta, real Unity Catalog (REST),
  real S3 (emulated). Mocking `SparkSession`/DataFrame would hide exactly
  the problems Databricks has (catalog, URI scheme, credentials).
- Testcontainers gives a **throwaway and reproducible** environment:
  pinned images, everything up and down with the test session
  (`ExitStack`), nothing left on the host, nothing leaking between runs.

Cost: Spark takes ~23s to start per job (pre-baked image — see
`test-performance.md`). The alternative (mocks) is cheap and false.

## 2. What has to come together for it to work (the 5 pieces)

### Piece 1 — a Docker network, stable aliases

All containers on a shared `Network`. The containers **do not see the
host's `127.0.0.1`**: Spark talks to S3 and to UC via the network
**aliases** (`floci`, `uc`).

> Gotcha: two UC servers on the same network with the same alias make the
> DNS ambiguous — Spark may talk to the wrong server. One alias per server
> (`uc`, `uc-sales`).

### Piece 2 — one container per component (pinned images)

| Container | Image | Role |
|---|---|---|
| Floci | `floci/floci:2.2.0` | S3 (data + `storage_root` of the catalogs) |
| UC server | `unitycatalog/unitycatalog:v0.6.0` | Metadata via REST (catalogs/schemas) |
| Spark Connect | `spark-uc:0.3.0` (custom) | Engine: Spark 4.2 + Delta 4.4.1 + UC 0.6.0 + hadoop-aws 3.5.0 |

The custom image (`Dockerfile.uc`) pre-bakes the jars: without it, every
container downloads ~105 MB from Ivy (and `--packages` becomes a download
on every run). The build pre-fetchs and the runtime is a **cache hit**.

**Why pre-baked is good:**

- **Speed**: `--packages` on a fresh container resolves and downloads
  ~105 MB (57 artifacts — Delta + UC connector + hadoop-aws) from Maven
  Central on every startup; pre-baked moves that download to the build and
  the runtime becomes an Ivy cache hit (cost measured in
  `test-performance.md`).
- **Pin in one place**: the versions (Delta 4.4.1, UC 0.6.0, hadoop-aws
  3.5.0 — the one bundled with Spark 4.2) live in the Dockerfile; the test
  does not drift when Ivy re-resolves transitive dependencies, and the
  wrong hadoop-aws breaks S3A silently.
- **Hermeticity**: the runtime does not need Maven Central — CI without
  network to the repository, no rate limit, reproducible.
- **Fail-fast**: a `--packages` with a nonexistent artifact fails in the
  Ivy resolution at container boot — `_FailFastPortWait` captures it with
  the log, instead of a generic 60s timeout.
- **S3A**: the official `apache/spark` image does not include `hadoop-aws`
  (`s3a://` fails without it) — pre-baked is mandatory, and the fixtures
  reuse the same image (the jar is already cached).
- **Non-root + Ivy**: the Ivy cache needs an owner = the runtime user (and
  the uid needs an entry in `/etc/passwd` — Hadoop calls `getpwuid`); all
  of that is a build-time concern, solved in one place (the BuildKit
  `user.home` gotcha, in the table).

### Piece 3 — confs per container (the glue)

Spark (via `start-connect-server.sh --conf ...`):

- **S3A**: `spark.hadoop.fs.s3a.{endpoint,access.key,secret.key,
  path.style.access}` — the endpoint is the **network alias**
  (`http://floci:9000`), never the host.
- **`spark.hadoop.fs.s3.impl=org.apache.hadoop.fs.s3a.S3AFileSystem`**:
  the UC server only knows the **`s3`** scheme (not `s3a`) for
  `storage_root`; that mapping is what resolves `s3://` in Spark.
- **Delta**: `spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension`
  + a **registered** `spark_catalog` (Delta 4.4 validates this in
  `checkRequiredConfigurations`, even without use — the pipeline server
  registers it just to pass the check).
- **UC catalogs** (one per name):
  `spark.sql.catalog.<name>=io.unitycatalog.spark.UCSingleCatalog` +
  `.uri=http://<alias>:8080` + `.type=static` + `.token=` (empty).

UC server (env vars, per bucket):

- `s3.bucketPath.0`, `s3.region.0`, `s3.accessKey.0`, `s3.secretKey.0`,
  `s3.sessionToken.0` — without these the server does not vend
  credentials for managed tables on `s3://` and
  `CREATE TABLE ... USING delta` fails.

### Piece 4 — bootstrap via REST (not DDL)

Catalogs + schemas are created with `POST /api/2.1/unity-catalog/catalogs`
and `/schemas` (the `bin/uc` CLI of the image is broken). The fixture does
the bootstrap **before** starting Spark — the session already finds
`gold.people`, `silver.people`, etc. ready.

### Piece 5 — session fixtures (with guaranteed teardown)

- `SparkSession.builder.remote(...).create()` — **never `getOrCreate`**
  (the suite creates several sessions; `getOrCreate` would return the
  previous global one).
- `stack.callback(session.stop)` registered **before** the first SQL that
  can fail — a stopped session is what makes the test process exit (the
  gRPC threads are non-daemon: a leaked session = `pytest` never exits).
- The pipeline (SDP) server **yields the endpoint**
  (`sc://127.0.0.1:<port>`), not the session: the
  `pyspark.pipelines.cli.run` CLI creates and stops its own session
  against that server.

## 3. Gotchas (symptom → cause → solution)

| Symptom | Cause | Solution |
|---|---|---|
| `NullPointerException ... CatalogPlugin.name()` when using a named catalog (e.g. `gold`) | `DeltaCatalog` inherits from `DelegatingCatalogExtension`; the delegate is only set when the catalog is `spark_catalog`. Bug `delta-io/delta#3312` (closed NOT_PLANNED) | Named catalogs are **UC** (`UCSingleCatalog`); `DeltaCatalog` only as `spark_catalog` |
| `DELTA_CONFIGURE_SPARK_SESSION_WITH_EXTENSION_AND_CATALOG` | Delta 4.4 requires the extension + `spark_catalog` in the session, even without use | Always register both confs when Delta is in the image |
| `NoSuchMethodError` in Delta on Spark 4.2 | The `delta-spark_2.13` artifact was compiled against 4.1 | Use the **suffixed** artifact `delta-spark_4.2_2.13:4.4.1` |
| `No FileSystem for scheme 's3a'` | The official Spark image does not include `hadoop-aws` | Pre-baked in the image + `--packages` (cache hit) |
| Catalog `s3a://...` rejected by UC | UC v0.6.0's `UriScheme` only knows `s3` (and it does not touch the local filesystem for a cloud scheme) | `storage_root` in `s3://` + `fs.s3.impl` mapping to S3A |
| Managed table without credentials on S3 | UC server without the bucket configuration | `s3.*.0` env vars on the UC container |
| `[CATALOG_NOT_FOUND] gold` in a pipeline run | In remote-only mode, `getOrCreate()` consults the active session (thread-local) before `SPARK_REMOTE` — a fixture session from another test leaked | Force `ConnectSparkSession._active_session.session` before the run and clean it in the `finally` |
| `type object 'SparkSession' has no attribute '_active_session'` | `_active_session` only exists on the **connect** class (`pyspark.sql.connect.session.SparkSession`), not the unified one | Use the connect class to force/clean the active session |
| `pytest` never finishes (hang at exit) | The Connect session was not stopped (gRPC non-daemon) | Every session created in a test is stopped; the fixture uses `stack.callback(session.stop)` before the SQL that can fail. `pytest-timeout` does not save you: it kills the test, not the process — and the hang is at exit, after the summary |
| `SHOW CATALOGS` does not list a conf-registered catalog | The command only returns `spark_catalog`; catalogs via conf (`demo`, `gold`...) do not appear | Discover the session's catalogs from the conf (`spark.sql.catalog.*`) — that is what the cleanup does |
| `DROP SCHEMA spark_catalog.default` fails | Spark does not allow dropping the default database | The cleanup keeps `spark_catalog.default` (and the UC default) and only drops the tables inside; catalogs without namespace enumeration (Iceberg hadoop) are skipped — the container is ephemeral |
| Container dies at boot (exit 0) with no warning | `start-connect-server.sh` without `--wait` daemonizes and the container exits | Always `--wait` (server in the foreground) |
| Port wait "ok" but the REST fails | The TCP port accepts before the API is ready (request reset) | Wait strategy probe on the real path (`/api/2.1/unity-catalog/catalogs`) |
| Generic 60s timeout masking an error | Port open ≠ process alive (e.g. `--packages` with a nonexistent artifact) | `_FailFastPortWait`: checks the container status on every poll and fails with the log |
| `Invalid UID, could not determine effective user` | Hadoop (`UgiLinuxImpl`) calls `getpwuid` for the uid; a uid with no entry in `/etc/passwd` breaks | Named user with a fixed uid in the image |
| Empty Ivy cache for the final user | BuildKit pins `user.home=/root` at build time (ignores `HOME`) | Copy `/root/.ivy2.5.2` to the final user's home at build time |
| `F.count_star` does not exist (4.2) | Removed API | `F.count(F.lit(1))` |

## 4. Test conventions (what "doing it right" means)

- **TDD**: unit without Docker (orchestrator, pipeline spec) +
  integration with real Spark (session fixtures, session scope).
- **Pure SQL**: the files in `flows/` run exactly as written — no
  `replace`/templating in the test; one file = one statement.
- **Unqualified names for intermediates**, explicit catalog on the final
  target (`gold.people.names`); explicit Delta (`USING delta` /
  `.format("delta")`) when the local side requires it and Databricks is
  neutral.
- **The committed spec is the source of truth**: the pipeline test only
  changes `configuration.pipeline.sources.path` (sibling spec, removed
  afterwards) — it never rewrites the project spec.
- **Deterministic asserts**: explicit `ORDER BY` + access by attribute
  (`r.region`), never by tuple position.
- **Data in the bucket** (`s3a://pyspark-test/...`), not in the host
  filesystem — the Spark container does not see it.

## 5. Checklist — setting up a new test environment

1. Network + aliases (one alias per service, unique on the network).
2. S3 (Floci) on the network; bucket with a fixed name (Floci starts
   empty).
3. UC server on the network + the bucket's `s3.*.0` env vars.
4. REST bootstrap: catalogs + schemas.
5. Spark Connect on the same network: `--wait`, `--packages` (cache hit),
   S3A/S3 confs, Delta extension, registered catalogs, UC catalog(s) with
   `UCSingleCatalog`.
6. A fixture that creates the session with `create()`, registers
   `stack.callback(stop)` and only then runs SQL.
7. Test: data in the bucket → run → assert with SQL using `ORDER BY`.

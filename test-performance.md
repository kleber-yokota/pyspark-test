# Test suite performance (why 37 tests take ~3 minutes)

**Conclusion (measured, 2026-10):** the time is not in the tests — it is in
the infrastructure. The suite starts **7 independent Spark environments in
Docker** (session-scoped fixtures) and the **first query of each
environment pays the Spark Connect session cold start** (~5–19s). Each
test itself takes <1s (most <0.1s).

The pipeline test (``pipelines/sales``) pays three Connect cold starts
(seed session, run session, assert session) against the same server.

Reference setup: [`spark.md`](spark.md).

---

## 1. Measurement (real run: `pytest` → 37 passed in ~167s)

### Cost per stage of the `uc` fixture (instrumented step by step)

| Stage | Time |
|---|---|
| UC server container (HTTP ready) | 2.70s |
| REST: create catalog `demo` + schema `default` | 0.04s |
| Spark Connect container (pre-baked image, port open) | 0.42s |
| `SparkSession.builder.remote(...).create()` | ~0s (lazy) |
| **First query** (`SELECT 1`) | **18.9s** |
| Next query (`SELECT 1`) | 0.03s |

### First query per session (cold start)

| Session (fixture) | Image / version | 1st query | following |
|---|---|---|---|
| `spark` | `apache/spark:4.2.0-python3` | 5.6s | ~0.3s |
| `uc` | `spark-uc` (4.2 + Delta + UC) | 18.9s | 0.03–0.14s |
| `iceberg_uc` | `spark-iceberg-uc` (4.1 + Iceberg + UC) | 18.9s | 0.03–0.14s |
| `delta_iceberg_uc` | `spark-delta-iceberg-uc` (4.1 + Delta + Iceberg + UC) | ~6–10s | 0.03–1.4s |
| `spark_s3a` | `spark-uc` (4.2 + S3A) | ~6.6s | ~0.3s |

`spark_gold` and `pipeline_env` reuse the same image/pattern as `uc`
(UC server + REST bootstrap + Spark Connect on the Floci network) and pay
the same kind of cold start.

### Sum of the costs (full run)

- Cold starts: ~5.6 + ~6.6 + ~18.9 + ~10 + ~10 + ~18.9 + ~6–10 ≈
  **~75–90s**
- `floci`/`s3_*` fixtures: <1s
- `@uc_clean` (1–2 UC tests): `SHOW NAMESPACES`/`SHOW TABLES`/`DROP` per
  catalog → 0.3–1.6s per test
- Individual gRPC round trips: 0.03–1.4s (grows with the CPU contention
  when several JVMs run in parallel in the same suite)

---

## 2. Why is the first query so expensive?

Spark Connect is **lazy**: `builder.remote(...).create()` only connects
when the first command is executed. That first command creates the session
on the **server (JVM) side** — it loads classes, initializes the
Delta/Iceberg extensions and the UC connector (REST). The containers
themselves are cheap: warm Docker + pre-baked images = ~3s for the UC
server + Spark Connect pair.

There is no way to "warm up" the session without paying that cost: it is
per process/session of the client.

## 3. Why 7 environments (and not 1)?

Different versions and jars require different images/sessions — they
cannot be mixed in the same session:

- **Iceberg** only publishes a runtime for **Spark 4.1** (there is no 4.2
  yet) → two version lines (4.2 and 4.1) coexist in the suite;
- **Delta / Iceberg / UC** have pre-baked jars in separate images
  (jars in the image; `--packages` is an Ivy cache hit, no download);
- **Floci** emulates S3 (test bucket);
- **S3A** (`hadoop-aws`) requires its own session with the
  endpoint/credentials configured (`spark_s3a` reuses the `spark-uc`
  image — the jar is already in the Ivy cache);
- The **jobs/pipeline** emulations (`spark_gold`, `pipeline_env`) each
  need their own UC server + catalogs (`gold`, `sales`).

That is why the UC/Iceberg groups use `pytest.mark.timeout(180)`: the
fixture setup (cold start) is embedded in the time of the **first** test
of each group.

## 4. How to check again

```bash
# Duration per phase (call/setup/teardown) of each test
uv run pytest -v --durations=30

# Instrument the fixture (temporary, in `tests/infra.py`):
#   a print per stage in `_uc_spark_session` / `start_spark` (stderr)
# + a temporary test that times `sql(...).collect()` sequentially.
```

## 5. Can it be reduced?

| Option | Effect |
|---|---|
| Run a subset: `uv run pytest tests/test_capitalize.py` | ~7s (1 environment, 1 cold start) |
| Parallelize per group with `pytest-xdist` (1 worker per module/group) | suite ~167s → ~40–50s; the containers are independent (different ports); cost: RAM (~7 JVMs + UC servers simultaneously) |
| Warm the session in the fixture | **does not** change the wall time — it only moves the cost from the test to the fixture |
| Reduce the cold start | intrinsic to Spark Connect + the extensions (JVM + Delta/Iceberg/UC); only a different architecture would change it |

**Recommendation:** accept ~167s for the full suite (it is the cost of
the version/jars matrix); for local iteration, run the subset of the
module in question.

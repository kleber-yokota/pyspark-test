"""Local integration for the Databricks jobs (bundle ``databricks.yml``).

The test is **driven by the yml**: it reads a job from ``resources.jobs``
and runs the flow locally against a real Spark (Docker), with the same
semantics as Databricks: a task only runs after **all** of the tasks it
depends on have finished. If the yml changes (new task, ordering, missing
dependency, or cycle), the test follows along or fails with a clear error.
The orchestrator (``Orchestrator``) and the task executor (``run_task``)
live in ``tests/orchestrator.py``.

Each bundle job becomes one test instance (parametrized ``job_source``
fixture — run a single one with ``pytest -k <job_key>``): the fixture seeds
the bucket, under the ``<job>/`` prefix, with the files referenced in the
job's task parameters, and the flow that runs is only the one for that
job. The Spark session comes from the ``job_session`` fixture: pure S3A
for most jobs, with the ``gold`` catalog (Unity Catalog) only for the job
that writes to it (``names``).

Task conventions:

- ``spark_python_task``: the file exposes ``main(session, source, catalog,
  schema)``; the ``__main__`` block (what Databricks executes) calls
  ``main`` with the source from the task parameter (``sys.argv``);
- ``sql_task`` (``source: WORKSPACE``): the SQL file runs as-is on the
  local session — unqualified names resolve against the session's default
  catalog (``spark_catalog`` locally; ``dev`` on Databricks); the ``gold``
  catalog (Unity Catalog) exists on both sides.

What is left to the workspace (not emulated here): serverless compute, the
real UC catalogs, and the serverless SQL warehouse.
"""

from __future__ import annotations

from pathlib import Path

import pytest
import yaml
from orchestrator import BUNDLE_YML, DATA_DIR, Orchestrator


def _jobs() -> dict:
    data = yaml.safe_load(BUNDLE_YML.read_text())
    return data.get("resources", {}).get("jobs", {})


JOBS = _jobs()
assert JOBS, "the bundle defines no jobs in resources.jobs"


@pytest.fixture(params=sorted(JOBS), ids=sorted(JOBS))
def job_source(s3_client, s3_bucket, request) -> tuple[str, str]:
    """Seeds this job's raw data into the bucket; returns ``(job_key, prefix)``.

    For each ``spark_python_task`` of the job, seeds into the bucket
    (prefix ``<job>/``) the ``tests/data/`` file indicated by the task's
    first parameter — the prefix is the ``s3a://`` root the flow reads.
    """
    job_key = request.param
    for task in JOBS[job_key]["tasks"]:
        params = task.get("spark_python_task", {}).get("parameters", [])
        if not params:
            continue
        name = Path(params[0]).name
        s3_client.put_object(
            Bucket=s3_bucket,
            Key=f"{job_key}/{name}",
            Body=(DATA_DIR / name).read_bytes(),
        )
    return job_key, f"s3a://{s3_bucket}/{job_key}"


# Jobs that write to the ``gold`` catalog (Unity Catalog) — the others run
# on the pure S3A session.
GOLD_JOBS = {"names"}


@pytest.fixture
def job_session(job_source, request):
    """Session for the job: ``spark_gold`` (with the UC catalogs) for
    ``GOLD_JOBS``, ``spark_s3a`` (pure) for the rest — resolved here only.
    """
    job_key, _ = job_source
    name = "spark_gold" if job_key in GOLD_JOBS else "spark_s3a"
    yield request.getfixturevalue(name)


def test_job_runs_locally(job_session, job_source) -> None:
    """Runs the flow for ONLY this job (in the graph order of the yml)."""
    job_key, prefix = job_source
    job = JOBS[job_key]
    tasks = job["tasks"]

    # Structure: a referenced environment_key must exist in the job.
    environments = {e["environment_key"] for e in job.get("environments", [])}
    for task in tasks:
        environment_key = task.get("environment_key")
        if environment_key is not None:
            assert environment_key in environments, (
                f"task {task['task_key']!r} references environment_key "
                f"{environment_key!r} which does not exist in the job"
            )

    Orchestrator(tasks, job_session, prefix).run_job()

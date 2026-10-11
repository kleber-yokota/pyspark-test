"""Local job orchestrator for the Databricks bundle (``databricks.yml``).

Runs a job's tasks against a local Spark session following the
``depends_on`` graph: a task runs only after all of its dependencies.
Used by ``test_job_bundle.py``; unit-tested in ``test_orchestrator.py``.
"""

from __future__ import annotations

import importlib
from pathlib import Path

BUNDLE_YML = Path(__file__).resolve().parent.parent / "databricks.yml"
# Raw flow data (the single source of truth; seeded into the bucket per job).
DATA_DIR = Path(__file__).resolve().parent / "data"
# Local catalog (Docker Spark). SQL files use unqualified names, so they
# resolve against the session's default catalog (``spark_catalog`` here,
# ``dev`` on Databricks) and run as-is in both.
LOCAL_CATALOG = "spark_catalog"


class Orchestrator:
    """Runs a job's tasks following the ``depends_on`` graph from the yml.

    The graph is built from the yml (one node per task, edges per
    ``depends_on``). A task is **ready** once all of its dependencies have
    been executed; the executor waits for the dependencies (readiness loop)
    and only then runs the task — the same semantics as the job on
    Databricks.
    """

    def __init__(self, tasks: list[dict], session, source: str):
        self.session = session
        self.source = source
        self.graph = self._build_graph(tasks)
        self.executed: set[str] = set()

    @staticmethod
    def _build_graph(tasks: list[dict]) -> dict[str, dict]:
        """Job graph: ``task_key -> {"task": dict, "deps": [task_key]}``."""
        by_key = {t["task_key"]: t for t in tasks}
        graph: dict[str, dict] = {}
        for task in tasks:
            deps = [dep["task_key"] for dep in task.get("depends_on", [])]
            for dep in deps:
                if dep not in by_key:
                    raise ValueError(f"task missing from the yml: {dep!r}")
            graph[task["task_key"]] = {"task": task, "deps": deps}
        return graph

    def _ready(self, key: str) -> bool:
        """A task can run once all of its dependencies have been executed."""
        return all(dep in self.executed for dep in self.graph[key]["deps"])

    def run_job(self) -> None:
        """Waits and runs the job: at each step, runs whatever is ready."""
        pending = set(self.graph)
        while pending:
            ready = [key for key in pending if self._ready(key)]
            if not ready:
                raise ValueError(f"dependency cycle between: {sorted(pending)}")
            for key in ready:
                self._execute(key)
                pending.discard(key)

    def _execute(self, key: str) -> None:
        run_task(self.graph[key]["task"], self.session, self.source)
        self.executed.add(key)


def run_task(task: dict, session, source: str) -> None:
    """Runs a task locally against the container's Spark session.

    ``source`` is the s3a:// prefix where the job's raw files are seeded; a
    python task reads ``source/<basename of its first parameter>``.
    """
    key = task["task_key"]
    if "spark_python_task" in task:
        python_file = task["spark_python_task"]["python_file"]
        path = BUNDLE_YML.parent / python_file
        assert path.is_file(), f"python_file not found: {python_file}"
        module = importlib.import_module(
            python_file.removesuffix(".py").replace("/", ".")
        )
        params = task["spark_python_task"].get("parameters", [])
        assert params, f"task {key!r} has no source parameter"
        module.main(session, f"{source}/{Path(params[0]).name}", LOCAL_CATALOG)
    elif "sql_task" in task:
        spec = task["sql_task"]["file"]
        assert spec["source"] == "WORKSPACE", "only source WORKSPACE is supported"
        sql = (BUNDLE_YML.parent / spec["path"]).read_text()
        # Same semantics as the Databricks sql_task: the statement must run
        # without error (SELECT materializes; DDL like CREATE TABLE applies).
        # No row-count assertion — the file runs as-is, whatever it does.
        session.sql(sql).collect()
    else:
        types = [k for k in task if k.endswith("_task")]
        raise AssertionError(
            f"task {key!r} uses a type not supported by the local test: {types}"
        )

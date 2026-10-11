"""Unit tests for the ``Orchestrator`` (task graph from the yml) — no Spark/Docker.

The real executor (``orchestrator.run_task``) is replaced by a recorder:
what is verified here is the **orchestration** of the graph built from the
yml — which tasks run, how many times, in which order — for the shapes the
yml can describe:

- more than one entry (independent tasks at the start);
- more than one dependency (a task that depends on several — fan-in);
- more than one exit (terminal tasks — fan-out);
- combinations of the three (chain, diamond);
- tasks listed in the yml out of dependency order (disordered file — the
  execution order is still the graph's).

They also cover the failures: missing dependency and dependency cycle.
"""

from __future__ import annotations

import orchestrator
import pytest


def _task(key: str, *deps: str) -> dict:
    """Builds a task from the yml (``depends_on`` in Databricks format)."""
    task: dict = {"task_key": key}
    if deps:
        task["depends_on"] = [{"task_key": dep} for dep in deps]
    return task


@pytest.fixture
def run(monkeypatch):
    """Swaps the executor for a recorder.

    Returns ``run(tasks) -> order``, where ``order`` is the list of
    ``task_key`` in the order they were executed — the only observable
    effect of the orchestration.
    """
    order: list[str] = []
    monkeypatch.setattr(
        orchestrator,
        "run_task",
        lambda task, _session, _source: order.append(task["task_key"]),
    )

    def _run(tasks: list[dict]) -> list[str]:
        orchestrator.Orchestrator(
            tasks, session=None, source="s3a://b/job/orders.csv"
        ).run_job()
        return list(order)

    return _run


def test_single_entry_single_exit(run) -> None:
    """Job with a single task: it runs and the flow finishes."""
    assert run([_task("etl")]) == ["etl"]


def test_multiple_entries_each_run_once(run) -> None:
    """Independent tasks (entries): all run, each one once."""
    order = run([_task("a"), _task("b"), _task("c")])
    assert sorted(order) == ["a", "b", "c"]


def test_multiple_dependencies_run_before_the_task(run) -> None:
    """Task with several dependencies (fan-in): runs only after all of them."""
    order = run([_task("a"), _task("b"), _task("c"), _task("validate", "a", "b", "c")])
    assert order[-1] == "validate"
    assert sorted(order[:-1]) == ["a", "b", "c"]


def test_flow_runs_each_task_exactly_once(run) -> None:
    """Chain: the flow finishes and each task runs once, in graph order."""
    order = run([_task("a"), _task("b", "a"), _task("c", "b"), _task("d", "c")])
    assert sorted(order) == ["a", "b", "c", "d"]
    assert [order.index(t) for t in ("a", "b", "c", "d")] == [0, 1, 2, 3]


def test_multiple_exits_run_after_the_dependency(run) -> None:
    """Terminal tasks (exits): all after the dependency, each one once."""
    order = run(
        [_task("etl"), _task("v1", "etl"), _task("v2", "etl"), _task("v3", "etl")]
    )
    assert order[0] == "etl"
    assert sorted(order[1:]) == ["v1", "v2", "v3"]


def test_combination_of_entries_dependencies_and_exits(run) -> None:
    """Two entries -> fan-in -> two exits (combination of everything)."""
    order = run(
        [
            _task("a"),
            _task("b"),
            _task("c", "a", "b"),
            _task("d", "c"),
            _task("e", "c"),
        ]
    )
    assert sorted(order) == ["a", "b", "c", "d", "e"]
    assert order.index("a") < order.index("c")
    assert order.index("b") < order.index("c")
    assert order.index("c") < order.index("d")
    assert order.index("c") < order.index("e")


def test_yml_out_of_order_runs_in_graph_order(run) -> None:
    """The yml lists the dependent BEFORE the dependency (disordered file)."""
    order = run([_task("validate", "etl"), _task("etl")])
    assert order == ["etl", "validate"]


def test_diamond_with_shuffled_yml_order(run) -> None:
    """Full graph in random order in the yml: the dependencies hold."""
    order = run(
        [
            _task("d", "b", "c"),
            _task("c", "a"),
            _task("a"),
            _task("b", "a"),
        ]
    )
    assert order[0] == "a"
    assert order[-1] == "d"
    assert sorted(order[1:-1]) == ["b", "c"]


def test_missing_dependency_raises_clear_error() -> None:
    """``depends_on`` to an absent task fails when building the graph (in the yml)."""
    with pytest.raises(ValueError, match="task missing from the yml: 'ghost'"):
        orchestrator.Orchestrator(
            [_task("etl", "ghost")], session=None, source="s3a://b"
        )


def test_dependency_cycle_raises_error(run) -> None:
    """Cycle (a -> b -> a): the flow does not finish and the error is clear."""
    with pytest.raises(ValueError, match="dependency cycle"):
        run([_task("a", "b"), _task("b", "a")])


def test_self_dependency_raises_error(run) -> None:
    """A self-loop is a cycle too."""
    with pytest.raises(ValueError, match="dependency cycle"):
        run([_task("a", "a")])


def test_session_and_source_reach_the_executor(monkeypatch) -> None:
    """The orchestrator forwards ``session`` and ``source`` to each task."""
    seen: list[tuple] = []
    monkeypatch.setattr(
        orchestrator,
        "run_task",
        lambda task, session, source: seen.append((task["task_key"], session, source)),
    )
    session, source = object(), "s3a://b/job/orders.csv"
    orchestrator.Orchestrator(
        [_task("a")], session=session, source=source
    ).run_job()
    assert seen == [("a", session, source)]

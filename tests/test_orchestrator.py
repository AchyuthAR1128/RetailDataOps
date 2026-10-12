"""Focused tests for end-to-end pipeline orchestration."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import pytest

from retaildataops.orchestrator import (
    OrchestrationError,
    PipelinePaths,
    PipelineRequest,
    build_request,
    run_pipeline,
)


@dataclass
class _Result:
    returncode: int


def _request(tmp_path: Path, **kwargs: object) -> PipelineRequest:
    values: dict[str, object] = {
        "start_date": "2011-01-29",
        "end_date": "2011-02-04",
        "batch_id": None,
        "paths": PipelinePaths(),
        "retries": 0,
        "project_root": tmp_path,
        "python_executable": "python-test",
    }
    values.update(kwargs)
    return build_request(**values)  # type: ignore[arg-type]


def test_stage_order_and_argument_forwarding(tmp_path: Path) -> None:
    calls: list[list[str]] = []

    def runner(command: list[str], **_: object) -> _Result:
        calls.append(command)
        return _Result(0)

    request = _request(tmp_path, batch_id="batch_20110129_20110204")

    assert run_pipeline(request, runner=runner) == 0
    assert [Path(command[1]).name for command in calls] == [
        "run_replay.py",
        "run_bronze.py",
        "run_silver.py",
        "run_gold.py",
    ]
    assert "--batch-id" in calls[1]
    assert calls[1][calls[1].index("--batch-id") + 1] == request.batch_id
    assert calls[2][calls[2].index("--batch-id") + 1] == request.batch_id


def test_failure_stops_pipeline_and_returns_nonzero(tmp_path: Path) -> None:
    calls: list[str] = []

    def runner(command: list[str], **_: object) -> _Result:
        calls.append(Path(command[1]).name)
        return _Result(1 if len(calls) == 2 else 0)

    assert run_pipeline(_request(tmp_path), runner=runner) == 1
    assert calls == ["run_replay.py", "run_bronze.py"]


def test_retry_repeats_only_failed_stage(tmp_path: Path) -> None:
    calls: list[str] = []
    results = iter([1, 0, 0, 0, 0])

    def runner(command: list[str], **_: object) -> _Result:
        calls.append(Path(command[1]).name)
        return _Result(next(results))

    assert run_pipeline(_request(tmp_path, retries=1), runner=runner) == 0
    assert calls == [
        "run_replay.py",
        "run_replay.py",
        "run_bronze.py",
        "run_silver.py",
        "run_gold.py",
    ]


def test_invalid_batch_selection_is_rejected(tmp_path: Path) -> None:
    with pytest.raises(OrchestrationError, match="batch_id"):
        _request(tmp_path, batch_id="not-a-batch")

    with pytest.raises(OrchestrationError, match="Provide"):
        build_request(
            start_date=None,
            end_date=None,
            batch_id=None,
            paths=PipelinePaths(),
            retries=0,
            project_root=tmp_path,
        )

    with pytest.raises(OrchestrationError, match="after"):
        build_request(
            start_date="2011-02-04",
            end_date="2011-01-29",
            batch_id=None,
            paths=PipelinePaths(),
            retries=0,
            project_root=tmp_path,
        )


def test_dates_select_deterministic_batch(tmp_path: Path) -> None:
    request = _request(tmp_path)
    assert request.batch_id == "batch_20110129_20110204"

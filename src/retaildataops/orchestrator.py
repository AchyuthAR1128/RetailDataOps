"""End-to-end orchestration for the RetailDataOps medallion pipeline."""

from __future__ import annotations

import logging
import re
import subprocess
import sys
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import date, datetime
from pathlib import Path
from typing import Any

LOGGER = logging.getLogger(__name__)
_BATCH_ID_PATTERN = re.compile(r"^batch_(?P<start>\d{8})_(?P<end>\d{8})$")


class OrchestrationError(Exception):
    """Raised when pipeline inputs are invalid or a stage cannot run."""


@dataclass(frozen=True)
class PipelinePaths:
    """Filesystem locations used by the four pipeline stages."""

    raw_dir: Path = Path("data/raw")
    landing_dir: Path = Path("data/landing")
    bronze_dir: Path = Path("data/bronze")
    silver_dir: Path = Path("data/silver")
    gold_dir: Path = Path("data/gold")


@dataclass(frozen=True)
class PipelineRequest:
    """Validated request for one replay batch and its downstream stages."""

    start_date: str
    end_date: str
    batch_id: str
    paths: PipelinePaths = field(default_factory=PipelinePaths)
    retries: int = 0
    project_root: Path = Path(".")
    python_executable: str = sys.executable


def batch_id_to_dates(batch_id: str) -> tuple[str, str]:
    """Return ISO dates encoded in a deterministic replay batch ID."""
    match = _BATCH_ID_PATTERN.fullmatch(batch_id)
    if match is None:
        raise OrchestrationError("batch_id must match batch_YYYYMMDD_YYYYMMDD.")
    start = match.group("start")
    end = match.group("end")
    try:
        start_date = datetime.strptime(start, "%Y%m%d").date()
        end_date = datetime.strptime(end, "%Y%m%d").date()
    except ValueError as err:
        raise OrchestrationError(
            f"batch_id contains invalid dates: '{batch_id}'."
        ) from err
    if start_date > end_date:
        raise OrchestrationError(
            f"batch_id start date cannot follow end date: '{batch_id}'."
        )
    return start_date.isoformat(), end_date.isoformat()


def build_request(
    *,
    start_date: str | None,
    end_date: str | None,
    batch_id: str | None,
    paths: PipelinePaths,
    retries: int,
    project_root: Path,
    python_executable: str = sys.executable,
) -> PipelineRequest:
    """Validate CLI values and construct a pipeline request."""
    if retries < 0:
        raise OrchestrationError("retries must be zero or greater.")

    if batch_id is not None:
        inferred_start, inferred_end = batch_id_to_dates(batch_id)
        if start_date is not None and start_date != inferred_start:
            raise OrchestrationError(
                "start_date does not match the dates encoded in batch_id."
            )
        if end_date is not None and end_date != inferred_end:
            raise OrchestrationError(
                "end_date does not match the dates encoded in batch_id."
            )
        start_date = inferred_start
        end_date = inferred_end
    elif start_date is None or end_date is None:
        raise OrchestrationError(
            "Provide --batch-id or both --start-date and --end-date."
        )
    else:
        try:
            from retaildataops.replay import ReplaySimulator

            parsed_start = date.fromisoformat(start_date)
            parsed_end = date.fromisoformat(end_date)
        except ValueError as err:
            raise OrchestrationError(
                "start_date and end_date must use YYYY-MM-DD."
            ) from err
        if parsed_start > parsed_end:
            raise OrchestrationError("start_date cannot be after end_date.")
        expected_batch_id = ReplaySimulator.generate_batch_id(parsed_start, parsed_end)
        batch_id = expected_batch_id

    return PipelineRequest(
        start_date=start_date,
        end_date=end_date,
        batch_id=batch_id,
        paths=paths,
        retries=retries,
        project_root=project_root,
        python_executable=python_executable,
    )


def _stage_commands(request: PipelineRequest) -> list[tuple[str, list[str]]]:
    """Build commands for the stages without executing them."""
    scripts_dir = request.project_root / "scripts"
    path_args = request.paths
    return [
        (
            "Replay",
            [
                request.python_executable,
                str(scripts_dir / "run_replay.py"),
                "--start-date",
                request.start_date,
                "--end-date",
                request.end_date,
                "--raw-dir",
                str(path_args.raw_dir),
                "--landing-dir",
                str(path_args.landing_dir),
            ],
        ),
        (
            "Bronze",
            [
                request.python_executable,
                str(scripts_dir / "run_bronze.py"),
                "--batch-id",
                request.batch_id,
                "--landing-dir",
                str(path_args.landing_dir),
                "--bronze-dir",
                str(path_args.bronze_dir),
            ],
        ),
        (
            "Silver",
            [
                request.python_executable,
                str(scripts_dir / "run_silver.py"),
                "--batch-id",
                request.batch_id,
                "--bronze-dir",
                str(path_args.bronze_dir),
                "--landing-dir",
                str(path_args.landing_dir),
                "--silver-dir",
                str(path_args.silver_dir),
            ],
        ),
        (
            "Gold",
            [
                request.python_executable,
                str(scripts_dir / "run_gold.py"),
                "--silver-dir",
                str(path_args.silver_dir),
                "--gold-dir",
                str(path_args.gold_dir),
            ],
        ),
    ]


def run_pipeline(
    request: PipelineRequest,
    *,
    runner: Callable[..., Any] = subprocess.run,
) -> int:
    """Run all stages in order, stopping at the first failed stage.

    A failed stage is retried up to ``request.retries`` times. Retries invoke
    the same idempotent stage again and never remove or overwrite its outputs.
    """
    run_command = runner
    for stage_name, command in _stage_commands(request):
        attempts = request.retries + 1
        for attempt in range(1, attempts + 1):
            LOGGER.info(
                "Starting %s stage (attempt %d/%d).",
                stage_name,
                attempt,
                attempts,
            )
            try:
                completed = run_command(
                    command,
                    cwd=request.project_root,
                    check=False,
                )
                return_code = int(getattr(completed, "returncode", completed))
            except OSError as err:
                LOGGER.error("%s stage failed to start: %s", stage_name, err)
                return_code = 1

            if return_code == 0:
                LOGGER.info("%s stage succeeded.", stage_name)
                break

            LOGGER.error(
                "%s stage failed with exit code %d.",
                stage_name,
                return_code,
            )
            if attempt == attempts:
                return return_code or 1
        else:
            return 1
    LOGGER.info("RetailDataOps pipeline completed successfully.")
    return 0


def configure_logging() -> None:
    """Configure concise stage logging for the CLI."""
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")

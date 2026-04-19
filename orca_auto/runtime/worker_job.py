from __future__ import annotations

import argparse
import subprocess
import sys
from pathlib import Path
from typing import Type

from core.orca_runner import OrcaRunner

BackgroundRunJobProcess = subprocess.Popen


def _build_background_run_job_command(
    *,
    config_path: str,
    reaction_dir: str,
    force: bool = False,
    admission_token: str | None = None,
    admission_app_name: str | None = None,
    admission_task_id: str | None = None,
) -> list[str]:
    cmd = [
        sys.executable,
        "-m",
        "orca_auto.runtime.worker_job",
        "--config",
        config_path,
        "--reaction-dir",
        reaction_dir,
    ]
    if force:
        cmd.append("--force")
    if admission_token:
        cmd.extend(["--admission-token", admission_token])
    if admission_app_name:
        cmd.extend(["--admission-app-name", admission_app_name])
    if admission_task_id:
        cmd.extend(["--admission-task-id", admission_task_id])
    return cmd


def start_background_run_job(
    *,
    config_path: str,
    reaction_dir: str,
    force: bool = False,
    admission_token: str | None = None,
    admission_app_name: str | None = None,
    admission_task_id: str | None = None,
    runner_cls: Type[OrcaRunner] = OrcaRunner,
) -> BackgroundRunJobProcess[str]:
    if runner_cls is not OrcaRunner:
        raise ValueError("start_background_run_job only supports the default OrcaRunner")
    return subprocess.Popen(
        _build_background_run_job_command(
            config_path=config_path,
            reaction_dir=str(Path(reaction_dir)),
            force=force,
            admission_token=admission_token,
            admission_app_name=admission_app_name,
            admission_task_id=admission_task_id,
        ),
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        stdin=subprocess.DEVNULL,
        start_new_session=True,
        text=True,
    )


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="python -m orca_auto.runtime.worker_job")
    parser.add_argument("--config", required=True)
    parser.add_argument("--reaction-dir", required=True)
    parser.add_argument("--force", action="store_true")
    parser.add_argument("--admission-token", default=None)
    parser.add_argument("--admission-app-name", default=None)
    parser.add_argument("--admission-task-id", default=None)
    return parser


def main(argv: list[str] | None = None) -> int:
    from core.commands.run_job import execute_run_job

    args = build_parser().parse_args(argv)
    return execute_run_job(
        args.config,
        args.reaction_dir,
        force=bool(args.force),
        reservation_token=str(args.admission_token).strip() or None,
        admission_app_name=str(args.admission_app_name).strip() or None,
        admission_task_id=str(args.admission_task_id).strip() or None,
    )


if __name__ == "__main__":
    raise SystemExit(main())

from __future__ import annotations

import argparse
import os
import signal
from typing import Any

from .commands import queue as queue_cmd


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="python -m chemstack.xtb.worker_job")
    parser.add_argument("--config", required=True)
    parser.add_argument("--queue-root", required=True)
    parser.add_argument("--queue-id", required=True)
    parser.add_argument("--admission-root", required=True)
    parser.add_argument("--admission-token", default=None)
    parser.add_argument("--auto-organize", action="store_true")
    return parser


class _SignalController:
    def __init__(self) -> None:
        self._cancel_requested = False
        self._process: Any | None = None

    def should_cancel(self) -> bool:
        return self._cancel_requested

    def set_running_job(self, value: Any | None) -> None:
        if value is None:
            self._process = None
            return
        self._process = getattr(value, "process", value)

    def install(self) -> None:
        try:
            signal.signal(queue_cmd.WORKER_CANCEL_SIGNAL, self._handle_cancel)
            signal.signal(signal.SIGTERM, self._handle_shutdown)
            signal.signal(signal.SIGINT, self._handle_shutdown)
        except ValueError:
            pass

    def _handle_cancel(self, _signum: int, _frame: object) -> None:
        self._cancel_requested = True
        if self._process is not None:
            queue_cmd._terminate_process(self._process)

    def _handle_shutdown(self, _signum: int, _frame: object) -> None:
        if self._process is not None:
            queue_cmd._terminate_process(self._process)
        os._exit(queue_cmd.WORKER_SHUTDOWN_EXIT_CODE)


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    controller = _SignalController()
    controller.install()
    return queue_cmd.run_worker_job(
        config_path=args.config,
        queue_root=args.queue_root,
        queue_id=args.queue_id,
        admission_root=args.admission_root,
        admission_token=str(args.admission_token).strip() or None,
        auto_organize=bool(args.auto_organize),
        should_cancel=controller.should_cancel,
        register_running_job=controller.set_running_job,
    )


if __name__ == "__main__":
    raise SystemExit(main())

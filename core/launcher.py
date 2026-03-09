from __future__ import annotations

import os
import re
import subprocess
import sys
import time
from pathlib import Path
from typing import Sequence

from . import cli
from .commands._helpers import default_config_path


BACKGROUND_ENV_VAR = "ORCA_AUTO_RUN_INP_BACKGROUND"
LOG_DIR_ENV_VAR = "ORCA_AUTO_LOG_DIR"
_FALSEY_ENV_VALUES = {"0", "false", "no", "off"}
_LOG_LABEL_RE = re.compile(r"[^A-Za-z0-9._-]+")
_EARLY_EXIT_WAIT_SECONDS = 0.2


def _config_path_from_args(argv: Sequence[str]) -> str | None:
    args = list(argv)
    for idx, token in enumerate(args):
        if token == "--config" and idx + 1 < len(args):
            return args[idx + 1]
        if token.startswith("--config="):
            return token.split("=", 1)[1]
    return None


def _detect_command(argv: Sequence[str]) -> str | None:
    args = list(argv)
    idx = 0
    while idx < len(args):
        token = args[idx]
        if token == "--config":
            idx += 2
            continue
        if token.startswith("--config="):
            idx += 1
            continue
        if token in {"--verbose", "-v"}:
            idx += 1
            continue
        if token == "--":
            if idx + 1 < len(args):
                return args[idx + 1]
            return None
        if token.startswith("-"):
            idx += 1
            continue
        return token
    return None


def _background_requested_by_default() -> bool:
    raw = os.getenv(BACKGROUND_ENV_VAR, "1").strip()
    return raw.casefold() not in _FALSEY_ENV_VALUES


def _wants_background(argv: Sequence[str]) -> bool:
    if _detect_command(argv) != "run-inp":
        return False

    want_background = _background_requested_by_default()
    for token in argv:
        if token in {"-h", "--help"}:
            return False
        if token == "--foreground":
            want_background = False
    return want_background


def _reaction_dir_from_args(argv: Sequence[str]) -> str:
    args = list(argv)
    for idx, token in enumerate(args):
        if token == "--reaction-dir" and idx + 1 < len(args):
            return args[idx + 1]
        if token.startswith("--reaction-dir="):
            return token.split("=", 1)[1]
    return ""


def _default_log_dir(argv: Sequence[str]) -> Path:
    env_path = os.getenv(LOG_DIR_ENV_VAR, "").strip()
    if env_path:
        return Path(env_path).expanduser().resolve()

    config_text = _config_path_from_args(argv) or default_config_path()
    config_path = Path(config_text).expanduser().resolve()
    if config_path.parent.name == "config":
        return config_path.parent.parent / "logs"
    return Path.home() / "orca_auto" / "logs"


def _sanitize_log_label(raw: str) -> str:
    label = _LOG_LABEL_RE.sub("", raw)
    return label or "runinp"


def _build_log_file(argv: Sequence[str]) -> Path:
    reaction_dir = _reaction_dir_from_args(argv)
    label = "runinp"
    if reaction_dir:
        label = _sanitize_log_label(Path(reaction_dir).name)

    log_dir = _default_log_dir(argv)
    log_dir.mkdir(parents=True, exist_ok=True)
    timestamp = time.strftime("%Y%m%d_%H%M%S")
    return log_dir / f"run_inp_{timestamp}_{label}.log"


def _tail_log_lines(log_file: Path, *, limit: int = 20) -> list[str]:
    try:
        return log_file.read_text(encoding="utf-8", errors="ignore").splitlines()[-limit:]
    except OSError:
        return []


def _print_status(status: str, pid: int | str, log_file: Path) -> None:
    print(f"status: {status}")
    print(f"pid: {pid}")
    print(f"log: {log_file}")


def _run_in_background(argv: Sequence[str]) -> int:
    args = list(argv)
    log_file = _build_log_file(args)
    command = [sys.executable, "-m", "core.cli", *args]

    try:
        with log_file.open("w", encoding="utf-8") as handle:
            proc = subprocess.Popen(
                command,
                stdout=handle,
                stderr=subprocess.STDOUT,
                stdin=subprocess.DEVNULL,
                text=True,
                start_new_session=True,
                env=os.environ.copy(),
            )
    except OSError as exc:
        try:
            log_file.write_text(f"[orca_auto] failed to start background process: {exc}\n", encoding="utf-8")
        except OSError:
            pass
        _print_status("failed_early", "unavailable", log_file)
        return 1

    time.sleep(_EARLY_EXIT_WAIT_SECONDS)
    if proc.poll() is None:
        _print_status("started", proc.pid, log_file)
        return 0

    return_code = proc.wait()
    if return_code == 0:
        _print_status("finished", proc.pid, log_file)
        return 0

    _print_status("failed_early", proc.pid, log_file)
    tail_lines = _tail_log_lines(log_file)
    if tail_lines:
        print("last_log_lines:")
        for line in tail_lines:
            print(line)
    return 1


def main(argv: Sequence[str] | None = None) -> int:
    args = list(sys.argv[1:] if argv is None else argv)
    if _wants_background(args):
        return _run_in_background(args)
    return int(cli.main(args))


if __name__ == "__main__":
    raise SystemExit(main())

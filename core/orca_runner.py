from __future__ import annotations

import logging
import os
import signal
import subprocess
from dataclasses import dataclass
from pathlib import Path
from typing import List

logger = logging.getLogger(__name__)


@dataclass
class RunResult:
    out_path: str
    return_code: int


class OrcaRunner:
    def __init__(self, orca_executable: str) -> None:
        self.orca_executable = orca_executable

    def _terminate_subprocess_tree(self, proc: subprocess.Popen) -> None:
        if proc.poll() is not None:
            return
        logger.warning("Terminating ORCA process tree (pid=%d)", proc.pid)
        try:
            os.killpg(proc.pid, signal.SIGTERM)
        except Exception:
            try:
                proc.terminate()
            except Exception:
                pass

        try:
            proc.wait(timeout=3)
        except Exception:
            try:
                os.killpg(proc.pid, signal.SIGKILL)
            except Exception:
                try:
                    proc.kill()
                except Exception:
                    pass

    def run(self, inp_path: Path) -> RunResult:
        inp = inp_path.resolve()
        out = inp.with_suffix(".out")
        cwd = str(inp.parent)

        command: List[str] = [self.orca_executable, inp.name]
        logger.info("Running ORCA: %s in %s", command, cwd)

        return_code = 1
        with out.open("w", encoding="utf-8") as handle:
            proc = subprocess.Popen(
                command,
                cwd=cwd,
                stdout=handle,
                stderr=subprocess.STDOUT,
                text=True,
                start_new_session=True,
            )
            try:
                return_code = proc.wait()
            except KeyboardInterrupt:
                handle.write("\n[orca_auto] interrupted by user; terminating ORCA process tree\n")
                handle.flush()
                self._terminate_subprocess_tree(proc)
                raise
        return RunResult(out_path=str(out), return_code=return_code)

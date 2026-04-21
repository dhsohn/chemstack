from __future__ import annotations

import fcntl
import sys
from multiprocessing import get_context
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from chemstack.core.utils.lock import file_lock


def _hold_lock_until_released(lock_path: str, ready, release) -> None:
    with file_lock(Path(lock_path), timeout_seconds=1.0):
        ready.set()
        release.wait(5)


def test_file_lock_writes_metadata(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    lock_path = tmp_path / "nested" / "resource.lock"
    monkeypatch.setattr("chemstack.core.utils.lock.os.getpid", lambda: 4321)
    monkeypatch.setattr("chemstack.core.utils.lock.now_utc_iso", lambda: "2026-04-19T12:34:56+00:00")

    with file_lock(lock_path):
        contents = lock_path.read_text(encoding="utf-8")

    assert lock_path.parent.exists()
    assert contents == "pid=4321\nacquired_at=2026-04-19T12:34:56+00:00\n"


def test_file_lock_times_out_when_lock_is_held(tmp_path: Path) -> None:
    ctx = get_context("fork")
    lock_path = tmp_path / "held.lock"
    ready = ctx.Event()
    release = ctx.Event()
    process = ctx.Process(
        target=_hold_lock_until_released,
        args=(str(lock_path), ready, release),
    )
    process.start()

    try:
        assert ready.wait(5), "child process never acquired the lock"

        with pytest.raises(TimeoutError, match=r"Timed out acquiring lock: .*held\.lock"):
            with file_lock(lock_path, timeout_seconds=0.05):
                pass
    finally:
        release.set()
        process.join(timeout=5)
        if process.is_alive():
            process.terminate()
            process.join(timeout=5)


def test_file_lock_ignores_unlock_oserror(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    lock_path = tmp_path / "resource.lock"
    calls: list[int] = []

    def fake_flock(fd: int, flags: int) -> None:
        calls.append(flags)
        if flags & fcntl.LOCK_UN:
            raise OSError("unlock failed")

    monkeypatch.setattr("chemstack.core.utils.lock.fcntl.flock", fake_flock)

    with file_lock(lock_path):
        pass

    assert fcntl.LOCK_UN in calls

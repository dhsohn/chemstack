"""DFT monitor 테스트."""

from __future__ import annotations

import json
import os
from pathlib import Path

from core.dft_index import DFTIndex
from core.dft_monitor import DFTMonitor


_COMPLETED_OUT = "\n".join([
    "! B3LYP def2-SVP Opt",
    "* xyz 0 1",
    "C 0.0 0.0 0.0",
    "H 0.0 0.0 1.0",
    "*",
    "",
    "CARTESIAN COORDINATES (ANGSTROEM)",
    "----------------------------",
    " C    0.000000    0.000000    0.000000",
    " H    0.000000    0.000000    1.000000",
    "",
    "FINAL SINGLE POINT ENERGY      -100.123456789",
    "",
    "                             ****ORCA TERMINATED NORMALLY****",
    "TOTAL RUN TIME: 0 days 0 hours 1 minutes 2 seconds 3 msec",
])

_RUNNING_OPT_OUT = "\n".join([
    "! B3LYP def2-SVP Opt",
    "* xyz 0 1",
    "C 0.0 0.0 0.0",
    "H 0.0 0.0 1.0",
    "*",
    "",
    "CARTESIAN COORDINATES (ANGSTROEM)",
    "----------------------------",
    " C    0.000000    0.000000    0.000000",
    " H    0.000000    0.000000    1.000000",
    "",
    "---------------------------------------------------",
    "| Geometry Optimization Cycle   1                 |",
    "---------------------------------------------------",
    "",
    "FINAL SINGLE POINT ENERGY      -100.100000000",
])


def _make_index(tmp_path: Path) -> DFTIndex:
    db_path = str(tmp_path / "dft.db")
    index = DFTIndex()
    index.initialize(db_path)
    return index


def test_baseline_seed_prevents_restart_spam(tmp_path: Path) -> None:
    kb_dir = tmp_path / "kb"
    kb_dir.mkdir(parents=True)
    out_file = kb_dir / "calc.out"
    out_file.write_text(_COMPLETED_OUT, encoding="utf-8")
    (kb_dir / "run_state.json").write_text('{"status": "completed"}', encoding="utf-8")

    state_file = str(tmp_path / "automation" / "dft_monitor_state.json")
    index = _make_index(tmp_path)

    monitor = DFTMonitor(index, [str(kb_dir)], state_file=state_file)

    # 첫 실행: baseline만 기록
    report1 = monitor.scan()
    assert report1.new_results == []
    assert report1.baseline_seeded is True
    assert Path(state_file).is_file()

    # 재시작(새 인스턴스) 후 동일 파일 재알림 없음
    monitor2 = DFTMonitor(index, [str(kb_dir)], state_file=state_file)
    report2 = monitor2.scan()
    assert report2.new_results == []

    # 파일 변경 시 알림
    out_file.write_text(_COMPLETED_OUT + "\n# changed\n", encoding="utf-8")
    mtime = os.path.getmtime(out_file)
    os.utime(out_file, (mtime + 5.0, mtime + 5.0))

    report3 = monitor2.scan()
    assert len(report3.new_results) == 1
    assert report3.new_results[0].status == "completed"

    index.close()


def test_running_calc_not_indexed(tmp_path: Path) -> None:
    kb_dir = tmp_path / "kb"
    kb_dir.mkdir(parents=True)
    out_file = kb_dir / "running.out"
    out_file.write_text(_RUNNING_OPT_OUT, encoding="utf-8")
    (kb_dir / "run_state.json").write_text('{"status": "running"}', encoding="utf-8")

    state_file = str(tmp_path / "automation" / "state.json")
    index = _make_index(tmp_path)

    monitor = DFTMonitor(index, [str(kb_dir)], state_file=state_file)
    monitor.scan()  # baseline

    out_file.write_text(_RUNNING_OPT_OUT + "\n# updated\n", encoding="utf-8")
    mtime = os.path.getmtime(out_file)
    os.utime(out_file, (mtime + 5.0, mtime + 5.0))

    report = monitor.scan()
    assert len(report.new_results) == 1
    assert report.new_results[0].status == "running"
    # running 계산은 인덱스에 저장되지 않아야 함
    assert index._count() == 0

    index.close()


def test_symlink_dedup(tmp_path: Path) -> None:
    kb_dir = tmp_path / "kb"
    run_dir = kb_dir / "run_dir"
    run_dir.mkdir(parents=True)
    alias_dir = tmp_path / "run_alias"
    alias_dir.symlink_to(run_dir, target_is_directory=True)

    out_file = run_dir / "running.out"
    out_file.write_text(_RUNNING_OPT_OUT, encoding="utf-8")
    (run_dir / "run_state.json").write_text('{"status": "running"}', encoding="utf-8")

    state_file = str(tmp_path / "automation" / "state.json")
    index = _make_index(tmp_path)

    # alias 경로로 먼저 baseline
    monitor1 = DFTMonitor(index, [str(alias_dir)], state_file=state_file)
    monitor1.scan()

    # 실제 경로로 재시작 — 중복 알림 없어야 함
    monitor2 = DFTMonitor(index, [str(run_dir)], state_file=state_file)
    report = monitor2.scan()
    assert report.new_results == []

    index.close()

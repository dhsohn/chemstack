"""DFT index tests."""

from __future__ import annotations

import json
import threading
from pathlib import Path

from core.dft_index import DFTIndex


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


def _setup_kb(tmp_path: Path) -> Path:
    """Create ORCA output + run_state.json in a test KB directory."""
    kb_dir = tmp_path / "orca_runs" / "job1"
    kb_dir.mkdir(parents=True)
    (kb_dir / "calc.out").write_text(_COMPLETED_OUT, encoding="utf-8")
    (kb_dir / "run_state.json").write_text(
        json.dumps({"status": "completed"}),
        encoding="utf-8",
    )
    return tmp_path / "orca_runs"


def test_initialize_creates_db(tmp_path: Path) -> None:
    db_path = str(tmp_path / "dft.db")
    index = DFTIndex()
    index.initialize(db_path)
    assert Path(db_path).is_file()
    index.close()


def test_index_calculations(tmp_path: Path) -> None:
    kb_dir = _setup_kb(tmp_path)
    db_path = str(tmp_path / "dft.db")

    index = DFTIndex()
    index.initialize(db_path)

    result = index.index_calculations([str(kb_dir)])
    assert result["indexed"] == 1
    assert result["total"] == 1
    assert result["failed"] == 0

    # Re-indexing — skip if unchanged
    result2 = index.index_calculations([str(kb_dir)])
    assert result2["indexed"] == 0
    assert result2["skipped"] == 1
    assert result2["total"] == 1

    index.close()


def test_upsert_single(tmp_path: Path) -> None:
    kb_dir = _setup_kb(tmp_path)
    db_path = str(tmp_path / "dft.db")
    out_path = str(kb_dir / "job1" / "calc.out")

    index = DFTIndex()
    index.initialize(db_path)

    assert index.upsert_single(out_path) is True
    results = index.query({})
    assert len(results) == 1
    assert results[0]["method"] == "B3LYP"
    assert results[0]["formula"] == "CH"

    index.close()


def test_query_filters(tmp_path: Path) -> None:
    kb_dir = _setup_kb(tmp_path)
    db_path = str(tmp_path / "dft.db")

    index = DFTIndex()
    index.initialize(db_path)
    index.index_calculations([str(kb_dir)])

    # Method filter
    assert len(index.query({"method": "B3LYP"})) == 1
    assert len(index.query({"method": "PBE0"})) == 0

    # Formula search
    assert len(index.search_by_formula("CH")) == 1
    assert len(index.search_by_formula("XYZ")) == 0

    index.close()


def test_get_stats(tmp_path: Path) -> None:
    kb_dir = _setup_kb(tmp_path)
    db_path = str(tmp_path / "dft.db")

    index = DFTIndex()
    index.initialize(db_path)
    index.index_calculations([str(kb_dir)])

    stats = index.get_stats()
    assert stats["total"] == 1
    assert "completed" in stats["by_status"]
    assert "B3LYP" in stats["by_method"]

    index.close()


def test_concurrent_read_write(tmp_path: Path) -> None:
    """Verify that concurrent reads and writes don't raise errors."""
    kb_dir = _setup_kb(tmp_path)
    db_path = str(tmp_path / "dft.db")

    index = DFTIndex()
    index.initialize(db_path)
    index.index_calculations([str(kb_dir)])

    errors: list[Exception] = []

    def reader() -> None:
        try:
            for _ in range(20):
                index.query({"method": "B3LYP"})
                index.get_stats()
        except Exception as exc:
            errors.append(exc)

    def writer() -> None:
        try:
            out_path = str(kb_dir / "job1" / "calc.out")
            for _ in range(10):
                index.upsert_single(out_path)
        except Exception as exc:
            errors.append(exc)

    threads = [
        threading.Thread(target=reader),
        threading.Thread(target=reader),
        threading.Thread(target=writer),
    ]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    assert errors == [], f"Concurrent access errors: {errors}"
    index.close()


def test_removed_file_is_cleaned_from_index(tmp_path: Path) -> None:
    kb_dir = _setup_kb(tmp_path)
    db_path = str(tmp_path / "dft.db")

    index = DFTIndex()
    index.initialize(db_path)
    index.index_calculations([str(kb_dir)])
    assert index._count() == 1

    # Re-index after deleting file
    (kb_dir / "job1" / "calc.out").unlink()
    (kb_dir / "job1" / "run_state.json").unlink()
    result = index.index_calculations([str(kb_dir)])
    assert result["removed"] == 1
    assert result["total"] == 0

    index.close()

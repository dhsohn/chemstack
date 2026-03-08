"""DFT 인덱스 테스트."""

from __future__ import annotations

import json
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
    """테스트용 KB 디렉토리에 ORCA 출력 + run_state.json을 생성한다."""
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

    # 재인덱싱 — 변경 없으면 skip
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

    # method 필터
    assert len(index.query({"method": "B3LYP"})) == 1
    assert len(index.query({"method": "PBE0"})) == 0

    # formula 검색
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


def test_removed_file_is_cleaned_from_index(tmp_path: Path) -> None:
    kb_dir = _setup_kb(tmp_path)
    db_path = str(tmp_path / "dft.db")

    index = DFTIndex()
    index.initialize(db_path)
    index.index_calculations([str(kb_dir)])
    assert index._count() == 1

    # 파일 삭제 후 재인덱싱
    (kb_dir / "job1" / "calc.out").unlink()
    (kb_dir / "job1" / "run_state.json").unlink()
    result = index.index_calculations([str(kb_dir)])
    assert result["removed"] == 1
    assert result["total"] == 0

    index.close()

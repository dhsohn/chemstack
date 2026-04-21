# ruff: noqa: E402

from __future__ import annotations

import json
import sys
from pathlib import Path
from typing import cast

import pytest

REPO_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO_ROOT))
sys.path.insert(0, str(REPO_ROOT / "src"))

from chemstack.flow.adapters import orca as orca_adapter


def test_normalize_bool_and_safe_int_cover_string_and_default_paths() -> None:
    assert orca_adapter._normalize_bool(True) is True
    assert orca_adapter._normalize_bool(" yes ") is True
    assert orca_adapter._normalize_bool("off") is False

    assert orca_adapter._safe_int("12") == 12
    assert orca_adapter._safe_int("bad", default=7) == 7
    assert orca_adapter._safe_int(None, default=9) == 9


def test_load_json_dict_and_list_handle_missing_invalid_and_type_filtered_payloads(tmp_path: Path) -> None:
    missing = tmp_path / "missing.json"
    assert orca_adapter._load_json_dict(missing) == {}
    assert orca_adapter._load_json_list(missing) == []

    invalid = tmp_path / "invalid.json"
    invalid.write_text("{not-json", encoding="utf-8")
    assert orca_adapter._load_json_dict(invalid) == {}
    assert orca_adapter._load_json_list(invalid) == []

    wrong_type = tmp_path / "wrong-type.json"
    wrong_type.write_text(json.dumps(["x", {"ok": True}]), encoding="utf-8")
    assert orca_adapter._load_json_dict(wrong_type) == {}
    assert orca_adapter._load_json_list(wrong_type) == [{"ok": True}]

    dict_payload = tmp_path / "dict.json"
    dict_payload.write_text(json.dumps({"status": "ok"}), encoding="utf-8")
    assert orca_adapter._load_json_dict(dict_payload) == {"status": "ok"}
    assert orca_adapter._load_json_list(dict_payload) == []


def test_load_jsonl_records_skips_blank_invalid_and_non_dict_rows(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    records_path = tmp_path / "records.jsonl"
    records_path.write_text('\n{"queue_id":"q1"}\n42\nnot-json\n{"queue_id":"q2"}\n', encoding="utf-8")
    assert orca_adapter._load_jsonl_records(records_path) == [{"queue_id": "q1"}, {"queue_id": "q2"}]

    class _BrokenPath:
        def exists(self) -> bool:
            return True

        def read_text(self, encoding: str = "utf-8") -> str:
            raise OSError("boom")

    assert orca_adapter._load_jsonl_records(cast(Path, _BrokenPath())) == []

    monkeypatch.setattr(
        orca_adapter,
        "json",
        type(
            "_JSONStub",
            (),
            {
                "loads": staticmethod(orca_adapter.json.loads),
                "JSONDecodeError": orca_adapter.json.JSONDecodeError,
            },
        )(),
    )
    assert orca_adapter._load_jsonl_records(records_path) == [{"queue_id": "q1"}, {"queue_id": "q2"}]


def test_import_orca_auto_module_returns_none_or_raises_by_error_origin(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    repo_root = tmp_path / "chemstack"
    repo_root.mkdir()
    monkeypatch.setattr(orca_adapter, "_sibling_orca_auto_repo_root", lambda: repo_root)

    calls: list[str] = []

    def missing_then_missing(name: str) -> object:
        calls.append(name)
        raise ModuleNotFoundError(name=name)

    monkeypatch.setattr(orca_adapter, "import_module", missing_then_missing)
    original_sys_path = list(orca_adapter.sys.path)
    try:
        assert orca_adapter._import_orca_auto_module("chemstack.orca.tracking") is None
        assert calls == ["chemstack.orca.tracking", "chemstack.orca.tracking"]
        assert str(repo_root) in orca_adapter.sys.path
    finally:
        orca_adapter.sys.path[:] = original_sys_path

    def unrelated_missing(name: str) -> object:
        raise ModuleNotFoundError(name="different_module")

    monkeypatch.setattr(orca_adapter, "import_module", unrelated_missing)
    with pytest.raises(ModuleNotFoundError) as excinfo:
        orca_adapter._import_orca_auto_module("chemstack.orca.tracking")
    assert excinfo.value.name == "different_module"


def test_resolve_candidate_path_and_direct_dir_target_cover_existing_and_oserror_paths(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    xyz_path = tmp_path / "candidate.xyz"
    xyz_path.write_text("2\ncomment\nH 0 0 0\nH 0 0 0.7\n", encoding="utf-8")
    assert orca_adapter._resolve_candidate_path(str(xyz_path)) == xyz_path.resolve()
    assert orca_adapter._resolve_candidate_path("   ") is None
    assert orca_adapter._direct_dir_target(str(xyz_path)) == xyz_path.parent.resolve()
    assert orca_adapter._direct_dir_target(str(tmp_path)) == tmp_path.resolve()
    assert orca_adapter._direct_dir_target(str(tmp_path / "missing")) is None

    class _ExpandUserBroken:
        def expanduser(self) -> "_ExpandUserBroken":
            raise OSError("bad path")

    monkeypatch.setattr(orca_adapter, "Path", lambda raw: _ExpandUserBroken())
    assert orca_adapter._resolve_candidate_path("/tmp/ignored") is None
    assert orca_adapter._direct_dir_target("/tmp/ignored") is None

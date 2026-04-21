from __future__ import annotations

import os
from pathlib import Path

import pytest

from chemstack.core.config.schema import CommonRuntimeConfig

from chemstack.crest.commands import _helpers
from chemstack.crest.config import AppConfig


def _cfg(tmp_path: Path) -> AppConfig:
    allowed_root = tmp_path / "allowed_root"
    organized_root = tmp_path / "organized_root"
    allowed_root.mkdir()
    organized_root.mkdir()
    return AppConfig(
        runtime=CommonRuntimeConfig(
            allowed_root=str(allowed_root),
            organized_root=str(organized_root),
        )
    )


def _write_xyz(path: Path, comment: str = "test") -> None:
    path.write_text(f"1\n{comment}\nH 0.0 0.0 0.0\n", encoding="utf-8")


def _set_mtime(path: Path, *, seconds: int) -> None:
    stamp_ns = seconds * 1_000_000_000
    os.utime(path, ns=(stamp_ns, stamp_ns))


def test_load_job_manifest_returns_empty_dict_when_manifest_is_missing(tmp_path: Path) -> None:
    assert _helpers.load_job_manifest(tmp_path) == {}


def test_load_job_manifest_reads_yaml_mapping(tmp_path: Path) -> None:
    manifest_path = tmp_path / _helpers.MANIFEST_FILE_NAME
    manifest_path.write_text("mode: nci\ninput_xyz: picked.xyz\n", encoding="utf-8")

    manifest = _helpers.load_job_manifest(tmp_path)

    assert manifest == {"mode": "nci", "input_xyz": "picked.xyz"}


def test_load_job_manifest_rejects_non_mapping_yaml(tmp_path: Path) -> None:
    manifest_path = tmp_path / _helpers.MANIFEST_FILE_NAME
    manifest_path.write_text("- not\n- a\n- mapping\n", encoding="utf-8")

    with pytest.raises(ValueError, match="Invalid CREST job manifest"):
        _helpers.load_job_manifest(tmp_path)


@pytest.mark.parametrize(
    ("manifest", "expected"),
    [
        ({}, "standard"),
        ({"mode": " NCI "}, "nci"),
        ({"mode": "fast"}, "standard"),
    ],
)
def test_job_mode_normalizes_mode_values(manifest: dict[str, object], expected: str) -> None:
    assert _helpers.job_mode(manifest) == expected


def test_select_latest_xyz_prefers_non_generated_candidates(tmp_path: Path) -> None:
    generated = tmp_path / "crest_best.xyz"
    selected = tmp_path / "molecule.xyz"
    _write_xyz(generated, "generated")
    _write_xyz(selected, "selected")
    _set_mtime(selected, seconds=10)
    _set_mtime(generated, seconds=20)

    latest = _helpers.select_latest_xyz(tmp_path)

    assert latest == selected


def test_select_latest_xyz_falls_back_to_newest_generated_candidate(tmp_path: Path) -> None:
    older = tmp_path / "coord.xyz"
    newer = tmp_path / "struc_final.xyz"
    _write_xyz(older, "older")
    _write_xyz(newer, "newer")
    _set_mtime(older, seconds=10)
    _set_mtime(newer, seconds=20)

    latest = _helpers.select_latest_xyz(tmp_path)

    assert latest == newer


def test_select_latest_xyz_raises_when_directory_has_no_xyz_files(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match=r"No \.xyz file found"):
        _helpers.select_latest_xyz(tmp_path)


def test_select_input_xyz_returns_manifest_selected_file(tmp_path: Path) -> None:
    selected = tmp_path / "nested" / "chosen.xyz"
    selected.parent.mkdir()
    _write_xyz(selected, "chosen")

    resolved = _helpers.select_input_xyz(tmp_path, {"input_xyz": "nested/chosen.xyz"})

    assert resolved == selected.resolve()


def test_select_input_xyz_rejects_missing_manifest_selected_file(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="Manifest input_xyz not found"):
        _helpers.select_input_xyz(tmp_path, {"input_xyz": "missing.xyz"})


def test_select_input_xyz_rejects_non_xyz_manifest_selected_file(tmp_path: Path) -> None:
    not_xyz = tmp_path / "chosen.txt"
    not_xyz.write_text("not xyz\n", encoding="utf-8")

    with pytest.raises(ValueError, match=r"must point to a \.xyz file"):
        _helpers.select_input_xyz(tmp_path, {"input_xyz": "chosen.txt"})


def test_select_input_xyz_uses_latest_xyz_when_manifest_has_no_input(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fallback = tmp_path / "fallback.xyz"
    _write_xyz(fallback, "fallback")
    called_with: list[Path] = []

    def fake_select_latest_xyz(job_dir: Path) -> Path:
        called_with.append(job_dir)
        return fallback

    monkeypatch.setattr(_helpers, "select_latest_xyz", fake_select_latest_xyz)

    selected = _helpers.select_input_xyz(tmp_path, {})

    assert selected == fallback
    assert called_with == [tmp_path]


def test_queued_state_payload_copies_resource_request_and_sets_timestamps(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(_helpers, "now_utc_iso", lambda: "2026-04-19T00:00:00+00:00")
    resource_request = {"max_cores": 8, "max_memory_gb": 32}
    job_dir = tmp_path / "job"
    selected_xyz = job_dir / "input.xyz"

    payload = _helpers.queued_state_payload(
        job_id="crest-123",
        job_dir=job_dir,
        selected_xyz=selected_xyz,
        mode="nci",
        molecule_key="mol-1",
        resource_request=resource_request,
    )

    assert payload == {
        "job_id": "crest-123",
        "job_dir": str(job_dir),
        "selected_input_xyz": str(selected_xyz),
        "molecule_key": "mol-1",
        "mode": "nci",
        "status": "queued",
        "created_at": "2026-04-19T00:00:00+00:00",
        "updated_at": "2026-04-19T00:00:00+00:00",
        "resource_request": {"max_cores": 8, "max_memory_gb": 32},
        "resource_actual": {"max_cores": 8, "max_memory_gb": 32},
    }
    assert payload["resource_request"] is not resource_request
    assert payload["resource_actual"] is not resource_request


def test_resolve_job_dir_delegates_to_validate_job_dir(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    cfg = _cfg(tmp_path)
    calls: list[tuple[str, str, str]] = []
    resolved = tmp_path / "resolved-job"

    def fake_validate_job_dir(raw_job_dir: str, allowed_root: str, *, label: str) -> Path:
        calls.append((raw_job_dir, allowed_root, label))
        return resolved

    monkeypatch.setattr(_helpers, "validate_job_dir", fake_validate_job_dir)

    job_dir = _helpers.resolve_job_dir(cfg, "~/job-42")

    assert job_dir == resolved
    assert calls == [("~/job-42", cfg.runtime.allowed_root, "Job directory")]


def test_new_job_id_delegates_to_timestamped_token(monkeypatch: pytest.MonkeyPatch) -> None:
    tokens: list[str] = []

    def fake_timestamped_token(prefix: str) -> str:
        tokens.append(prefix)
        return "crest-20260419-000000"

    monkeypatch.setattr(_helpers, "timestamped_token", fake_timestamped_token)

    job_id = _helpers.new_job_id()

    assert job_id == "crest-20260419-000000"
    assert tokens == ["crest"]

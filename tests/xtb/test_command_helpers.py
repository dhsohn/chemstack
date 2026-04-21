from __future__ import annotations

from pathlib import Path

import pytest
import yaml

from chemstack.xtb.commands import _helpers


def _write_xyz(path: Path, comment: str) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        "\n".join(
            [
                "3",
                comment,
                "O 0.000000 0.000000 0.000000",
                "H 0.000000 0.000000 0.970000",
                "H 0.000000 0.750000 -0.240000",
                "",
            ]
        ),
        encoding="utf-8",
    )
    return path


def _write_manifest(job_dir: Path, payload: object) -> Path:
    path = job_dir / _helpers.MANIFEST_FILE_NAME
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(yaml.safe_dump(payload, sort_keys=False), encoding="utf-8")
    return path


def test_load_job_manifest_requires_existing_mapping(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="Missing xTB job manifest"):
        _helpers.load_job_manifest(tmp_path)

    _write_manifest(tmp_path, ["not", "a", "mapping"])

    with pytest.raises(ValueError, match="Invalid xTB job manifest"):
        _helpers.load_job_manifest(tmp_path)


def test_job_type_rejects_unknown_manifest_values() -> None:
    with pytest.raises(ValueError, match="Unsupported xtb job_type: weird_mode"):
        _helpers.job_type({"job_type": "weird_mode"})


def test_as_int_new_job_id_and_choose_xyz_error_paths(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    xyz_root = tmp_path / "inputs"
    xyz_root.mkdir()
    _write_xyz(xyz_root / "sample.xyz", "sample")
    text_file = xyz_root / "notes.txt"
    text_file.write_text("not xyz\n", encoding="utf-8")

    assert _helpers._as_int("oops", 7) == 7
    monkeypatch.setattr(_helpers, "timestamped_token", lambda prefix: f"{prefix}_001")
    assert _helpers.new_job_id() == "xtb_001"

    with pytest.raises(ValueError, match="reactant file not found"):
        _helpers._choose_xyz(xyz_root, "missing.xyz", label="reactant")

    with pytest.raises(ValueError, match="reactant file must be .xyz"):
        _helpers._choose_xyz(xyz_root, "notes.txt", label="reactant")

    with pytest.raises(ValueError, match="No .xyz files found in input directory"):
        _helpers._choose_xyz(tmp_path / "empty", "", label="input")


def test_resolve_job_inputs_path_search_prefers_non_excluded_xyz(tmp_path: Path) -> None:
    job_dir = tmp_path / "Reaction Batch 01"
    reactants_dir = job_dir / "reactants"
    products_dir = job_dir / "products"
    _write_xyz(reactants_dir / "xtb_seed.xyz", "excluded")
    selected_reactant = _write_xyz(reactants_dir / "Starter Geometry.xyz", "selected reactant")
    _write_xyz(products_dir / "coord.xyz", "excluded")
    selected_product = _write_xyz(products_dir / "Product Final.xyz", "selected product")
    manifest = {
        "job_type": "path_search",
        "reaction_key": "SnAr Step 1",
    }

    resolved = _helpers.resolve_job_inputs(job_dir, manifest)

    assert resolved == {
        "job_type": "path_search",
        "reaction_key": "snar_step_1",
        "selected_input_xyz": selected_reactant.resolve(),
        "secondary_input_xyz": selected_product.resolve(),
        "input_summary": {
            "reactant_xyz": str(selected_reactant.resolve()),
            "product_xyz": str(selected_product.resolve()),
            "reactant_count": 2,
            "product_count": 2,
        },
    }


def test_resolve_job_inputs_ranking_collects_sorted_candidates(tmp_path: Path) -> None:
    job_dir = tmp_path / "Cyclization Ranking"
    candidates_dir = job_dir / "screening-set"
    selected_candidate = _write_xyz(candidates_dir / "a_candidate.xyz", "first")
    trailing_candidate = _write_xyz(candidates_dir / "b_candidate.xyz", "second")
    manifest = {
        "job_type": "ranking",
        "candidates_dir": "screening-set",
        "top_n": 0,
    }

    resolved = _helpers.resolve_job_inputs(job_dir, manifest)

    assert resolved == {
        "job_type": "ranking",
        "reaction_key": "cyclization_ranking",
        "selected_input_xyz": selected_candidate.resolve(),
        "secondary_input_xyz": None,
        "input_summary": {
            "candidates_dir": str(candidates_dir.resolve()),
            "candidate_count": 2,
            "candidate_paths": [
                str(selected_candidate.resolve()),
                str(trailing_candidate.resolve()),
            ],
            "top_n": 1,
        },
    }


def test_resolve_job_inputs_reports_missing_required_directories_and_candidates(
    tmp_path: Path,
) -> None:
    path_job_dir = tmp_path / "Path Search"

    with pytest.raises(ValueError, match="Missing reactants directory"):
        _helpers.resolve_job_inputs(path_job_dir, {"job_type": "path_search"})

    (path_job_dir / "reactants").mkdir(parents=True)

    with pytest.raises(ValueError, match="Missing products directory"):
        _helpers.resolve_job_inputs(path_job_dir, {"job_type": "path_search"})

    ranking_job_dir = tmp_path / "Ranking"
    ranking_job_dir.mkdir()
    with pytest.raises(ValueError, match="Missing ranking candidates directory"):
        _helpers.resolve_job_inputs(ranking_job_dir, {"job_type": "ranking"})

    empty_ranking_dir = ranking_job_dir / "candidates"
    empty_ranking_dir.mkdir()
    with pytest.raises(ValueError, match="No .xyz candidates found in ranking directory"):
        _helpers.resolve_job_inputs(ranking_job_dir, {"job_type": "ranking"})


def test_resolve_job_inputs_opt_respects_explicit_input_xyz(tmp_path: Path) -> None:
    job_dir = tmp_path / "Optimization Input"
    explicit_input = _write_xyz(job_dir / "coord.xyz", "explicit input")
    _write_xyz(job_dir / "other.xyz", "fallback")
    manifest = {
        "job_type": "opt",
        "molecule_key": "Catalyst Variant A",
        "input_xyz": "coord.xyz",
    }

    resolved = _helpers.resolve_job_inputs(job_dir, manifest)

    assert resolved == {
        "job_type": "opt",
        "reaction_key": "catalyst_variant_a",
        "selected_input_xyz": explicit_input.resolve(),
        "secondary_input_xyz": None,
        "input_summary": {
            "input_xyz": str(explicit_input.resolve()),
            "input_count": 1,
        },
    }


def test_queued_state_payload_copies_candidate_and_resource_metadata(tmp_path: Path) -> None:
    selected_input = _write_xyz(tmp_path / "candidate.xyz", "queued input").resolve()
    candidate_paths = ["/tmp/a.xyz", "/tmp/b.xyz"]
    input_summary = {
        "candidate_count": "2",
        "candidate_paths": candidate_paths,
        "top_n": 2,
    }
    resource_request = {"max_cores": 6, "max_memory_gb": 24}

    payload = _helpers.queued_state_payload(
        job_id="xtb_20260420_000001",
        job_dir=tmp_path.resolve(),
        selected_input_xyz=selected_input,
        job_type="ranking",
        reaction_key="mol_a",
        input_summary=input_summary,
        resource_request=resource_request,
    )

    candidate_paths.append("/tmp/c.xyz")
    resource_request["max_cores"] = 99

    assert payload["job_id"] == "xtb_20260420_000001"
    assert payload["job_dir"] == str(tmp_path.resolve())
    assert payload["selected_input_xyz"] == str(selected_input)
    assert payload["job_type"] == "ranking"
    assert payload["reaction_key"] == "mol_a"
    assert payload["status"] == "queued"
    assert payload["created_at"] == payload["updated_at"]
    assert payload["candidate_count"] == 2
    assert payload["candidate_paths"] == ["/tmp/a.xyz", "/tmp/b.xyz"]
    assert payload["selected_candidate_paths"] == []
    assert payload["input_summary"]["candidate_count"] == "2"
    assert payload["input_summary"]["top_n"] == 2
    assert payload["resource_request"] == {"max_cores": 6, "max_memory_gb": 24}
    assert payload["resource_actual"] == {"max_cores": 6, "max_memory_gb": 24}

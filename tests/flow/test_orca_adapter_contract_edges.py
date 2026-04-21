from __future__ import annotations

import json
import sys
from pathlib import Path
from types import SimpleNamespace
from typing import cast

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "src"))

from chemstack.core.indexing import JobLocationRecord

from chemstack.flow.adapters import orca as orca_adapter


def _write_json(path: Path, payload: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=True, indent=2), encoding="utf-8")


def _write_text(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")


def _write_xyz(path: Path, *, comment: str = "comment") -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        "\n".join(
            [
                "2",
                comment,
                "H 0.0 0.0 0.0",
                "H 0.0 0.0 0.74",
                "",
            ]
        ),
        encoding="utf-8",
    )


def _module_not_found(name: str) -> ModuleNotFoundError:
    error = ModuleNotFoundError(f"No module named '{name}'")
    error.name = name
    return error


def test_json_loader_helpers_handle_missing_invalid_and_filtered_inputs(tmp_path: Path) -> None:
    assert orca_adapter._load_json_dict(tmp_path / "missing.json") == {}
    assert orca_adapter._load_json_list(tmp_path / "missing_list.json") == []
    assert orca_adapter._load_jsonl_records(tmp_path / "missing_records.jsonl") == []

    invalid_dict = tmp_path / "invalid_dict.json"
    invalid_list = tmp_path / "invalid_list.json"
    non_dict = tmp_path / "non_dict.json"
    non_list = tmp_path / "non_list.json"
    list_path = tmp_path / "queue.json"
    jsonl_path = tmp_path / "records.jsonl"

    _write_text(invalid_dict, "{broken")
    _write_text(invalid_list, "{broken")
    _write_json(non_dict, ["not", "a", "dict"])
    _write_json(non_list, {"not": "a-list"})
    _write_json(list_path, [{"queue_id": "q1"}, "skip", 3, {"queue_id": "q2"}])
    _write_text(
        jsonl_path,
        '\n{"run_id": "run_1"}\nnot-json\n["skip"]\n{"run_id": "run_2"}\n',
    )

    assert orca_adapter._load_json_dict(invalid_dict) == {}
    assert orca_adapter._load_json_dict(non_dict) == {}
    assert orca_adapter._load_json_list(invalid_list) == []
    assert orca_adapter._load_json_list(non_list) == []
    assert orca_adapter._load_json_list(list_path) == [{"queue_id": "q1"}, {"queue_id": "q2"}]
    assert orca_adapter._load_jsonl_records(jsonl_path) == [{"run_id": "run_1"}, {"run_id": "run_2"}]


def test_load_jsonl_records_returns_empty_on_read_error() -> None:
    class ExplodingPath:
        def exists(self) -> bool:
            return True

        def read_text(self, encoding: str = "utf-8") -> str:
            raise OSError("read failure")

    assert orca_adapter._load_jsonl_records(cast(Path, ExplodingPath())) == []


def test_import_orca_auto_module_retries_sibling_repo_and_returns_none(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    sibling_repo = tmp_path / "chemstack"
    sibling_repo.mkdir()
    calls: list[str] = []

    def fake_import(module_name: str) -> object:
        calls.append(module_name)
        raise _module_not_found("chemstack")

    monkeypatch.setattr(orca_adapter, "import_module", fake_import)
    monkeypatch.setattr(orca_adapter, "_sibling_orca_auto_repo_root", lambda: sibling_repo)
    monkeypatch.setattr(orca_adapter.sys, "path", ["existing"])

    assert orca_adapter._import_orca_auto_module("chemstack.orca.tracking") is None
    assert calls == ["chemstack.orca.tracking", "chemstack.orca.tracking"]
    assert orca_adapter.sys.path[0] == str(sibling_repo)


def test_import_orca_auto_module_reraises_unrelated_import_errors(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        orca_adapter,
        "import_module",
        lambda _module_name: (_ for _ in ()).throw(_module_not_found("different_module")),
    )

    with pytest.raises(ModuleNotFoundError, match="different_module"):
        orca_adapter._import_orca_auto_module("chemstack.orca.tracking")


def test_tracked_helper_guards_return_empty_for_missing_helpers_and_exceptions(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    assert orca_adapter._tracked_artifact_context(index_root=None, targets=("job_1",)) == (None, None, {}, {}, {})
    assert (
        orca_adapter._tracked_runtime_context(
            index_root=None,
            organized_root=None,
            target="job_1",
            queue_id="",
            run_id="",
            reaction_dir="",
        )
        is None
    )
    assert (
        orca_adapter._tracked_contract_payload(
            index_root=None,
            organized_root=None,
            target="job_1",
            queue_id="",
            run_id="",
            reaction_dir="",
        )
        is None
    )

    monkeypatch.setattr(orca_adapter, "_orca_auto_tracking_module", lambda: None)
    assert orca_adapter._tracked_artifact_context(index_root=tmp_path, targets=("job_1",)) == (None, None, {}, {}, {})
    assert (
        orca_adapter._tracked_runtime_context(
            index_root=tmp_path,
            organized_root=None,
            target="job_1",
            queue_id="",
            run_id="",
            reaction_dir="",
        )
        is None
    )

    calls: list[str] = []

    def load_job_artifact_context(_index_root: Path, target: str) -> SimpleNamespace:
        calls.append(target)
        return SimpleNamespace(job_dir=None)

    monkeypatch.setattr(
        orca_adapter,
        "_orca_auto_tracking_module",
        lambda: SimpleNamespace(load_job_artifact_context=load_job_artifact_context),
    )
    assert orca_adapter._tracked_artifact_context(index_root=tmp_path, targets=("   ", "job_2")) == (None, None, {}, {}, {})
    assert calls == ["job_2"]

    monkeypatch.setattr(orca_adapter, "_orca_auto_tracking_module", lambda: SimpleNamespace())
    assert (
        orca_adapter._tracked_runtime_context(
            index_root=tmp_path,
            organized_root=None,
            target="job_3",
            queue_id="",
            run_id="",
            reaction_dir="",
        )
        is None
    )

    monkeypatch.setattr(
        orca_adapter,
        "_orca_auto_tracking_module",
        lambda: SimpleNamespace(
            load_job_runtime_context=lambda *_args, **_kwargs: (_ for _ in ()).throw(RuntimeError("boom"))
        ),
    )
    assert (
        orca_adapter._tracked_runtime_context(
            index_root=tmp_path,
            organized_root=None,
            target="job_4",
            queue_id="",
            run_id="",
            reaction_dir="",
        )
        is None
    )

    monkeypatch.setattr(
        orca_adapter,
        "_orca_auto_tracking_module",
        lambda: SimpleNamespace(load_job_runtime_context=lambda *_args, **_kwargs: SimpleNamespace(artifact=None)),
    )
    assert (
        orca_adapter._tracked_runtime_context(
            index_root=tmp_path,
            organized_root=None,
            target="job_5",
            queue_id="",
            run_id="",
            reaction_dir="",
        )
        is None
    )

    monkeypatch.setattr(
        orca_adapter,
        "_orca_auto_tracking_module",
        lambda: SimpleNamespace(
            load_orca_contract_payload=lambda *_args, **_kwargs: (_ for _ in ()).throw(RuntimeError("boom"))
        ),
    )
    assert (
        orca_adapter._tracked_contract_payload(
            index_root=tmp_path,
            organized_root=None,
            target="job_6",
            queue_id="",
            run_id="",
            reaction_dir="",
        )
        is None
    )


def test_path_and_record_helpers_cover_relative_deduped_and_subpath_cases(tmp_path: Path) -> None:
    base_dir = tmp_path / "base"
    organized_dir = tmp_path / "organized"
    other_dir = tmp_path / "other"
    file_path = tmp_path / "note.txt"
    base_dir.mkdir()
    organized_dir.mkdir()
    other_dir.mkdir()
    _write_text(file_path, "note")

    record = JobLocationRecord(
        job_id="job_1",
        app_name="orca_auto",
        job_type="orca_opt",
        status="running",
        original_run_dir=str(tmp_path / "missing_stub"),
        organized_output_dir=str(organized_dir),
        latest_known_path=str(tmp_path / "missing_latest"),
    )

    assert orca_adapter._resolve_artifact_path("", base_dir) == ""
    assert orca_adapter._resolve_artifact_path("relative.xyz", None) == "relative.xyz"
    assert orca_adapter._resolve_artifact_path("relative.xyz", base_dir) == str((base_dir / "relative.xyz").resolve())
    assert orca_adapter._record_organized_dir(None) is None
    assert orca_adapter._record_organized_dir(record) == organized_dir.resolve()
    assert orca_adapter._iter_existing_dirs(None, organized_dir, organized_dir, file_path, other_dir) == [
        organized_dir.resolve(),
        other_dir.resolve(),
    ]
    assert orca_adapter._is_subpath(organized_dir, tmp_path) is True
    assert orca_adapter._is_subpath(organized_dir, None) is False
    assert orca_adapter._is_subpath(tmp_path / "outside", organized_dir) is False


def test_derive_selected_input_xyz_and_attempt_helpers_fall_back_safely(tmp_path: Path) -> None:
    missing_inp = tmp_path / "missing.inp"
    no_xyzfile_inp = tmp_path / "no_xyzfile.inp"
    _write_text(no_xyzfile_inp, "! Opt\n* xyz 0 1 missing.xyz\n")

    state = {
        "attempts": [
            {
                "index": "bad",
                "inp_path": " attempt.inp ",
                "return_code": "bad",
                "markers": "not-a-list",
                "patch_actions": None,
            },
            "skip",
        ],
        "max_retries": "4",
        "final_result": {"status": "failed", "reason": "state_failed"},
    }
    report = {
        "attempt_count": "bad",
        "max_retries": "bad",
        "attempts": "bad",
        "final_result": "bad",
    }

    assert orca_adapter._derive_selected_input_xyz(str(missing_inp)) == ""
    assert orca_adapter._derive_selected_input_xyz(str(no_xyzfile_inp)) == ""
    assert orca_adapter._attempt_count(state, report) == 2
    assert orca_adapter._attempt_count({}, {}) == 0
    assert orca_adapter._max_retries(state, report) == 4
    assert orca_adapter._coerce_attempts(state, report) == (
        {
            "index": 0,
            "attempt_number": 0,
            "inp_path": "attempt.inp",
            "out_path": "",
            "return_code": 0,
            "analyzer_status": "",
            "analyzer_reason": "",
            "markers": [],
            "patch_actions": [],
            "started_at": "",
            "ended_at": "",
        },
    )
    assert orca_adapter._coerce_attempts({}, {}) == ()
    assert orca_adapter._final_result_payload(state, report) == {"status": "failed", "reason": "state_failed"}
    assert orca_adapter._final_result_payload({}, {}) == {}


def test_prefer_orca_optimized_xyz_uses_latest_known_file_parent_and_empty_fallback(tmp_path: Path) -> None:
    run_dir = tmp_path / "run_dir"
    run_dir.mkdir()

    latest_marker = run_dir / "latest.out"
    last_out = run_dir / "final.out"
    preferred_xyz = run_dir / "final.xyz"
    _write_text(latest_marker, "latest marker")
    _write_text(last_out, "orca output")
    _write_xyz(preferred_xyz, comment="optimized")

    chosen = orca_adapter._prefer_orca_optimized_xyz(
        selected_inp="",
        selected_input_xyz="",
        current_dir=None,
        organized_dir=None,
        latest_known_path=str(latest_marker),
        last_out_path=str(last_out),
    )

    empty = orca_adapter._prefer_orca_optimized_xyz(
        selected_inp="",
        selected_input_xyz="",
        current_dir=None,
        organized_dir=None,
        latest_known_path=str(tmp_path / "missing.out"),
        last_out_path="",
    )

    assert chosen == str(preferred_xyz.resolve())
    assert empty == ""


@pytest.mark.parametrize(
    ("queue_entry", "state", "report", "expected_status"),
    [
        ({"status": "running"}, {}, {}, "running"),
        ({}, {"status": "completed"}, {}, "completed"),
        ({"status": "custom_queue_status"}, {}, {}, "custom_queue_status"),
        ({}, {"status": "custom_state_status"}, {}, "custom_state_status"),
    ],
)
def test_status_from_payloads_covers_remaining_fallback_branches(
    queue_entry: dict[str, object],
    state: dict[str, object],
    report: dict[str, object],
    expected_status: str,
) -> None:
    status, analyzer_status, reason, completed_at = orca_adapter._status_from_payloads(
        queue_entry=queue_entry,
        state=state,
        report=report,
    )

    assert status == expected_status
    assert analyzer_status == ""
    assert reason == ""
    assert completed_at == ""


def test_load_orca_artifact_contract_refreshes_from_queue_reaction_dir_tracking_context(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    allowed_root = tmp_path / "orca_runs"
    reaction_dir = allowed_root / "rxn_refresh"
    inp = reaction_dir / "job_refresh.inp"
    source_xyz = reaction_dir / "source.xyz"
    final_out = reaction_dir / "job_refresh.out"
    optimized_xyz = reaction_dir / "job_refresh.xyz"

    reaction_dir.mkdir(parents=True)
    _write_text(inp, "! Opt\n* xyzfile 0 1 source.xyz\n")
    _write_xyz(source_xyz)
    _write_text(final_out, "normal termination\n")
    _write_xyz(optimized_xyz, comment="optimized")
    _write_json(
        allowed_root / "queue.json",
        [
            {
                "queue_id": "q_refresh",
                "task_id": "job_refresh",
                "reaction_dir": str(reaction_dir),
                "status": "running",
            }
        ],
    )

    calls: list[tuple[str, ...]] = []
    record = SimpleNamespace(
        app_name="orca_auto",
        status="queued",
        selected_input_xyz="",
        latest_known_path="",
        organized_output_dir="",
        original_run_dir=str(reaction_dir),
        resource_request={},
        resource_actual={},
    )

    def fake_tracked_artifact_context(*, index_root: Path | None, targets: tuple[str, ...]):
        calls.append(targets)
        if targets == ("job_refresh", "", ""):
            return None, None, {}, {}, {}
        if targets == (str(reaction_dir.resolve()),):
            return (
                reaction_dir.resolve(),
                record,
                {
                    "run_id": "run_refresh",
                    "selected_inp": "job_refresh.inp",
                    "status": "running",
                    "final_result": {
                        "status": "completed",
                        "analyzer_status": "completed",
                        "reason": "normal_termination",
                        "last_out_path": "job_refresh.out",
                    },
                },
                {"attempt_count": "1"},
                {"selected_input_xyz": "source.xyz"},
            )
        return None, None, {}, {}, {}

    monkeypatch.setattr(orca_adapter, "_tracked_contract_payload", lambda **kwargs: None)
    monkeypatch.setattr(orca_adapter, "_tracked_runtime_context", lambda **kwargs: None)
    monkeypatch.setattr(orca_adapter, "_tracked_artifact_context", fake_tracked_artifact_context)
    monkeypatch.setattr(orca_adapter, "_resolve_job_dir", lambda *_args, **_kwargs: (None, None))

    contract = orca_adapter.load_orca_artifact_contract(
        target="job_refresh",
        orca_allowed_root=allowed_root,
    )

    assert calls == [("job_refresh", "", ""), (str(reaction_dir.resolve()),)]
    assert contract.run_id == "run_refresh"
    assert contract.status == "completed"
    assert contract.queue_id == "q_refresh"
    assert contract.queue_status == "running"
    assert contract.reaction_dir == str(reaction_dir.resolve())
    assert contract.selected_inp == str(inp.resolve())
    assert contract.selected_input_xyz == str(source_xyz.resolve())
    assert contract.last_out_path == str(final_out.resolve())
    assert contract.optimized_xyz_path == str(optimized_xyz.resolve())


def test_load_orca_artifact_contract_uses_tracked_status_state_fallbacks_and_current_dir_as_organized_output(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    allowed_root = tmp_path / "orca_runs"
    organized_root = tmp_path / "orca_outputs"
    current_dir = organized_root / "opt" / "edge_case_run"
    selected_xyz = current_dir / "edge_source.xyz"

    current_dir.mkdir(parents=True)
    _write_xyz(selected_xyz)
    _write_json(
        current_dir / "run_state.json",
        {
            "status": "   ",
            "attempts": [{"index": 1}],
            "max_retries": "4",
            "final_result": {
                "analyzer_status": "partial",
                "reason": "state_only_reason",
            },
        },
    )
    _write_json(current_dir / "organized_ref.json", {"selected_input_xyz": "edge_source.xyz"})

    record = JobLocationRecord(
        job_id="job_edge_case",
        app_name="orca_auto",
        job_type="orca_opt",
        status="submitted",
        original_run_dir=str(tmp_path / "stub"),
        selected_input_xyz="",
        organized_output_dir="",
        latest_known_path="",
        resource_request=cast(dict[str, int], {"max_cores": "2"}),
        resource_actual={},
    )

    monkeypatch.setattr(orca_adapter, "_tracked_contract_payload", lambda **kwargs: None)
    monkeypatch.setattr(orca_adapter, "_tracked_runtime_context", lambda **kwargs: None)
    monkeypatch.setattr(orca_adapter, "_tracked_artifact_context", lambda **kwargs: (None, None, {}, {}, {}))
    monkeypatch.setattr(orca_adapter, "resolve_job_location", lambda *_args, **_kwargs: record)

    contract = orca_adapter.load_orca_artifact_contract(
        target=str(current_dir),
        orca_allowed_root=allowed_root,
        orca_organized_root=organized_root,
    )

    assert contract.status == "submitted"
    assert contract.reason == "state_only_reason"
    assert contract.analyzer_status == "partial"
    assert contract.reaction_dir == str(current_dir.resolve())
    assert contract.latest_known_path == str(current_dir.resolve())
    assert contract.organized_output_dir == str(current_dir.resolve())
    assert contract.selected_inp == str(selected_xyz.resolve())
    assert contract.selected_input_xyz == str(selected_xyz.resolve())
    assert contract.attempt_count == 1
    assert contract.max_retries == 4
    assert contract.final_result == {"analyzer_status": "partial", "reason": "state_only_reason"}
    assert contract.report_json_path == ""
    assert contract.report_md_path == ""
    assert contract.resource_request == {"max_cores": 2}
    assert contract.resource_actual == {"max_cores": 2}


def test_load_orca_artifact_contract_returns_sparse_contract_when_nothing_resolves(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(orca_adapter, "_tracked_contract_payload", lambda **kwargs: None)
    monkeypatch.setattr(orca_adapter, "_tracked_runtime_context", lambda **kwargs: None)
    monkeypatch.setattr(orca_adapter, "_tracked_artifact_context", lambda **kwargs: (None, None, {}, {}, {}))
    monkeypatch.setattr(orca_adapter, "_resolve_job_dir", lambda *_args, **_kwargs: (None, None))

    contract = orca_adapter.load_orca_artifact_contract(
        target="  missing-target  ",
        queue_id=" q_hint ",
    )

    assert contract.run_id == ""
    assert contract.status == "unknown"
    assert contract.reason == ""
    assert contract.reaction_dir == ""
    assert contract.latest_known_path == "missing-target"
    assert contract.organized_output_dir == ""
    assert contract.queue_id == "q_hint"
    assert contract.queue_status == ""
    assert contract.selected_inp == ""
    assert contract.selected_input_xyz == ""
    assert contract.run_state_path == ""
    assert contract.report_json_path == ""
    assert contract.report_md_path == ""

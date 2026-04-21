# ruff: noqa: E402

from __future__ import annotations

import json
import sys
from pathlib import Path
from types import SimpleNamespace
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO_ROOT))
sys.path.insert(0, str(REPO_ROOT / "src"))

from chemstack.flow import cli
from chemstack.flow.contracts import XtbDownstreamPolicy


class _FakeSerializable:
    def __init__(self, **payload: Any) -> None:
        self.__dict__.update(payload)

    def to_dict(self) -> dict[str, Any]:
        return dict(self.__dict__)


def test_cmd_xtb_candidates_text_output_builds_policy_and_formats_candidates(
    monkeypatch,
    capsys,
) -> None:
    contract = SimpleNamespace(job_id="xtb-job-1", job_type="xtb_ts", reaction_key="rxn-1")
    captured: dict[str, Any] = {}
    candidates = [
        _FakeSerializable(
            source_job_id="xtb-job-1",
            source_job_type="xtb_ts",
            reaction_key="rxn-1",
            selected_input_xyz="/tmp/input-1.xyz",
            rank=1,
            kind="ts_guess",
            artifact_path="/tmp/candidate-1.xyz",
            selected=True,
            metadata={},
        ),
        _FakeSerializable(
            source_job_id="xtb-job-1",
            source_job_type="xtb_ts",
            reaction_key="rxn-1",
            selected_input_xyz="/tmp/input-2.xyz",
            rank=2,
            kind="optimized_geometry",
            artifact_path="/tmp/candidate-2.xyz",
            selected=False,
            metadata={},
        ),
    ]

    def fake_load_xtb_artifact_contract(*, xtb_index_root: str, target: str) -> Any:
        captured["load_kwargs"] = {"xtb_index_root": xtb_index_root, "target": target}
        return contract

    def fake_select_xtb_downstream_inputs(contract_arg: Any, *, policy: XtbDownstreamPolicy) -> list[Any]:
        captured["contract"] = contract_arg
        captured["policy"] = policy
        return candidates

    monkeypatch.setattr(cli, "load_xtb_artifact_contract", fake_load_xtb_artifact_contract)
    monkeypatch.setattr(cli, "select_xtb_downstream_inputs", fake_select_xtb_downstream_inputs)

    args = SimpleNamespace(
        xtb_index_root="/tmp/xtb-index",
        target="xtb-job-1",
        preferred_kinds=["optimized_geometry", "ts_guess"],
        max_candidates=2,
        include_unselected=True,
        json=False,
    )

    assert cli.cmd_xtb_candidates(args) == 0

    stdout = capsys.readouterr().out
    policy = captured["policy"]
    assert captured["load_kwargs"] == {"xtb_index_root": "/tmp/xtb-index", "target": "xtb-job-1"}
    assert captured["contract"] is contract
    assert isinstance(policy, XtbDownstreamPolicy)
    assert policy.preferred_kinds == ("optimized_geometry", "ts_guess")
    assert policy.max_candidates == 2
    assert policy.selected_only is False
    assert "source_job_id: xtb-job-1" in stdout
    assert "candidate_count: 2" in stdout
    assert "- rank=1 kind=ts_guess selected=True path=/tmp/candidate-1.xyz" in stdout
    assert "- rank=2 kind=optimized_geometry selected=False path=/tmp/candidate-2.xyz" in stdout


def test_cmd_crest_inspect_json_output_serializes_contract(monkeypatch, capsys) -> None:
    contract = _FakeSerializable(
        job_id="crest-job-1",
        mode="nci",
        status="completed",
        reason="done",
        job_dir="/tmp/crest-job",
        latest_known_path="/tmp/crest-job",
        organized_output_dir="/tmp/crest-job/organized",
        molecule_key="mol-1",
        selected_input_xyz="/tmp/crest-job/input.xyz",
        retained_conformer_count=2,
        retained_conformer_paths=["/tmp/crest-job/conf-1.xyz", "/tmp/crest-job/conf-2.xyz"],
    )

    def fake_load_crest_artifact_contract(*, crest_index_root: str, target: str) -> Any:
        assert crest_index_root == "/tmp/crest-index"
        assert target == "crest-job-1"
        return contract

    monkeypatch.setattr(cli, "load_crest_artifact_contract", fake_load_crest_artifact_contract)

    args = SimpleNamespace(crest_index_root="/tmp/crest-index", target="crest-job-1", json=True)
    assert cli.cmd_crest_inspect(args) == 0

    payload = json.loads(capsys.readouterr().out)
    assert payload["job_id"] == "crest-job-1"
    assert payload["mode"] == "nci"
    assert payload["retained_conformer_count"] == 2
    assert payload["retained_conformer_paths"] == [
        "/tmp/crest-job/conf-1.xyz",
        "/tmp/crest-job/conf-2.xyz",
    ]


def test_build_parser_parses_xtb_candidates_options() -> None:
    parser = cli.build_parser()

    args = parser.parse_args(
        [
            "xtb",
            "candidates",
            "job-42",
            "--xtb-index-root",
            "/tmp/xtb-index",
            "--preferred-kind",
            "selected_path",
            "--preferred-kind",
            "ts_guess",
            "--include-unselected",
            "--max-candidates",
            "5",
            "--json",
        ]
    )

    assert args.command == "xtb"
    assert args.xtb_command == "candidates"
    assert args.target == "job-42"
    assert args.xtb_index_root == "/tmp/xtb-index"
    assert args.preferred_kinds == ["selected_path", "ts_guess"]
    assert args.include_unselected is True
    assert args.max_candidates == 5
    assert args.json is True
    assert args.func is cli.cmd_xtb_candidates


def test_main_dispatches_to_selected_cli_handler(monkeypatch) -> None:
    captured: dict[str, Any] = {}

    def fake_cmd_crest_inspect(args: Any) -> int:
        captured["target"] = args.target
        captured["crest_index_root"] = args.crest_index_root
        captured["json"] = args.json
        return 17

    monkeypatch.setattr(cli, "cmd_crest_inspect", fake_cmd_crest_inspect)

    result = cli.main(["crest", "inspect", "crest-job-9", "--crest-index-root", "/tmp/crest-root"])

    assert result == 17
    assert captured == {
        "target": "crest-job-9",
        "crest_index_root": "/tmp/crest-root",
        "json": False,
    }

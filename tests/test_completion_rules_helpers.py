from __future__ import annotations

from pathlib import Path

from core.completion_rules import detect_completion_mode


def test_detect_completion_mode_skips_blank_and_comment_lines_before_route(tmp_path: Path) -> None:
    inp = tmp_path / "rxn.inp"
    inp.write_text(
        "\n# comment\n   \n! NEB-TS IRC\n* xyz 0 1\nH 0 0 0\n*\n",
        encoding="utf-8",
    )

    mode = detect_completion_mode(inp)

    assert mode.kind == "ts"
    assert mode.require_irc is True
    assert mode.route_line == "! NEB-TS IRC"


def test_detect_completion_mode_defaults_to_opt_when_no_route_line_is_present(tmp_path: Path) -> None:
    inp = tmp_path / "rxn.inp"
    inp.write_text(
        "\n# comment only\n* xyz 0 1\nH 0 0 0\n*\n",
        encoding="utf-8",
    )

    mode = detect_completion_mode(inp)

    assert mode.kind == "opt"
    assert mode.require_irc is False
    assert mode.route_line == ""

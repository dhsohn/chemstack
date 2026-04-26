from __future__ import annotations

import runpy
import warnings

import pytest


def _run_module_as_main(module_name: str) -> None:
    with warnings.catch_warnings():
        warnings.filterwarnings("ignore", category=RuntimeWarning)
        runpy.run_module(module_name, run_name="__main__")


def test_chemstack_module_entrypoint_delegates_to_unified_cli(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from chemstack import cli as unified_cli

    monkeypatch.setattr(unified_cli, "main", lambda: 51)

    with pytest.raises(SystemExit) as exc_info:
        _run_module_as_main("chemstack.__main__")

    assert exc_info.value.code == 51


def test_orca_module_entrypoint_delegates_to_orca_cli(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from chemstack.orca import cli as orca_cli

    monkeypatch.setattr(orca_cli, "main", lambda: 52)

    with pytest.raises(SystemExit) as exc_info:
        _run_module_as_main("chemstack.orca.__main__")

    assert exc_info.value.code == 52

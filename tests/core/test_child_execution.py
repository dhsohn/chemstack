from __future__ import annotations

from collections.abc import Callable
from pathlib import Path
from types import SimpleNamespace

from chemstack.core.queue import child_execution


def test_child_worker_shutdown_controller_tracks_request() -> None:
    controller = child_execution.ChildWorkerShutdownController()

    assert controller.is_requested() is False
    controller.request()
    assert controller.is_requested() is True


def test_find_queue_entry_by_id_returns_matching_entry(tmp_path: Path) -> None:
    wanted = SimpleNamespace(queue_id="q-wanted")
    entries = [SimpleNamespace(queue_id="q-other"), wanted]

    assert (
        child_execution.find_queue_entry_by_id(
            tmp_path,
            "q-wanted",
            list_queue_fn=lambda _root: entries,
        )
        is wanted
    )
    assert (
        child_execution.find_queue_entry_by_id(
            tmp_path,
            "missing",
            list_queue_fn=lambda _root: entries,
        )
        is None
    )


def test_child_admission_token_activation_and_release_are_conditional(tmp_path: Path) -> None:
    calls: list[tuple[str, str]] = []

    assert (
        child_execution.activate_child_admission_token(
            tmp_path,
            None,
            work_dir=tmp_path / "work",
            queue_id="q-1",
            source="source",
            activate_reserved_slot_fn=lambda *_args, **_kwargs: calls.append(("activate", "none")),
        )
        is True
    )
    assert calls == []

    assert (
        child_execution.activate_child_admission_token(
            tmp_path,
            "token",
            work_dir=tmp_path / "work",
            queue_id="q-1",
            source="source",
            activate_reserved_slot_fn=lambda *_args, **_kwargs: "slot",
        )
        is True
    )
    assert (
        child_execution.activate_child_admission_token(
            tmp_path,
            "token",
            work_dir=tmp_path / "work",
            queue_id="q-1",
            source="source",
            activate_reserved_slot_fn=lambda *_args, **_kwargs: None,
        )
        is False
    )

    child_execution.release_child_admission_token(
        tmp_path,
        None,
        release_slot_fn=lambda *_args: calls.append(("release", "none")),
    )
    assert calls == []

    child_execution.release_child_admission_token(
        tmp_path,
        "token",
        release_slot_fn=lambda _root, token: calls.append(("release", token)),
    )
    assert calls == [("release", "token")]


def test_install_shutdown_request_handlers_wires_controller() -> None:
    installed: list[Callable[[], None]] = []
    controller = child_execution.ChildWorkerShutdownController()

    child_execution.install_shutdown_request_handlers(
        controller,
        install_signal_handlers_fn=lambda callback: installed.append(callback),
    )

    assert controller.is_requested() is False
    installed[0]()
    assert controller.is_requested() is True

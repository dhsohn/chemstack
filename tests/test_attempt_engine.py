from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace

from core.attempt_engine import _retry_recipe_step, run_attempts
from core.state_store import new_state, state_path


class _InterruptRunner:
    def run(self, _inp_path: Path):
        raise KeyboardInterrupt


class _RetryThenSuccessRunner:
    def __init__(self) -> None:
        self.calls = 0

    def run(self, inp_path: Path):
        self.calls += 1
        inp_path.with_suffix(".xyz").write_text(
            "2\nretry geometry\nH 0 0 0\nH 0 0 0.75\n",
            encoding="utf-8",
        )
        out_path = inp_path.with_suffix(".out")
        if self.calls == 1:
            out_path.write_text("SCF NOT CONVERGED AFTER 300 CYCLES\n", encoding="utf-8")
            return SimpleNamespace(out_path=str(out_path), return_code=1)
        out_path.write_text(
            "****ORCA TERMINATED NORMALLY****\nTOTAL RUN TIME: 0 days 0 hours 0 minutes 1 seconds 0 msec\n",
            encoding="utf-8",
        )
        return SimpleNamespace(out_path=str(out_path), return_code=0)


def _retry_inp_path(selected_inp: Path, retry_number: int) -> Path:
    return selected_inp.parent / f"{selected_inp.stem}.retry{retry_number:02d}.inp"


class TestAttemptEngine(unittest.TestCase):
    def test_keyboard_interrupt_emits_single_run_interrupted_event(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            reaction_dir = Path(td)
            selected_inp = reaction_dir / "rxn.inp"
            selected_inp.write_text("! Opt\n* xyz 0 1\nH 0 0 0\nH 0 0 0.74\n*\n", encoding="utf-8")
            state = new_state(reaction_dir, selected_inp, max_retries=3)

            emitted_payloads = []

            rc = run_attempts(
                reaction_dir,
                selected_inp,
                state,
                resumed=False,
                runner=_InterruptRunner(),
                max_retries=3,
                as_json=False,
                retry_inp_path=_retry_inp_path,
                to_resolved_local=lambda raw: Path(raw),
                emit=lambda payload, _as_json: emitted_payloads.append(payload),
            )

            saved = json.loads(state_path(reaction_dir).read_text(encoding="utf-8"))

        self.assertEqual(rc, 130)
        self.assertEqual(saved["final_result"]["reason"], "interrupted_by_user")
        self.assertEqual(saved["status"], "failed")
        self.assertEqual(len(emitted_payloads), 1)

    def test_retry_notification_callback_receives_retry_context(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            reaction_dir = Path(td)
            selected_inp = reaction_dir / "rxn.inp"
            selected_inp.write_text("! Opt\n* xyz 0 1\nH 0 0 0\nH 0 0 0.74\n*\n", encoding="utf-8")
            state = new_state(reaction_dir, selected_inp, max_retries=2)
            notifications = []

            rc = run_attempts(
                reaction_dir,
                selected_inp,
                state,
                resumed=False,
                runner=_RetryThenSuccessRunner(),
                max_retries=2,
                as_json=False,
                retry_inp_path=_retry_inp_path,
                to_resolved_local=lambda raw: Path(raw),
                emit=lambda _payload, _as_json: None,
                notify_retry=lambda payload: notifications.append(payload),
            )

        self.assertEqual(rc, 0)
        self.assertEqual(len(notifications), 1)
        event = notifications[0]
        self.assertEqual(event["attempt_index"], 1)
        self.assertEqual(event["retry_number"], 1)
        self.assertEqual(event["max_retries"], 2)
        self.assertEqual(event["analyzer_status"], "error_scf")
        self.assertEqual(event["analyzer_reason"], "scf_not_converged")
        self.assertTrue(event["failed_inp"].endswith("rxn.inp"))
        self.assertTrue(event["next_inp"].endswith("rxn.retry01.inp"))
        self.assertIn("route_add_tightscf_slowconv", event["patch_actions"])
        self.assertIn("geometry_restart_from_rxn.xyz", event["patch_actions"])

    def test_start_and_finish_callbacks_emit_immediate_terminal_lifecycle_events(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            reaction_dir = Path(td)
            selected_inp = reaction_dir / "rxn.inp"
            selected_inp.write_text("! Opt\n* xyz 0 1\nH 0 0 0\nH 0 0 0.74\n*\n", encoding="utf-8")
            state = new_state(reaction_dir, selected_inp, max_retries=2)
            started_notifications = []
            finished_notifications = []
            retry_notifications = []

            rc = run_attempts(
                reaction_dir,
                selected_inp,
                state,
                resumed=False,
                runner=_RetryThenSuccessRunner(),
                max_retries=2,
                as_json=False,
                retry_inp_path=_retry_inp_path,
                to_resolved_local=lambda raw: Path(raw),
                emit=lambda _payload, _as_json: None,
                notify_started=lambda payload: started_notifications.append(payload),
                notify_finished=lambda payload: finished_notifications.append(payload),
                notify_retry=lambda payload: retry_notifications.append(payload),
            )

        self.assertEqual(rc, 0)
        self.assertEqual(len(started_notifications), 1)
        self.assertEqual(len(retry_notifications), 1)
        self.assertEqual(len(finished_notifications), 1)

        started = started_notifications[0]
        self.assertEqual(started["attempt_index"], 1)
        self.assertEqual(started["status"], "running")
        self.assertTrue(started["current_inp"].endswith("rxn.inp"))

        finished = finished_notifications[0]
        self.assertEqual(finished["status"], "completed")
        self.assertEqual(finished["analyzer_status"], "completed")
        self.assertEqual(finished["reason"], "normal_termination")
        self.assertEqual(finished["attempt_count"], 2)
        self.assertTrue(finished["last_out_path"].endswith("rxn.retry01.out"))


class TestRetryRecipeStep(unittest.TestCase):
    def test_retry_recipe_step_caps_to_max_recipes(self) -> None:
        self.assertEqual(_retry_recipe_step(1), 1)
        self.assertEqual(_retry_recipe_step(2), 2)
        self.assertEqual(_retry_recipe_step(3), 3)
        self.assertEqual(_retry_recipe_step(4), 4)
        self.assertEqual(_retry_recipe_step(5), 4)
        self.assertEqual(_retry_recipe_step(8), 4)


if __name__ == "__main__":
    unittest.main()

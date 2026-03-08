from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

from core.attempt_engine import _retry_recipe_step, run_attempts
from core.state_store import new_state, state_path


class _InterruptRunner:
    def run(self, _inp_path: Path):
        raise KeyboardInterrupt


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

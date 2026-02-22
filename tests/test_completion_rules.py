import tempfile
import unittest
from pathlib import Path

from core.completion_rules import detect_completion_mode


class TestCompletionRules(unittest.TestCase):
    def test_detect_ts_and_irc(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            inp = Path(td) / "rxn.inp"
            inp.write_text("! OptTS Freq IRC\n* xyz 0 1\nH 0 0 0\nH 0 0 0.74\n*\n", encoding="utf-8")
            mode = detect_completion_mode(inp)
        self.assertEqual(mode.kind, "ts")
        self.assertTrue(mode.require_irc)

    def test_detect_opt_without_irc(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            inp = Path(td) / "rxn.inp"
            inp.write_text("! Opt Freq\n* xyz 0 1\nH 0 0 0\nH 0 0 0.74\n*\n", encoding="utf-8")
            mode = detect_completion_mode(inp)
        self.assertEqual(mode.kind, "opt")
        self.assertFalse(mode.require_irc)


if __name__ == "__main__":
    unittest.main()


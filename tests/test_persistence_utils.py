from __future__ import annotations

import json
import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path

from core.persistence_utils import atomic_write_json, atomic_write_text, now_utc_iso, timestamped_token


class TestPersistenceUtils(unittest.TestCase):
    def test_now_utc_iso_returns_timezone_aware_timestamp(self) -> None:
        parsed = datetime.fromisoformat(now_utc_iso())
        self.assertIsNotNone(parsed.tzinfo)
        self.assertEqual(parsed.utcoffset(), timezone.utc.utcoffset(parsed))

    def test_timestamped_token_preserves_prefix(self) -> None:
        token = timestamped_token("slot")
        self.assertTrue(token.startswith("slot_"))

    def test_atomic_write_text_overwrites_without_tmp_leaks(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            target = root / "sample.txt"
            target.write_text("before", encoding="utf-8")

            atomic_write_text(target, "after")

            self.assertEqual(target.read_text(encoding="utf-8"), "after")
            self.assertEqual(list(root.glob("*.tmp.*")), [])

    def test_atomic_write_json_supports_non_ascii_payloads(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            target = root / "sample.json"

            atomic_write_json(
                target,
                {"message": "한글"},
                ensure_ascii=False,
                indent=None,
            )

            self.assertEqual(json.loads(target.read_text(encoding="utf-8")), {"message": "한글"})
            self.assertEqual(list(root.glob("*.tmp.*")), [])


if __name__ == "__main__":
    unittest.main()

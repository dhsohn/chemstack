from __future__ import annotations

import unittest
from pathlib import Path
from types import SimpleNamespace

from scripts.dft_progress_report import CaseReport, _build_message, _categorize_case


class TestDftProgressReport(unittest.TestCase):
    def _make_case(
        self,
        *,
        name: str,
        category: str,
        status: str = "running",
        active_proc_count: int = 1,
    ) -> CaseReport:
        return CaseReport(
            name=name,
            path=Path(f"/tmp/{name}"),
            category=category,
            status=status,
            run_id=f"{name}_run",
            selected_inp_name=f"{name}.inp",
            started_at=None,
            updated_at=None,
            out_path=None,
            out_size_bytes=0,
            out_mtime=None,
            cycle=None,
            energy=None,
            tail_line="",
            active_proc_count=active_proc_count,
            has_run_lock=False,
            terminated_normally=False,
            max_iter=None,
            cycle_rate_per_hour=None,
            eta_hours=None,
        )

    def test_categorize_case_completed_not_running_even_with_active_process(self) -> None:
        category = _categorize_case(
            status="completed",
            active_proc_count=2,
            terminated_normally=False,
        )
        self.assertEqual(category, "completed")

    def test_running_details_has_blank_line_between_cases(self) -> None:
        cfg = SimpleNamespace(runtime=SimpleNamespace(allowed_root="/tmp/orca_runs"))
        reports = [
            self._make_case(name="rxn_a", category="running"),
            self._make_case(name="rxn_b", category="running"),
        ]

        text = _build_message(
            cfg=cfg,
            reports=reports,
            proc_count=2,
            max_running=8,
            max_completed=3,
        )
        self.assertIn("tail: (tail line not found)\n\n- rxn_b |", text)


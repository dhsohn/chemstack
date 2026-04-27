from __future__ import annotations

import threading
import time
from pathlib import Path
from typing import Any

from chemstack.core.admission import AdmissionSlot, active_slot_count, list_slots
from chemstack.core.indexing import get_job_location
from chemstack.core.queue import list_queue
from chemstack.crest.commands import queue as crest_queue_cmd
from chemstack.flow.submitters import crest_auto as crest_submitter
from chemstack.flow.submitters import xtb_auto as xtb_submitter
from chemstack.xtb.commands import queue as xtb_queue_cmd


def _queue_status(entry: Any) -> str:
    return str(getattr(getattr(entry, "status", None), "value", "")).strip()


def _wait_for_active_slots(root: Path, *, expected: int, timeout: float = 5.0) -> list[AdmissionSlot]:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        slots = list_slots(root)
        if len(slots) == expected:
            return slots
        time.sleep(0.05)
    raise AssertionError(f"Timed out waiting for {expected} active admission slot(s)")


def test_xtb_and_crest_share_single_admission_slot(
    smoke_workspace: Any,
    xtb_opt_job: Path,
    crest_job: Path,
    capsys: Any,
) -> None:
    smoke_workspace.fake_xtb.write_text(
        """#!/usr/bin/env bash
set -euo pipefail
sleep 1.5
printf '1\\nfake xtb optimized\\nH 0.0 0.0 0.0\\n' > xtbopt.xyz
: > .xtboptok
printf '{"total energy": -4.2, "electronic energy": -4.4}\\n' > xtbout.json
printf 'charges\\n' > charges
printf 'wbo\\n' > wbo
printf 'topology\\n' > xtbtopo.mol
exit 0
""",
        encoding="utf-8",
    )
    smoke_workspace.fake_xtb.chmod(0o755)

    xtb_submission = xtb_submitter.submit_job_dir(
        job_dir=str(xtb_opt_job),
        priority=5,
        config_path=str(smoke_workspace.xtb_config_path),
        repo_root=str(smoke_workspace.repo_root),
    )
    crest_submission = crest_submitter.submit_job_dir(
        job_dir=str(crest_job),
        priority=5,
        config_path=str(smoke_workspace.crest_config_path),
        repo_root=str(smoke_workspace.repo_root),
    )

    assert xtb_submission["status"] == "submitted"
    assert crest_submission["status"] == "submitted"
    assert _queue_status(list_queue(smoke_workspace.xtb_allowed_root)[0]) == "pending"
    assert _queue_status(list_queue(smoke_workspace.crest_allowed_root)[0]) == "pending"

    xtb_outcomes: list[str] = []
    xtb_errors: list[BaseException] = []

    def _run_xtb_process_one() -> None:
        try:
            xtb_outcomes.append(
                xtb_queue_cmd._process_one(
                    xtb_queue_cmd.load_config(str(smoke_workspace.xtb_config_path)),
                    auto_organize=True,
                )
            )
        except BaseException as exc:
            xtb_errors.append(exc)

    xtb_thread = threading.Thread(target=_run_xtb_process_one)
    xtb_thread.start()

    slots = _wait_for_active_slots(smoke_workspace.admission_root, expected=1)
    assert active_slot_count(smoke_workspace.admission_root) == 1
    assert slots[0].app_name == "xtb_auto"
    assert slots[0].source == "chemstack.xtb.queue_worker"

    assert crest_queue_cmd._process_one(
        crest_queue_cmd.load_config(str(smoke_workspace.crest_config_path)),
        auto_organize=True,
    ) == "blocked"
    assert active_slot_count(smoke_workspace.admission_root) == 1
    assert len(list_slots(smoke_workspace.admission_root)) == 1
    assert _queue_status(list_queue(smoke_workspace.crest_allowed_root)[0]) == "pending"

    xtb_thread.join(timeout=15)
    assert not xtb_thread.is_alive()
    assert xtb_errors == []
    assert xtb_outcomes == ["processed"]
    xtb_stdout = capsys.readouterr().out
    assert f"queue_id: {xtb_submission['queue_id']}" in xtb_stdout
    assert f"job_id: {xtb_submission['job_id']}" in xtb_stdout
    assert "status: completed" in xtb_stdout

    assert list_slots(smoke_workspace.admission_root) == []

    xtb_record = get_job_location(smoke_workspace.xtb_allowed_root, xtb_submission["job_id"])
    assert xtb_record is not None
    assert xtb_record.status == "completed"
    assert xtb_record.organized_output_dir == ""
    assert xtb_record.latest_known_path == str(xtb_opt_job.resolve())
    assert Path(xtb_record.latest_known_path).exists()

    assert crest_queue_cmd._process_one(
        crest_queue_cmd.load_config(str(smoke_workspace.crest_config_path)),
        auto_organize=True,
    ) == "processed"
    crest_stdout = capsys.readouterr().out
    assert f"queue_id: {crest_submission['queue_id']}" in crest_stdout
    assert f"job_id: {crest_submission['job_id']}" in crest_stdout
    assert "status: completed" in crest_stdout

    crest_record = get_job_location(smoke_workspace.crest_allowed_root, crest_submission["job_id"])
    assert crest_record is not None
    assert crest_record.status == "completed"
    assert crest_record.organized_output_dir == ""
    assert crest_record.latest_known_path == str(crest_job.resolve())
    assert Path(crest_record.latest_known_path).exists()

    assert _queue_status(list_queue(smoke_workspace.xtb_allowed_root)[0]) == "completed"
    assert _queue_status(list_queue(smoke_workspace.crest_allowed_root)[0]) == "completed"
    assert list_slots(smoke_workspace.admission_root) == []

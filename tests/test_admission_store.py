import json
import os
import tempfile
import unittest
from dataclasses import dataclass
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

from chemstack.orca.admission_store import (
    ADMISSION_FILE_NAME,
    _save_slots,
    activate_slot,
    activate_reserved_slot,
    acquire_direct_slot,
    active_slot_count,
    list_slots,
    reconcile_stale_slots,
    release_slot,
    reserve_slot,
    update_slot_metadata as admission_update_slot_metadata,
)


class TestAdmissionStore(unittest.TestCase):
    def test_acquire_direct_slot_tracks_and_releases_slot(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            reaction_dir = root / "rxn"
            reaction_dir.mkdir()

            with acquire_direct_slot(root, max_concurrent=2, reaction_dir=str(reaction_dir)) as token:
                slots = list_slots(root)
                self.assertEqual(len(slots), 1)
                self.assertEqual(slots[0]["token"], token)
                self.assertEqual(slots[0]["reaction_dir"], str(reaction_dir))

            self.assertEqual(active_slot_count(root), 0)

    def test_acquire_direct_slot_ignores_same_reaction_run_lock(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            reaction_dir = root / "rxn_with_lock"
            reaction_dir.mkdir()
            (reaction_dir / "run.lock").write_text(
                json.dumps({"pid": os.getpid()}),
                encoding="utf-8",
            )

            with acquire_direct_slot(root, max_concurrent=1, reaction_dir=str(reaction_dir)) as token:
                slots = list_slots(root)
                self.assertEqual(len(slots), 1)
                self.assertEqual(slots[0]["token"], token)
                self.assertEqual(slots[0]["reaction_dir"], str(reaction_dir))

            self.assertEqual(active_slot_count(root), 0)

    def test_acquire_direct_slot_raises_when_limit_reached(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            first = root / "first"
            second = root / "second"
            first.mkdir()
            second.mkdir()

            with acquire_direct_slot(root, max_concurrent=1, reaction_dir=str(first)):
                with self.assertRaises(RuntimeError) as ctx:
                    with acquire_direct_slot(root, max_concurrent=1, reaction_dir=str(second)):
                        pass

            self.assertIn("Global admission limit reached", str(ctx.exception))

    def test_reserved_slot_activates_and_releases(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            reaction_dir = root / "queued"
            reaction_dir.mkdir()

            token = reserve_slot(
                root,
                1,
                queue_id="q_test",
                source="queue_worker",
            )
            self.assertIsNotNone(token)
            self.assertEqual(active_slot_count(root), 1)

            with activate_reserved_slot(
                root,
                token or "",
                reaction_dir=str(reaction_dir),
                source="queue_run",
                queue_id="q_test",
            ):
                slots = list_slots(root)
                self.assertEqual(len(slots), 1)
                self.assertEqual(slots[0]["state"], "active")
                self.assertEqual(slots[0]["reaction_dir"], str(reaction_dir))
                self.assertEqual(slots[0]["queue_id"], "q_test")
                self.assertEqual(slots[0]["source"], "queue_run")

            self.assertEqual(active_slot_count(root), 0)

    def test_reserved_slot_activation_can_attach_app_and_task_metadata(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            reaction_dir = root / "queued_meta"
            reaction_dir.mkdir()

            token = reserve_slot(
                root,
                1,
                queue_id="q_meta",
                source="queue_worker",
            )
            self.assertIsNotNone(token)

            with activate_reserved_slot(
                root,
                token or "",
                reaction_dir=str(reaction_dir),
                source="queue_run",
                queue_id="q_meta",
                app_name="chemstack_orca",
                task_id="task_meta_123",
            ):
                slots = list_slots(root)
                self.assertEqual(len(slots), 1)
            self.assertEqual(slots[0]["app_name"], "chemstack_orca")
            self.assertEqual(slots[0]["task_id"], "task_meta_123")
            self.assertEqual(slots[0]["reaction_dir"], str(reaction_dir))

            self.assertEqual(active_slot_count(root), 0)

    def test_reserve_slot_is_reserved_before_activation(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            token = reserve_slot(root, 1, source="queue_worker")

            self.assertIsNotNone(token)
            slots = list_slots(root)
            self.assertEqual(len(slots), 1)
            self.assertEqual(slots[0]["token"], token)
            self.assertEqual(slots[0]["state"], "reserved")

    def test_reserve_slot_uses_slot_prefix(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)

            token = reserve_slot(root, 1, source="queue_worker")

            self.assertIsNotNone(token)
            self.assertTrue((token or "").startswith("slot_"))
            self.assertEqual(active_slot_count(root), 1)

    def test_reconcile_stale_slots_removes_dead_owner(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            payload = [
                {
                    "token": "slot_dead",
                    "state": "active",
                    "reaction_dir": str(root / "rxn"),
                    "queue_id": None,
                    "owner_pid": 987654321,
                    "process_start_ticks": None,
                    "source": "direct_run",
                    "acquired_at": "2026-03-20T00:00:00+00:00",
                }
            ]
            (root / ADMISSION_FILE_NAME).write_text(json.dumps(payload), encoding="utf-8")

            removed = reconcile_stale_slots(root)

            self.assertEqual(removed, 1)
            self.assertEqual(active_slot_count(root), 0)

    @patch("chemstack.orca.admission_store.process_start_ticks", return_value=999)
    @patch("chemstack.orca.admission_store.is_process_alive", return_value=True)
    def test_list_slots_treats_pid_reuse_as_stale(
        self,
        mock_alive,
        mock_ticks,
    ) -> None:
        with tempfile.TemporaryDirectory() as tmp, patch(
            "chemstack.orca.admission_store._chem_core_admission_module",
            return_value=None,
        ):
            root = Path(tmp)
            payload = [
                {
                    "token": "slot_reused",
                    "state": "active",
                    "reaction_dir": str(root / "rxn"),
                    "queue_id": None,
                    "owner_pid": os.getpid(),
                    "process_start_ticks": 123,
                    "source": "direct_run",
                    "acquired_at": "2026-03-20T00:00:00+00:00",
                }
            ]
            (root / ADMISSION_FILE_NAME).write_text(json.dumps(payload), encoding="utf-8")

            slots = list_slots(root)

            self.assertEqual(slots, [])
            mock_alive.assert_called()
            mock_ticks.assert_called()

    @patch("chemstack.orca.admission_store.is_process_alive", return_value=True)
    def test_list_slots_supports_chem_core_style_work_dir_payload(self, mock_alive) -> None:
        with tempfile.TemporaryDirectory() as tmp, patch(
            "chemstack.orca.admission_store._chem_core_admission_module",
            return_value=None,
        ):
            root = Path(tmp)
            reaction_dir = root / "rxn"
            payload = [
                {
                    "token": "slot_work_dir",
                    "state": "active",
                    "work_dir": str(reaction_dir),
                    "queue_id": "",
                    "owner_pid": os.getpid(),
                    "process_start_ticks": None,
                    "source": "queue_run",
                    "acquired_at": "2026-03-20T00:00:00+00:00",
                    "app_name": "chemstack_orca",
                    "task_id": "task_123",
                }
            ]
            (root / ADMISSION_FILE_NAME).write_text(json.dumps(payload), encoding="utf-8")

            slots = list_slots(root)

            self.assertEqual(len(slots), 1)
            self.assertEqual(slots[0]["reaction_dir"], str(reaction_dir))
            self.assertEqual(slots[0]["work_dir"], str(reaction_dir))
            self.assertEqual(slots[0]["task_id"], "task_123")
            mock_alive.assert_called()

    def test_save_slots_delegates_to_chem_core_backend_when_available(self) -> None:
        @dataclass(frozen=True)
        class FakeChemCoreSlot:
            token: str
            owner_pid: int
            process_start_ticks: int | None
            source: str
            acquired_at: str
            app_name: str = ""
            task_id: str = ""
            workflow_id: str = ""
            state: str = "active"
            work_dir: str = ""
            queue_id: str = ""

        captured: dict[str, object] = {}

        def _fake_save_slots(root: Path, slots: list[FakeChemCoreSlot]) -> None:
            captured["root"] = root
            captured["slots"] = slots

        fake_backend = SimpleNamespace(
            AdmissionSlot=FakeChemCoreSlot,
            _save_slots=_fake_save_slots,
        )

        with tempfile.TemporaryDirectory() as tmp, patch(
            "chemstack.orca.admission_store._chem_core_admission_module",
            return_value=fake_backend,
        ):
            root = Path(tmp)
            _save_slots(
                root,
                [
                    {
                        "token": "slot_test",
                        "state": "active",
                        "reaction_dir": str(root / "rxn"),
                        "queue_id": "q_123",
                        "owner_pid": 123,
                        "process_start_ticks": 456,
                        "source": "direct_run",
                        "acquired_at": "2026-03-20T00:00:00+00:00",
                        "app_name": "chemstack_orca",
                        "task_id": "task_123",
                        "workflow_id": "wf_123",
                    }
                ],
            )

        self.assertEqual(captured["root"], root)
        slots = captured["slots"]
        self.assertIsInstance(slots, list)
        assert isinstance(slots, list)
        self.assertEqual(len(slots), 1)
        saved_slot = slots[0]
        self.assertIsInstance(saved_slot, FakeChemCoreSlot)
        assert isinstance(saved_slot, FakeChemCoreSlot)
        self.assertEqual(saved_slot.work_dir, str(root / "rxn"))
        self.assertEqual(saved_slot.task_id, "task_123")
        self.assertEqual(saved_slot.queue_id, "q_123")

    def test_reserve_slot_persists_backend_compatible_metadata_when_available(self) -> None:
        @dataclass(frozen=True)
        class FakeChemCoreSlot:
            token: str
            owner_pid: int
            process_start_ticks: int | None
            source: str
            acquired_at: str
            app_name: str = ""
            task_id: str = ""
            workflow_id: str = ""
            state: str = "active"
            work_dir: str = ""
            queue_id: str = ""

        captured: dict[str, object] = {}

        def _fake_save_slots(root: Path, slots: list[FakeChemCoreSlot]) -> None:
            captured["root"] = root
            captured["slots"] = slots

        fake_backend = SimpleNamespace(
            AdmissionSlot=FakeChemCoreSlot,
            _load_slots=lambda root: [],
            _save_slots=_fake_save_slots,
        )

        with tempfile.TemporaryDirectory() as tmp, patch(
            "chemstack.orca.admission_store._chem_core_admission_module",
            return_value=fake_backend,
        ):
            root = Path(tmp)
            token = reserve_slot(
                root,
                2,
                reaction_dir=str(root / "rxn"),
                queue_id="q_123",
                source="queue_worker",
                app_name="chemstack_orca",
                task_id="task_123",
                workflow_id="wf_123",
            )

        self.assertIsNotNone(token)
        self.assertEqual(captured["root"], root)
        slots = captured["slots"]
        self.assertIsInstance(slots, list)
        assert isinstance(slots, list)
        self.assertEqual(len(slots), 1)
        saved_slot = slots[0]
        self.assertIsInstance(saved_slot, FakeChemCoreSlot)
        assert isinstance(saved_slot, FakeChemCoreSlot)
        self.assertEqual(saved_slot.token, token)
        self.assertEqual(saved_slot.state, "reserved")
        self.assertEqual(saved_slot.work_dir, str(root / "rxn"))
        self.assertEqual(saved_slot.queue_id, "q_123")
        self.assertEqual(saved_slot.app_name, "chemstack_orca")
        self.assertEqual(saved_slot.task_id, "task_123")
        self.assertEqual(saved_slot.workflow_id, "wf_123")

    def test_reserve_slot_keeps_external_run_limit_with_backend_available(self) -> None:
        @dataclass(frozen=True)
        class FakeChemCoreSlot:
            token: str
            owner_pid: int
            process_start_ticks: int | None
            source: str
            acquired_at: str
            app_name: str = ""
            task_id: str = ""
            workflow_id: str = ""
            state: str = "active"
            work_dir: str = ""
            queue_id: str = ""

        save_slots = MagicMock()
        fake_backend = SimpleNamespace(
            AdmissionSlot=FakeChemCoreSlot,
            _load_slots=lambda root: [],
            _save_slots=save_slots,
        )

        with tempfile.TemporaryDirectory() as tmp, patch(
            "chemstack.orca.admission_store._chem_core_admission_module",
            return_value=fake_backend,
        ):
            root = Path(tmp)
            direct_dir = root / "direct_active"
            direct_dir.mkdir()
            (direct_dir / "run.lock").write_text(json.dumps({"pid": os.getpid()}), encoding="utf-8")

            token = reserve_slot(
                root,
                1,
                reaction_dir=str(root / "queued"),
                source="queue_worker",
            )

        self.assertIsNone(token)
        save_slots.assert_called_once()
        self.assertEqual(save_slots.call_args.args[0], root)
        self.assertEqual(save_slots.call_args.args[1], [])

    def test_activate_slot_delegates_to_chem_core_backend_when_available(self) -> None:
        activate_reserved = MagicMock(return_value=object())
        fake_backend = SimpleNamespace(activate_reserved_slot=activate_reserved)

        with tempfile.TemporaryDirectory() as tmp, patch(
            "chemstack.orca.admission_store._chem_core_admission_module",
            return_value=fake_backend,
        ):
            root = Path(tmp)
            updated = activate_slot(
                root,
                "slot_123",
                reaction_dir=str(root / "rxn"),
                source="queue_run",
                owner_pid=4321,
                queue_id="q_123",
            )

        self.assertTrue(updated)
        activate_reserved.assert_called_once_with(
            root,
            "slot_123",
            state="active",
            work_dir=str(root / "rxn"),
            queue_id="q_123",
            owner_pid=4321,
            source="queue_run",
        )

    def test_activate_slot_with_metadata_uses_backend_then_updates_metadata(self) -> None:
        activate_reserved = MagicMock(return_value=object())
        fake_backend = SimpleNamespace(activate_reserved_slot=activate_reserved)

        with tempfile.TemporaryDirectory() as tmp, patch(
            "chemstack.orca.admission_store._chem_core_admission_module",
            return_value=fake_backend,
        ), patch(
            "chemstack.orca.admission_store.update_slot_metadata",
            return_value=True,
        ) as mock_update_metadata:
            root = Path(tmp)
            updated = activate_slot(
                root,
                "slot_meta",
                reaction_dir=str(root / "rxn"),
                source="queue_run",
                owner_pid=4321,
                queue_id="q_meta",
                app_name="chemstack_orca",
                task_id="task_meta",
                workflow_id="wf_meta",
            )

        self.assertTrue(updated)
        activate_reserved.assert_called_once_with(
            root,
            "slot_meta",
            state="active",
            work_dir=str(root / "rxn"),
            queue_id="q_meta",
            owner_pid=4321,
            source="queue_run",
        )
        mock_update_metadata.assert_called_once_with(
            root,
            "slot_meta",
            queue_id="q_meta",
            app_name="chemstack_orca",
            task_id="task_meta",
            workflow_id="wf_meta",
        )

    def test_release_slot_delegates_to_chem_core_backend_when_available(self) -> None:
        release = MagicMock(return_value=True)
        fake_backend = SimpleNamespace(release_slot=release)

        with tempfile.TemporaryDirectory() as tmp, patch(
            "chemstack.orca.admission_store._chem_core_admission_module",
            return_value=fake_backend,
        ):
            root = Path(tmp)
            released = release_slot(root, "slot_123")

        self.assertTrue(released)
        release.assert_called_once_with(root, "slot_123")

    def test_reconcile_stale_slots_delegates_to_chem_core_backend_when_available(self) -> None:
        reconcile = MagicMock(return_value=2)
        fake_backend = SimpleNamespace(reconcile_stale_slots=reconcile)

        with tempfile.TemporaryDirectory() as tmp, patch(
            "chemstack.orca.admission_store._chem_core_admission_module",
            return_value=fake_backend,
        ):
            root = Path(tmp)
            removed = reconcile_stale_slots(root)

        self.assertEqual(removed, 2)
        reconcile.assert_called_once_with(root)

    def test_update_slot_metadata_populates_reserved_slot_identity(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            token = reserve_slot(root, 1, source="queue_worker")
            self.assertIsNotNone(token)

            updated = admission_update_slot_metadata(
                root,
                token or "",
                queue_id="q_123",
                app_name="chemstack_orca",
                task_id="orca_task_123",
                workflow_id="wf_123",
            )

            self.assertTrue(updated)
            slots = list_slots(root)
            self.assertEqual(len(slots), 1)
            self.assertEqual(slots[0]["queue_id"], "q_123")
            self.assertEqual(slots[0]["app_name"], "chemstack_orca")
            self.assertEqual(slots[0]["task_id"], "orca_task_123")
            self.assertEqual(slots[0]["workflow_id"], "wf_123")

    @patch("chemstack.orca.admission_store.is_process_alive", return_value=True)
    def test_list_slots_reads_chem_core_backend_slots_when_available(self, mock_alive: MagicMock) -> None:
        @dataclass(frozen=True)
        class FakeChemCoreSlot:
            token: str
            owner_pid: int
            process_start_ticks: int | None
            source: str
            acquired_at: str
            app_name: str = ""
            task_id: str = ""
            workflow_id: str = ""
            state: str = "active"
            work_dir: str = ""
            queue_id: str = ""

        with tempfile.TemporaryDirectory() as tmp:
            list_slots_backend = MagicMock(
                return_value=[
                    FakeChemCoreSlot(
                        token="slot_backend",
                        owner_pid=os.getpid(),
                        process_start_ticks=None,
                        source="queue_run",
                        acquired_at="2026-03-20T00:00:00+00:00",
                        app_name="chemstack_orca",
                        task_id="task_backend",
                        workflow_id="wf_backend",
                        state="active",
                        work_dir=str(Path(tmp) / "rxn"),
                        queue_id="q_backend",
                    )
                ]
            )
            with patch(
            "chemstack.orca.admission_store._chem_core_admission_module",
            return_value=SimpleNamespace(
                list_slots=list_slots_backend,
                _load_slots=MagicMock(side_effect=AssertionError("backend._load_slots should not be used")),
            ),
            ):
                root = Path(tmp)
                slots = list_slots(root)

        self.assertEqual(len(slots), 1)
        self.assertEqual(slots[0]["reaction_dir"], str(Path(tmp) / "rxn"))
        self.assertEqual(slots[0]["work_dir"], str(Path(tmp) / "rxn"))
        self.assertEqual(slots[0]["app_name"], "chemstack_orca")
        self.assertEqual(slots[0]["task_id"], "task_backend")
        self.assertEqual(slots[0]["workflow_id"], "wf_backend")
        self.assertEqual(slots[0]["queue_id"], "q_backend")
        list_slots_backend.assert_called_once_with(Path(tmp))
        mock_alive.assert_not_called()

    def test_active_slot_count_delegates_to_chem_core_backend_when_available(self) -> None:
        active_count = MagicMock(return_value=3)
        fake_backend = SimpleNamespace(active_slot_count=active_count)

        with tempfile.TemporaryDirectory() as tmp, patch(
            "chemstack.orca.admission_store._chem_core_admission_module",
            return_value=fake_backend,
        ):
            root = Path(tmp)
            count = active_slot_count(root)

        self.assertEqual(count, 3)
        active_count.assert_called_once_with(root)

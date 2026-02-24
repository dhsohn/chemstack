from __future__ import annotations

import json
import os
import queue
import tempfile
import threading
import time
import unittest
from pathlib import Path
from unittest.mock import patch

from core.notifier import (
    render_message,
    make_event_id,
    event_run_started,
    event_attempt_completed,
    event_run_terminal,
    event_heartbeat,
    load_dedup_state,
    save_dedup_state,
    is_duplicate,
    mark_sent,
    compact_dedup_state,
    _overflow_drop,
    create_notifier,
    send_batch_summary,
    EVT_RUN_STARTED,
    EVT_ATTEMPT_COMPLETED,
    EVT_RUN_COMPLETED,
    EVT_RUN_FAILED,
    EVT_RUN_INTERRUPTED,
    EVT_HEARTBEAT,
)
from core.config import MonitoringConfig
from core.telegram_client import SendResult


class TestMakeEventId(unittest.TestCase):
    def test_simple(self):
        self.assertEqual(make_event_id("run1", "run_started"), "run1:run_started")

    def test_with_suffix(self):
        self.assertEqual(
            make_event_id("run1", "attempt_completed", "3"),
            "run1:attempt_completed:3",
        )


class TestEventPayloads(unittest.TestCase):
    def test_run_started(self):
        evt = event_run_started("run_001", "/dir", "input.inp")
        self.assertEqual(evt["event_id"], "run_001:run_started")
        self.assertEqual(evt["event_type"], EVT_RUN_STARTED)
        self.assertEqual(evt["run_id"], "run_001")
        self.assertIn("timestamp", evt)

    def test_attempt_completed(self):
        evt = event_attempt_completed(
            "run_001", "/dir", "input.inp",
            attempt_index=3, analyzer_status="completed", analyzer_reason="normal",
        )
        self.assertEqual(evt["event_id"], "run_001:attempt_completed:3")
        self.assertEqual(evt["event_type"], EVT_ATTEMPT_COMPLETED)
        self.assertEqual(evt["attempt_index"], 3)

    def test_run_completed(self):
        evt = event_run_terminal(
            EVT_RUN_COMPLETED, "run_001", "/dir", "input.inp",
            status="completed", reason="normal", attempt_count=2,
        )
        self.assertEqual(evt["event_id"], "run_001:run_completed")
        self.assertEqual(evt["attempt_count"], 2)

    def test_run_failed(self):
        evt = event_run_terminal(
            EVT_RUN_FAILED, "run_001", "/dir", "input.inp",
            status="failed", reason="retry_limit", attempt_count=5,
        )
        self.assertEqual(evt["event_id"], "run_001:run_failed")

    def test_run_interrupted(self):
        evt = event_run_terminal(
            EVT_RUN_INTERRUPTED, "run_001", "/dir", "input.inp",
            status="failed", reason="interrupted_by_user", attempt_count=1,
        )
        self.assertEqual(evt["event_id"], "run_001:run_interrupted")

    def test_heartbeat(self):
        evt = event_heartbeat(
            "run_001", "/dir", "input.inp",
            status="running", attempt_count=1, elapsed_sec=120.5,
        )
        self.assertTrue(evt["event_id"].startswith("run_001:heartbeat:"))
        self.assertEqual(evt["elapsed_sec"], 120.5)


class TestRenderMessage(unittest.TestCase):
    def test_started(self):
        evt = event_run_started("run_001", "/home/user/rxn", "rxn.inp")
        msg = render_message(evt)
        self.assertIn("[orca_auto] started", msg)
        self.assertIn("run_001", msg)
        self.assertIn("/home/user/rxn", msg)

    def test_attempt_completed(self):
        evt = event_attempt_completed(
            "run_001", "/dir", "inp",
            attempt_index=2, analyzer_status="error_scf", analyzer_reason="scf failed",
        )
        msg = render_message(evt)
        self.assertIn("attempt 2 done", msg)
        self.assertIn("error_scf", msg)

    def test_completed(self):
        evt = event_run_terminal(
            EVT_RUN_COMPLETED, "run_001", "/dir", "inp",
            status="completed", reason="normal", attempt_count=2,
        )
        msg = render_message(evt)
        self.assertIn("[orca_auto] completed", msg)
        self.assertIn("attempts=2", msg)

    def test_failed(self):
        evt = event_run_terminal(
            EVT_RUN_FAILED, "run_001", "/dir", "inp",
            status="failed", reason="retry_limit", attempt_count=5,
        )
        msg = render_message(evt)
        self.assertIn("[orca_auto] failed", msg)
        self.assertIn("retry_limit", msg)

    def test_interrupted(self):
        evt = event_run_terminal(
            EVT_RUN_INTERRUPTED, "run_001", "/dir", "inp",
            status="failed", reason="interrupted_by_user", attempt_count=1,
        )
        msg = render_message(evt)
        self.assertIn("[orca_auto] interrupted", msg)

    def test_heartbeat(self):
        evt = event_heartbeat(
            "run_001", "/dir", "inp",
            status="running", attempt_count=1, elapsed_sec=120.5,
        )
        msg = render_message(evt)
        self.assertIn("[orca_auto] heartbeat", msg)
        self.assertIn("elapsed_sec=120.5", msg)

    def test_path_masking(self):
        evt = event_run_started("run_001", "/home/user/secret/rxn", "rxn.inp")
        msg = render_message(evt, mask_paths=True)
        self.assertNotIn("/home/user/secret", msg)
        self.assertIn("rxn", msg)

    def test_unknown_event(self):
        evt = {"event_type": "custom_type", "run_id": "r1"}
        msg = render_message(evt)
        self.assertIn("custom_type", msg)
        self.assertIn("r1", msg)


class TestDedupState(unittest.TestCase):
    def test_load_empty_returns_default(self):
        with tempfile.TemporaryDirectory() as td:
            state = load_dedup_state(Path(td))
            self.assertEqual(state, {"sent_event_ids": {}})

    def test_save_and_load_roundtrip(self):
        with tempfile.TemporaryDirectory() as td:
            p = Path(td)
            state = {"sent_event_ids": {"evt1": "2026-01-01T00:00:00+00:00"}}
            save_dedup_state(p, state)
            loaded = load_dedup_state(p)
            self.assertEqual(loaded["sent_event_ids"]["evt1"], "2026-01-01T00:00:00+00:00")

    def test_corrupt_file_backed_up(self):
        with tempfile.TemporaryDirectory() as td:
            p = Path(td)
            (p / ".notify_state.json").write_text("not json!!!", encoding="utf-8")
            state = load_dedup_state(p)
            self.assertEqual(state, {"sent_event_ids": {}})
            corrupt_files = list(p.glob(".notify_state.corrupt.*"))
            self.assertEqual(len(corrupt_files), 1)

    def test_is_duplicate(self):
        state = {"sent_event_ids": {"evt1": "ts"}}
        self.assertTrue(is_duplicate(state, "evt1"))
        self.assertFalse(is_duplicate(state, "evt2"))

    def test_mark_sent(self):
        state = {"sent_event_ids": {}}
        mark_sent(state, "evt1")
        self.assertIn("evt1", state["sent_event_ids"])

    def test_compact_removes_old(self):
        old_ts = "2020-01-01T00:00:00+00:00"
        recent_ts = "2099-01-01T00:00:00+00:00"
        state = {"sent_event_ids": {"old": old_ts, "new": recent_ts}}
        removed = compact_dedup_state(state, ttl_sec=86400)
        self.assertEqual(removed, 1)
        self.assertNotIn("old", state["sent_event_ids"])
        self.assertIn("new", state["sent_event_ids"])

    def test_compact_removes_unparseable(self):
        state = {"sent_event_ids": {"bad": "not-a-date", "good": "2099-01-01T00:00:00+00:00"}}
        removed = compact_dedup_state(state, ttl_sec=86400)
        self.assertEqual(removed, 1)
        self.assertNotIn("bad", state["sent_event_ids"])

    def test_save_merges_existing_entries(self):
        with tempfile.TemporaryDirectory() as td:
            p = Path(td)
            save_dedup_state(p, {"sent_event_ids": {"evt1": "2026-01-01T00:00:00+00:00"}})
            save_dedup_state(p, {"sent_event_ids": {"evt2": "2026-01-02T00:00:00+00:00"}})
            loaded = load_dedup_state(p)
            self.assertIn("evt1", loaded["sent_event_ids"])
            self.assertIn("evt2", loaded["sent_event_ids"])
            self.assertTrue((p / ".notify_state.lock").exists())


class TestQueueOverflow(unittest.TestCase):
    def test_heartbeat_dropped_first(self):
        q = queue.Queue(maxsize=2)
        q.put({"event_type": EVT_HEARTBEAT, "event_id": "hb1"})
        q.put({"event_type": EVT_ATTEMPT_COMPLETED, "event_id": "ac1"})
        new_evt = {"event_type": EVT_RUN_COMPLETED, "event_id": "rc1"}
        result = _overflow_drop(q, new_evt)
        self.assertTrue(result)
        # Queue should now have ac1 only (heartbeat dropped)
        self.assertEqual(q.qsize(), 1)

    def test_attempt_completed_dropped_for_preserve_event(self):
        q = queue.Queue(maxsize=2)
        q.put({"event_type": EVT_ATTEMPT_COMPLETED, "event_id": "ac1"})
        q.put({"event_type": EVT_ATTEMPT_COMPLETED, "event_id": "ac2"})
        new_evt = {"event_type": EVT_RUN_COMPLETED, "event_id": "rc1"}
        result = _overflow_drop(q, new_evt)
        self.assertTrue(result)
        self.assertEqual(q.qsize(), 1)

    def test_new_heartbeat_dropped_if_queue_full(self):
        q = queue.Queue(maxsize=2)
        q.put({"event_type": EVT_RUN_STARTED, "event_id": "rs1"})
        q.put({"event_type": EVT_RUN_COMPLETED, "event_id": "rc1"})
        new_evt = {"event_type": EVT_HEARTBEAT, "event_id": "hb1"}
        result = _overflow_drop(q, new_evt)
        self.assertFalse(result)

    def test_no_drop_when_only_preserve_events(self):
        q = queue.Queue(maxsize=2)
        q.put({"event_type": EVT_RUN_STARTED, "event_id": "rs1"})
        q.put({"event_type": EVT_RUN_COMPLETED, "event_id": "rc1"})
        new_evt = {"event_type": EVT_ATTEMPT_COMPLETED, "event_id": "ac1"}
        result = _overflow_drop(q, new_evt)
        self.assertFalse(result)


class TestCreateNotifier(unittest.TestCase):
    def test_disabled_returns_none(self):
        mon = MonitoringConfig(enabled=False)
        with tempfile.TemporaryDirectory() as td:
            result = create_notifier(mon, Path(td), "run_001", "inp", {})
        self.assertIsNone(result)

    @patch.dict(os.environ, {}, clear=True)
    def test_missing_env_returns_none(self):
        mon = MonitoringConfig(enabled=True)
        with tempfile.TemporaryDirectory() as td:
            result = create_notifier(mon, Path(td), "run_001", "inp", {})
        self.assertIsNone(result)

    @patch.dict(os.environ, {
        "ORCA_AUTO_TELEGRAM_BOT_TOKEN": "tok",
        "ORCA_AUTO_TELEGRAM_CHAT_ID": "not_numeric",
    })
    def test_invalid_chat_id_returns_none(self):
        mon = MonitoringConfig(enabled=True)
        with tempfile.TemporaryDirectory() as td:
            result = create_notifier(mon, Path(td), "run_001", "inp", {})
        self.assertIsNone(result)

    @patch.dict(os.environ, {
        "ORCA_AUTO_TELEGRAM_BOT_TOKEN": "tok",
        "ORCA_AUTO_TELEGRAM_CHAT_ID": "-123456",
    })
    def test_negative_chat_id_accepted(self):
        mon = MonitoringConfig(enabled=True)
        with tempfile.TemporaryDirectory() as td:
            notifier = create_notifier(mon, Path(td), "run_001", "inp", {})
        self.assertIsNotNone(notifier)
        notifier.shutdown()

    @patch.dict(os.environ, {
        "ORCA_AUTO_TELEGRAM_BOT_TOKEN": "  ",
        "ORCA_AUTO_TELEGRAM_CHAT_ID": "123",
    })
    def test_blank_token_returns_none(self):
        mon = MonitoringConfig(enabled=True)
        with tempfile.TemporaryDirectory() as td:
            result = create_notifier(mon, Path(td), "run_001", "inp", {})
        self.assertIsNone(result)

    @patch.dict(os.environ, {
        "ORCA_AUTO_TELEGRAM_BOT_TOKEN": "tok",
        "ORCA_AUTO_TELEGRAM_CHAT_ID": "123456",
    })
    @patch("core.notifier.send_with_retry")
    def test_sync_delivery_mode_sends_without_worker(self, mock_send):
        mock_send.return_value = SendResult(success=True, status_code=200)
        mon = MonitoringConfig(enabled=True)
        mon.delivery.async_enabled = False
        mon.heartbeat.enabled = False
        with tempfile.TemporaryDirectory() as td:
            notifier = create_notifier(mon, Path(td), "run_001", "inp", {})
            self.assertIsNotNone(notifier)
            self.assertIsNone(notifier._worker_thread)
            evt = event_run_started("run_001", td, "inp")
            notifier.notify(evt)
            notifier.notify(evt)
            notifier.shutdown()
        self.assertEqual(mock_send.call_count, 1)

    @patch.dict(os.environ, {
        "ORCA_AUTO_TELEGRAM_BOT_TOKEN": "tok",
        "ORCA_AUTO_TELEGRAM_CHAT_ID": "123456",
    })
    @patch("core.notifier._worker_loop")
    def test_shutdown_can_enqueue_sentinel_when_queue_initially_full(self, mock_worker_loop):
        worker_started = threading.Event()

        def _slow_worker(q, _tg_config, _reaction_dir, _ttl, _mask_paths, alive_flag):
            alive_flag.set()
            worker_started.set()
            time.sleep(0.25)
            while True:
                item = q.get()
                if item is None:
                    break

        mock_worker_loop.side_effect = _slow_worker

        mon = MonitoringConfig(enabled=True)
        mon.heartbeat.enabled = False
        mon.delivery.queue_size = 1
        mon.delivery.worker_flush_timeout_sec = 1.0

        with tempfile.TemporaryDirectory() as td:
            notifier = create_notifier(mon, Path(td), "run_001", "inp", {})
            self.assertIsNotNone(notifier)
            self.assertTrue(worker_started.wait(timeout=1.0))
            notifier.notify(event_run_started("run_001", td, "inp"))
            notifier.shutdown()
            self.assertIsNotNone(notifier._worker_thread)
            self.assertFalse(notifier._worker_thread.is_alive())


class TestBatchSummary(unittest.TestCase):
    @patch.dict(os.environ, {
        "ORCA_AUTO_TELEGRAM_BOT_TOKEN": "tok",
        "ORCA_AUTO_TELEGRAM_CHAT_ID": "123456",
    })
    @patch("core.notifier.send_with_retry")
    @patch("core.notifier.logger")
    def test_logs_warning_when_batch_summary_delivery_fails(self, mock_logger, mock_send):
        mock_send.return_value = SendResult(success=False, status_code=503, error="upstream down")
        mon = MonitoringConfig(enabled=True)
        send_batch_summary(mon, "[orca_auto] organize summary")
        mock_logger.warning.assert_called_with(
            "Telegram send failed for batch summary: status=%d error=%s",
            503,
            "upstream down",
        )

    @patch.dict(os.environ, {}, clear=True)
    @patch("core.notifier.send_with_retry")
    def test_skip_when_credentials_missing(self, mock_send):
        mon = MonitoringConfig(enabled=True)
        send_batch_summary(mon, "[orca_auto] cleanup summary")
        mock_send.assert_not_called()


class TestMonitoringConfigValidation(unittest.TestCase):
    def test_default_config_loads_with_monitoring_disabled(self):
        from core.config import load_config
        import tempfile
        with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
            f.write(
                "runtime:\n"
                "  allowed_root: /tmp/a\n"
                "  organized_root: /tmp/b\n"
                "paths:\n"
                "  orca_executable: /usr/bin/orca\n"
            )
            f.flush()
            cfg = load_config(f.name)
        self.assertFalse(cfg.monitoring.enabled)
        os.unlink(f.name)

    def test_invalid_monitoring_disables_gracefully(self):
        from core.config import load_config
        import tempfile
        with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
            f.write(
                "runtime:\n"
                "  allowed_root: /tmp/a\n"
                "  organized_root: /tmp/b\n"
                "paths:\n"
                "  orca_executable: /usr/bin/orca\n"
                "monitoring:\n"
                "  enabled: true\n"
                "  telegram:\n"
                "    timeout_sec: 999\n"
            )
            f.flush()
            cfg = load_config(f.name)
        self.assertFalse(cfg.monitoring.enabled)
        os.unlink(f.name)


if __name__ == "__main__":
    unittest.main()

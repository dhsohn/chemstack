"""Tests for chemstack.orca.commands.queue foreground worker behavior."""

from __future__ import annotations

import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

from chemstack.orca.commands.queue import cmd_queue_worker
from chemstack.orca.config import AppConfig, RuntimeConfig


def _make_cfg(tmp: str) -> AppConfig:
    return AppConfig(runtime=RuntimeConfig(allowed_root=tmp))


def _make_args(tmp: str, **overrides):
    defaults = {
        "config": str(Path(tmp) / "config.yaml"),
        "auto_organize": False,
        "no_auto_organize": False,
    }
    defaults.update(overrides)
    return SimpleNamespace(**defaults)


class TestCmdQueueWorker(unittest.TestCase):
    @patch("chemstack.orca.commands.queue.load_config")
    @patch("chemstack.orca.commands.queue.read_worker_pid", return_value=12345)
    def test_worker_already_running(self, mock_pid: MagicMock, mock_load: MagicMock) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            mock_load.return_value = _make_cfg(tmp)
            args = _make_args(tmp)

            rc = cmd_queue_worker(args)

        self.assertEqual(rc, 1)

    @patch("chemstack.orca.commands.queue.load_config")
    @patch("chemstack.orca.commands.queue.read_worker_pid", return_value=None)
    @patch("chemstack.orca.commands.queue.QueueWorker")
    def test_worker_runs_in_foreground_only(
        self,
        mock_worker_cls: MagicMock,
        mock_pid: MagicMock,
        mock_load: MagicMock,
    ) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            mock_load.return_value = _make_cfg(tmp)
            mock_worker_cls.return_value.run.return_value = 0
            args = _make_args(tmp)

            rc = cmd_queue_worker(args)

        self.assertEqual(rc, 0)
        mock_worker_cls.assert_called_once_with(
            mock_load.return_value,
            args.config,
            max_concurrent=4,
            auto_organize=False,
        )

    @patch("chemstack.orca.commands.queue.load_config")
    @patch("chemstack.orca.commands.queue.read_worker_pid", return_value=None)
    @patch("chemstack.orca.commands.queue.QueueWorker")
    def test_worker_uses_config_max_concurrent_when_flag_omitted(
        self,
        mock_worker_cls: MagicMock,
        mock_pid: MagicMock,
        mock_load: MagicMock,
    ) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            cfg = _make_cfg(tmp)
            cfg.runtime.max_concurrent = 6
            mock_load.return_value = cfg
            mock_worker_cls.return_value.run.return_value = 0
            args = _make_args(tmp)

            rc = cmd_queue_worker(args)

        self.assertEqual(rc, 0)
        mock_worker_cls.assert_called_once_with(
            cfg,
            args.config,
            max_concurrent=6,
            auto_organize=False,
        )

    @patch("chemstack.orca.commands.queue.load_config")
    @patch("chemstack.orca.commands.queue.read_worker_pid", return_value=None)
    @patch("chemstack.orca.commands.queue.QueueWorker")
    def test_worker_uses_configured_auto_organize_by_default(
        self,
        mock_worker_cls: MagicMock,
        mock_pid: MagicMock,
        mock_load: MagicMock,
    ) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            cfg = _make_cfg(tmp)
            cfg.behavior.auto_organize_on_terminal = True
            mock_load.return_value = cfg
            mock_worker_cls.return_value.run.return_value = 0
            args = _make_args(tmp)

            rc = cmd_queue_worker(args)

        self.assertEqual(rc, 0)
        mock_worker_cls.assert_called_once_with(
            cfg,
            args.config,
            max_concurrent=4,
            auto_organize=True,
        )

    @patch("chemstack.orca.commands.queue.load_config")
    @patch("chemstack.orca.commands.queue.read_worker_pid", return_value=None)
    @patch("chemstack.orca.commands.queue.QueueWorker")
    def test_worker_cli_can_enable_auto_organize(
        self,
        mock_worker_cls: MagicMock,
        mock_pid: MagicMock,
        mock_load: MagicMock,
    ) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            cfg = _make_cfg(tmp)
            mock_load.return_value = cfg
            mock_worker_cls.return_value.run.return_value = 0
            args = _make_args(tmp, auto_organize=True)

            rc = cmd_queue_worker(args)

        self.assertEqual(rc, 0)
        mock_worker_cls.assert_called_once_with(
            cfg,
            args.config,
            max_concurrent=4,
            auto_organize=True,
        )

    @patch("chemstack.orca.commands.queue.load_config")
    @patch("chemstack.orca.commands.queue.read_worker_pid", return_value=None)
    @patch("chemstack.orca.commands.queue.QueueWorker")
    def test_worker_cli_can_disable_configured_auto_organize(
        self,
        mock_worker_cls: MagicMock,
        mock_pid: MagicMock,
        mock_load: MagicMock,
    ) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            cfg = _make_cfg(tmp)
            cfg.behavior.auto_organize_on_terminal = True
            mock_load.return_value = cfg
            mock_worker_cls.return_value.run.return_value = 0
            args = _make_args(tmp, no_auto_organize=True)

            rc = cmd_queue_worker(args)

        self.assertEqual(rc, 0)
        mock_worker_cls.assert_called_once_with(
            cfg,
            args.config,
            max_concurrent=4,
            auto_organize=False,
        )

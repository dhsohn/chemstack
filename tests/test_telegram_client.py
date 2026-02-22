from __future__ import annotations

import unittest
from unittest.mock import patch

from core.telegram_client import (
    TelegramConfig,
    SendResult,
    send_with_retry,
    _truncate_text,
    _sanitize_token_from_message,
    _should_retry,
    _compute_delay,
)


class TestTruncateText(unittest.TestCase):
    def test_short_text_unchanged(self):
        self.assertEqual(_truncate_text("hello", 100), "hello")

    def test_exact_limit_unchanged(self):
        text = "a" * 3500
        self.assertEqual(_truncate_text(text, 3500), text)

    def test_long_text_truncated(self):
        result = _truncate_text("a" * 4000, 3500)
        self.assertLessEqual(len(result), 3500)
        self.assertIn("[truncated]", result)

    def test_truncated_starts_with_original(self):
        text = "a" * 4000
        result = _truncate_text(text, 3500)
        self.assertTrue(result.startswith("a"))


class TestSanitizeToken(unittest.TestCase):
    def test_token_removed(self):
        result = _sanitize_token_from_message(
            "Error at bot123:token456/sendMessage", "123:token456",
        )
        self.assertNotIn("123:token456", result)
        self.assertIn("***", result)

    def test_empty_token(self):
        msg = "some error"
        self.assertEqual(_sanitize_token_from_message(msg, ""), msg)

    def test_no_token_in_message(self):
        result = _sanitize_token_from_message("clean message", "secret")
        self.assertEqual(result, "clean message")


class TestShouldRetry(unittest.TestCase):
    def test_5xx_retries(self):
        self.assertTrue(_should_retry(SendResult(False, 500)))
        self.assertTrue(_should_retry(SendResult(False, 502)))
        self.assertTrue(_should_retry(SendResult(False, 503)))

    def test_429_retries(self):
        self.assertTrue(_should_retry(SendResult(False, 429, retry_after=5.0)))

    def test_4xx_no_retry(self):
        self.assertFalse(_should_retry(SendResult(False, 400)))
        self.assertFalse(_should_retry(SendResult(False, 403)))
        self.assertFalse(_should_retry(SendResult(False, 404)))

    def test_network_error_retries(self):
        self.assertTrue(_should_retry(SendResult(False, 0, error="timeout")))

    def test_success_no_retry(self):
        self.assertFalse(_should_retry(SendResult(True, 200)))


class TestComputeDelay(unittest.TestCase):
    def test_respects_retry_after(self):
        cfg = TelegramConfig(bot_token="t", chat_id="1")
        result = SendResult(False, 429, retry_after=10.0)
        self.assertEqual(_compute_delay(cfg, result, 0), 10.0)

    def test_exponential_backoff_no_jitter(self):
        cfg = TelegramConfig(
            bot_token="t", chat_id="1",
            retry_backoff_sec=1.0, retry_jitter_sec=0.0,
        )
        result = SendResult(False, 500)
        self.assertAlmostEqual(_compute_delay(cfg, result, 0), 1.0, delta=0.01)
        self.assertAlmostEqual(_compute_delay(cfg, result, 1), 2.0, delta=0.01)
        self.assertAlmostEqual(_compute_delay(cfg, result, 2), 4.0, delta=0.01)

    def test_jitter_adds_randomness(self):
        cfg = TelegramConfig(
            bot_token="t", chat_id="1",
            retry_backoff_sec=1.0, retry_jitter_sec=0.5,
        )
        result = SendResult(False, 500)
        delay = _compute_delay(cfg, result, 0)
        self.assertGreaterEqual(delay, 1.0)
        self.assertLessEqual(delay, 1.5)


class TestSendWithRetry(unittest.TestCase):
    @patch("core.telegram_client.send_message")
    def test_success_on_first_try(self, mock_send):
        mock_send.return_value = SendResult(True, 200)
        cfg = TelegramConfig(bot_token="t", chat_id="1")
        result = send_with_retry(cfg, "test")
        self.assertTrue(result.success)
        self.assertEqual(mock_send.call_count, 1)

    @patch("core.telegram_client.send_message")
    @patch("core.telegram_client.time.sleep")
    def test_retries_on_5xx(self, mock_sleep, mock_send):
        mock_send.side_effect = [
            SendResult(False, 500, error="server error"),
            SendResult(False, 500, error="server error"),
            SendResult(True, 200),
        ]
        cfg = TelegramConfig(bot_token="t", chat_id="1", retry_count=3)
        result = send_with_retry(cfg, "test")
        self.assertTrue(result.success)
        self.assertEqual(mock_send.call_count, 3)

    @patch("core.telegram_client.send_message")
    def test_no_retry_on_4xx(self, mock_send):
        mock_send.return_value = SendResult(False, 400, error="bad request")
        cfg = TelegramConfig(bot_token="t", chat_id="1", retry_count=3)
        result = send_with_retry(cfg, "test")
        self.assertFalse(result.success)
        self.assertEqual(mock_send.call_count, 1)

    @patch("core.telegram_client.send_message")
    @patch("core.telegram_client.time.sleep")
    def test_retry_exhaustion(self, mock_sleep, mock_send):
        mock_send.return_value = SendResult(False, 500, error="down")
        cfg = TelegramConfig(bot_token="t", chat_id="1", retry_count=2)
        result = send_with_retry(cfg, "test")
        self.assertFalse(result.success)
        # 1 initial + 2 retries = 3 calls
        self.assertEqual(mock_send.call_count, 3)

    @patch("core.telegram_client.send_message")
    @patch("core.telegram_client.time.sleep")
    def test_429_respects_retry_after(self, mock_sleep, mock_send):
        mock_send.side_effect = [
            SendResult(False, 429, retry_after=7.0),
            SendResult(True, 200),
        ]
        cfg = TelegramConfig(bot_token="t", chat_id="1", retry_count=2)
        result = send_with_retry(cfg, "test")
        self.assertTrue(result.success)
        mock_sleep.assert_called_once_with(7.0)


if __name__ == "__main__":
    unittest.main()

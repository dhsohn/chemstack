from __future__ import annotations

import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from chemstack.core.config.schema import CommonRuntimeConfig, TelegramConfig


@pytest.mark.parametrize(
    ("allowed_root", "organized_root", "admission_root", "expected_root"),
    [
        ("/allowed", "/organized", None, "/allowed"),
        ("/allowed", "/organized", "/custom", "/custom"),
    ],
)
def test_common_runtime_config_resolved_admission_root(
    allowed_root: str,
    organized_root: str,
    admission_root: str | None,
    expected_root: str,
) -> None:
    config = CommonRuntimeConfig(
        allowed_root=allowed_root,
        organized_root=organized_root,
        admission_root=admission_root,
    )

    assert config.resolved_admission_root == expected_root


@pytest.mark.parametrize(
    ("max_concurrent", "admission_limit", "expected_limit"),
    [
        (4, None, 4),
        (0, None, 1),
        (3, -7, 1),
        (3, 2, 2),
    ],
)
def test_common_runtime_config_resolved_admission_limit_lower_bounds(
    max_concurrent: int,
    admission_limit: int | None,
    expected_limit: int,
) -> None:
    config = CommonRuntimeConfig(
        allowed_root="/allowed",
        organized_root="/organized",
        max_concurrent=max_concurrent,
        admission_limit=admission_limit,
    )

    assert config.resolved_admission_limit == expected_limit


@pytest.mark.parametrize(
    ("bot_token", "chat_id", "expected"),
    [
        ("", "", False),
        ("token", "", False),
        ("", "chat", False),
        ("token", "chat", True),
    ],
)
def test_telegram_config_enabled(bot_token: str, chat_id: str, expected: bool) -> None:
    config = TelegramConfig(bot_token=bot_token, chat_id=chat_id)

    assert config.enabled is expected

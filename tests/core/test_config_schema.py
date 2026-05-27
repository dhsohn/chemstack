from __future__ import annotations


import pytest


from chemstack.core.config.schema import (
    CommonRuntimeConfig,
    TelegramConfig,
    as_nonempty_str,
    normalize_admission_limit,
    normalize_default_max_retries,
    normalize_max_concurrent,
)


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


def test_common_runtime_config_can_copy_common_fields() -> None:
    config = CommonRuntimeConfig(
        allowed_root="/allowed",
        organized_root="/organized",
        max_concurrent=2,
        admission_root="/admission",
        admission_limit=1,
    )

    copied = config.to_common_runtime_config()

    assert copied == config
    assert copied is not config


@pytest.mark.parametrize(
    ("value", "default", "expected"),
    [
        (" /kept ", "fallback", " /kept "),
        ("   ", "fallback", "fallback"),
        (123, "fallback", "fallback"),
        (None, "fallback", "fallback"),
    ],
)
def test_as_nonempty_str_preserves_existing_string_behavior(
    value: object,
    default: str,
    expected: str,
) -> None:
    assert as_nonempty_str(value, default) == expected


@pytest.mark.parametrize(
    ("value", "default", "expected"),
    [
        ("9", 2, 9),
        ("bad", 2, 2),
        ("-3", 2, 0),
    ],
)
def test_normalize_default_max_retries(value: object, default: int, expected: int) -> None:
    assert normalize_default_max_retries(value, default) == expected


@pytest.mark.parametrize(
    ("value", "default", "expected"),
    [
        ("6", 4, 6),
        ("0", 4, 1),
        ("bad", 4, 4),
    ],
)
def test_normalize_max_concurrent(value: object, default: int, expected: int) -> None:
    assert normalize_max_concurrent(value, default) == expected


@pytest.mark.parametrize(
    ("value", "max_concurrent", "expected"),
    [
        (None, 4, None),
        ("2", 4, 2),
        ("0", 4, 4),
        ("bad", 4, 4),
        (True, 4, 1),
    ],
)
def test_normalize_admission_limit(
    value: object,
    max_concurrent: int,
    expected: int | None,
) -> None:
    assert normalize_admission_limit(value, max_concurrent) == expected


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

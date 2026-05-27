from __future__ import annotations

from dataclasses import dataclass, replace
from typing import Any, TypeVar, cast

_RuntimeAdmissionConfigT = TypeVar("_RuntimeAdmissionConfigT", bound="RuntimeAdmissionMixin")


def as_str(value: Any, default: str = "") -> str:
    if value is None:
        return default
    return str(value).strip()


def as_nonempty_str(value: Any, default: str = "") -> str:
    if isinstance(value, str) and value.strip():
        return value
    return default


def as_int(value: Any, default: int) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def as_bool(value: Any, default: bool = False) -> bool:
    if value is None:
        return default
    if isinstance(value, bool):
        return value
    text = str(value).strip().lower()
    if text in {"1", "true", "yes", "on"}:
        return True
    if text in {"0", "false", "no", "off"}:
        return False
    return default


def as_float(value: Any, default: float) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def positive_int(value: Any) -> int | None:
    if value in (None, ""):
        return None
    try:
        parsed = int(value)
    except (TypeError, ValueError):
        return None
    return parsed if parsed > 0 else None


def normalize_default_max_retries(value: Any, default: int = 2) -> int:
    return max(0, as_int(value, default))


def normalize_max_concurrent(value: Any, default: int = 4) -> int:
    return max(1, as_int(value, default))


def normalize_admission_limit(value: Any, max_concurrent: int) -> int | None:
    if value is None:
        return None
    try:
        if isinstance(value, bool):
            normalized_limit = int(value)
        elif isinstance(value, (int, str)):
            normalized_limit = int(value)
        else:
            raise TypeError("Unsupported admission_limit type")
    except (TypeError, ValueError):
        normalized_limit = max_concurrent
    if normalized_limit < 1:
        return max(1, max_concurrent)
    return normalized_limit


def resolved_admission_limit(admission_limit: Any, max_concurrent: Any) -> int:
    if admission_limit is not None:
        return max(1, int(admission_limit))
    return max(1, int(max_concurrent))


class RuntimeAdmissionMixin:
    allowed_root: str
    organized_root: str
    max_concurrent: int
    admission_root: str | None
    admission_limit: int | None

    @property
    def resolved_admission_root(self) -> str:
        return self.admission_root or self.allowed_root

    @property
    def resolved_admission_limit(self) -> int:
        return resolved_admission_limit(self.admission_limit, self.max_concurrent)

    def to_common_runtime_config(self: _RuntimeAdmissionConfigT) -> _RuntimeAdmissionConfigT:
        return cast(_RuntimeAdmissionConfigT, replace(cast(Any, self)))


@dataclass(frozen=True)
class CommonRuntimeConfig(RuntimeAdmissionMixin):
    allowed_root: str
    organized_root: str
    max_concurrent: int = 4
    admission_root: str | None = None
    admission_limit: int | None = None


@dataclass(frozen=True)
class CommonResourceConfig:
    max_cores_per_task: int = 8
    max_memory_gb_per_task: int = 32


@dataclass(frozen=True)
class EmptyBehaviorConfig:
    pass


@dataclass(frozen=True)
class TelegramConfig:
    bot_token: str = ""
    chat_id: str = ""
    timeout_seconds: float = 5.0
    max_attempts: int = 2
    retry_backoff_seconds: float = 0.5

    @property
    def enabled(self) -> bool:
        return bool(self.bot_token and self.chat_id)

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class CommonRuntimeConfig:
    allowed_root: str
    organized_root: str
    max_concurrent: int = 4
    admission_root: str | None = None
    admission_limit: int | None = None

    @property
    def resolved_admission_root(self) -> str:
        return self.admission_root or self.allowed_root

    @property
    def resolved_admission_limit(self) -> int:
        if self.admission_limit is not None:
            return max(1, int(self.admission_limit))
        return max(1, int(self.max_concurrent))


@dataclass(frozen=True)
class CommonResourceConfig:
    max_cores_per_task: int = 8
    max_memory_gb_per_task: int = 32


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

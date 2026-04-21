from __future__ import annotations

import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pytest

from chemstack.core.utils import persistence


FIXED_NOW = datetime(2026, 4, 19, 12, 34, 56, tzinfo=timezone.utc)


class _FixedDatetime:
    @classmethod
    def now(cls, tz: timezone | None = None) -> datetime:
        assert tz is timezone.utc
        return FIXED_NOW


def test_now_utc_iso_returns_utc_isoformat(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(persistence, "datetime", _FixedDatetime)

    assert persistence.now_utc_iso() == "2026-04-19T12:34:56+00:00"


def test_timestamped_token_uses_timestamp_and_token_hex(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(persistence, "datetime", _FixedDatetime)
    monkeypatch.setattr(persistence, "token_hex", lambda n: "abc123")

    assert persistence.timestamped_token("job") == "job_20260419_123456_abc123"


@pytest.mark.parametrize(
    ("value", "default", "expected"),
    [
        ("7", 0, 7),
        (3.9, 0, 3),
        ("bad", 11, 11),
        (None, -1, -1),
    ],
)
def test_coerce_int(value: Any, default: int, expected: int) -> None:
    assert persistence.coerce_int(value, default=default) == expected


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        ("12", 12),
        ("oops", None),
        (None, None),
    ],
)
def test_coerce_optional_int(value: Any, expected: int | None) -> None:
    assert persistence.coerce_optional_int(value) == expected


@pytest.mark.parametrize(
    ("value", "default", "expected"),
    [
        (True, False, True),
        (False, True, False),
        (3, False, True),
        (0.0, True, False),
        (" yes ", False, True),
        ("OFF", True, False),
        ("", True, False),
        ("maybe", True, True),
        (object(), False, False),
    ],
)
def test_coerce_bool(value: Any, default: bool, expected: bool) -> None:
    assert persistence.coerce_bool(value, default=default) is expected


def test_resolve_root_path_expands_user_and_resolves(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    home = tmp_path / "home"
    home.mkdir()
    root = home / "chem-core-root"
    root.mkdir()
    monkeypatch.setenv("HOME", str(home))

    assert persistence.resolve_root_path("~/chem-core-root/..") == home.resolve()
    assert persistence.resolve_root_path(root) == root.resolve()


def test_atomic_write_json_success_path(tmp_path: Path) -> None:
    path = tmp_path / "nested" / "payload.json"

    persistence.atomic_write_json(
        path,
        {"b": 1, "a": [1, 2], "text": "café"},
    )

    assert path.parent.exists()
    assert path.read_text(encoding="utf-8") == (
        '{\n'
        '  "b": 1,\n'
        '  "a": [\n'
        '    1,\n'
        '    2\n'
        '  ],\n'
        '  "text": "caf\\u00e9"\n'
        "}"
    )


def test_atomic_write_json_cleans_up_tmp_when_replace_fails(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    path = tmp_path / "nested" / "payload.json"
    created_tmp: list[Path] = []

    def fake_mkstemp(prefix: str, suffix: str, dir: str) -> tuple[int, str]:
        tmp_path_local = Path(dir) / f"{prefix}tmp{suffix}"
        fd = os.open(tmp_path_local, os.O_RDWR | os.O_CREAT | os.O_TRUNC, 0o600)
        created_tmp.append(tmp_path_local)
        return fd, str(tmp_path_local)

    def fake_replace(src: Path, dst: Path) -> None:
        raise OSError("replace failed")

    monkeypatch.setattr(persistence.tempfile, "mkstemp", fake_mkstemp)
    monkeypatch.setattr(persistence.os, "replace", fake_replace)

    with pytest.raises(OSError, match="replace failed"):
        persistence.atomic_write_json(path, {"ok": True})

    assert created_tmp
    assert not created_tmp[0].exists()
    assert not path.exists()


def test_atomic_write_json_swallows_unlink_oserror(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    path = tmp_path / "nested" / "payload.json"
    created_tmp: list[Path] = []
    original_unlink = Path.unlink

    def fake_mkstemp(prefix: str, suffix: str, dir: str) -> tuple[int, str]:
        tmp_path_local = Path(dir) / f"{prefix}tmp{suffix}"
        fd = os.open(tmp_path_local, os.O_RDWR | os.O_CREAT | os.O_TRUNC, 0o600)
        created_tmp.append(tmp_path_local)
        return fd, str(tmp_path_local)

    def fake_replace(src: Path, dst: Path) -> None:
        raise OSError("replace failed")

    def fake_unlink(self: Path, *args: Any, **kwargs: Any) -> None:
        if created_tmp and self == created_tmp[0]:
            original_unlink(self, *args, **kwargs)
            raise OSError("unlink failed")
        original_unlink(self, *args, **kwargs)

    monkeypatch.setattr(persistence.tempfile, "mkstemp", fake_mkstemp)
    monkeypatch.setattr(persistence.os, "replace", fake_replace)
    monkeypatch.setattr(Path, "unlink", fake_unlink)

    with pytest.raises(OSError, match="replace failed"):
        persistence.atomic_write_json(path, {"ok": True})

    assert created_tmp
    assert not created_tmp[0].exists()
    assert not path.exists()

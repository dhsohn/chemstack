from __future__ import annotations

import shutil
import unicodedata
from datetime import datetime, timezone
from typing import Any, Sequence

from chemstack.core.activity_icons import activity_status_icon
from chemstack.core.statuses import QUEUE_ACTIVE_STATUSES
from chemstack.core.utils import normalize_text
from chemstack.flow.templates import workflow_template_label

_ORCA_SELECTED_INP_HINTS = (
    ("neb", "NEB"),
    ("irc", "IRC"),
    ("ts", "TS"),
    ("opt", "Opt"),
    ("freq", "Freq"),
)


def _queue_table_now() -> datetime:
    return datetime.now(timezone.utc)


def _parse_activity_timestamp(value: Any) -> datetime | None:
    text = normalize_text(value)
    if not text:
        return None
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _queue_elapsed_started_at(item: dict[str, Any]) -> datetime | None:
    metadata = item.get("metadata")
    metadata = metadata if isinstance(metadata, dict) else {}
    restart_summary = metadata.get("restart_summary")
    restart_summary = restart_summary if isinstance(restart_summary, dict) else {}
    for value in (
        metadata.get("elapsed_started_at"),
        metadata.get("last_restarted_at"),
        restart_summary.get("restarted_at"),
        item.get("submitted_at"),
        item.get("updated_at"),
    ):
        parsed = _parse_activity_timestamp(value)
        if parsed is not None:
            return parsed
    return None


def _queue_elapsed_text(item: dict[str, Any], *, now: datetime | None = None) -> str:
    started_at = _queue_elapsed_started_at(item)
    if started_at is None:
        return "--:--:--"

    status = normalize_text(item.get("status")).lower()
    end_at = _parse_activity_timestamp(item.get("updated_at"))
    if status in QUEUE_ACTIVE_STATUSES or end_at is None:
        end_at = now or _queue_table_now()
    if end_at < started_at:
        end_at = started_at
    total_seconds = max(0, int((end_at - started_at).total_seconds()))
    hours = total_seconds // 3600
    minutes = (total_seconds % 3600) // 60
    seconds = total_seconds % 60
    return f"{hours:02d}:{minutes:02d}:{seconds:02d}"


def _queue_status_icon(item: dict[str, Any]) -> str:
    return activity_status_icon(item.get("status"))


def _queue_task_label(task_kind: Any) -> str:
    normalized = normalize_text(task_kind).lower()
    return {
        "crest_conformer_search": "conformer_search",
        "conformer_search": "conformer_search",
        "path_search": "TS path",
        "xtb_path_search": "TS path",
        "optts_freq": "OptTS+Freq",
        "optts": "OptTS",
        "ts": "TS",
        "opt": "Opt",
        "sp": "SP",
        "freq": "Freq",
        "irc": "IRC",
        "neb": "NEB",
        "orca": "ORCA",
        "xtb": "xTB",
        "crest": "CREST",
    }.get(normalized, normalize_text(task_kind) or "")


def _infer_orca_detail_from_metadata(metadata: dict[str, Any]) -> str:
    task_kind = normalize_text(metadata.get("task_kind")).lower()
    task_label = _queue_task_label(task_kind)
    if task_label and task_kind not in {"orca_run_inp", "run_inp"}:
        return task_label

    job_type = normalize_text(metadata.get("job_type")).lower()
    job_type_label = _queue_task_label(job_type)
    if job_type_label and job_type not in {"other", "unknown"}:
        return job_type_label
    selected_inp_name = normalize_text(
        metadata.get("selected_inp_name") or metadata.get("selected_inp")
    )
    lowered = selected_inp_name.lower()
    for marker, label in _ORCA_SELECTED_INP_HINTS:
        if marker in lowered:
            return label
    return "ORCA"


def _workflow_detail_text(metadata: dict[str, Any]) -> str:
    base = workflow_template_label(metadata.get("template_name"))
    request_parameters = metadata.get("request_parameters")
    request_parameters = request_parameters if isinstance(request_parameters, dict) else {}
    crest_mode = normalize_text(request_parameters.get("crest_mode"))
    return f"{base}({crest_mode})" if crest_mode else base


def _crest_detail_text(metadata: dict[str, Any]) -> str:
    base = _queue_task_label(metadata.get("task_kind")) or "conformer_search"
    mode = normalize_text(metadata.get("mode"))
    return f"{base}({mode})" if mode else base


def _xtb_detail_text(metadata: dict[str, Any]) -> str:
    return (
        _queue_task_label(metadata.get("task_kind"))
        or _queue_task_label(metadata.get("job_type"))
        or "xTB"
    )


_QUEUE_ENGINE_DETAIL_TEXT = {
    "crest": _crest_detail_text,
    "xtb": _xtb_detail_text,
    "orca": _infer_orca_detail_from_metadata,
}


def _queue_detail_text(item: dict[str, Any]) -> str:
    kind = normalize_text(item.get("kind")).lower()
    engine = normalize_text(item.get("engine")).lower()
    metadata = item.get("metadata")
    metadata = metadata if isinstance(metadata, dict) else {}

    if kind == "workflow":
        return _workflow_detail_text(metadata)
    if detail_text := _QUEUE_ENGINE_DETAIL_TEXT.get(engine):
        return detail_text(metadata)
    return normalize_text(item.get("label")) or normalize_text(item.get("source")) or "-"


def _queue_looks_like_path(value: str) -> bool:
    text = normalize_text(value)
    return "/" in text or "\\" in text


def _queue_path_name(value: Any) -> str:
    text = normalize_text(value)
    if not text:
        return ""
    normalized = text.replace("\\", "/").rstrip("/")
    if not normalized:
        return ""
    return normalized.rsplit("/", 1)[-1]


def _queue_metadata_path_name(metadata: dict[str, Any], keys: Sequence[str]) -> str:
    for key in keys:
        name = _queue_path_name(metadata.get(key))
        if name and name not in {"reaction_dir", "workflow.json"}:
            return name
    return ""


def _queue_name_text(item: dict[str, Any]) -> str:
    activity_id = normalize_text(item.get("activity_id")) or "-"
    kind = normalize_text(item.get("kind")).lower()
    label = normalize_text(item.get("label"))
    metadata = item.get("metadata")
    metadata = metadata if isinstance(metadata, dict) else {}

    if label and not _queue_looks_like_path(label):
        return label

    if kind == "workflow":
        workspace_name = _queue_metadata_path_name(metadata, ("workspace_dir", "workflow_file"))
        if workspace_name:
            return workspace_name
        return activity_id

    path_name = _queue_metadata_path_name(
        metadata,
        (
            "reaction_dir",
            "job_dir",
            "original_run_dir",
            "latest_known_path",
            "organized_output_dir",
        ),
    )
    if path_name:
        return path_name

    label_name = _queue_path_name(label)
    if label_name and label_name not in {"reaction_dir", "workflow.json"}:
        return label_name

    return activity_id


def _queue_truncate(value: str, *, max_width: int) -> str:
    text = normalize_text(value)
    if _queue_display_width(text) <= max_width:
        return text
    if max_width <= 3:
        return _queue_trim_to_width(text, max_width)
    return _queue_trim_to_width(text, max_width - 3) + "..."


def _queue_char_width(char: str) -> int:
    if not char:
        return 0
    if unicodedata.combining(char):
        return 0
    if unicodedata.category(char) == "Cf":
        return 0
    if unicodedata.east_asian_width(char) in {"W", "F"}:
        return 2
    return 1


def _queue_text(value: Any) -> str:
    if value is None:
        return ""
    return str(value)


def _queue_display_width(value: str) -> int:
    return sum(_queue_char_width(char) for char in _queue_text(value))


def _queue_trim_to_width(value: str, max_width: int) -> str:
    if max_width <= 0:
        return ""
    trimmed: list[str] = []
    current_width = 0
    for char in _queue_text(value):
        char_width = _queue_char_width(char)
        if current_width + char_width > max_width:
            break
        trimmed.append(char)
        current_width += char_width
    return "".join(trimmed)


def _queue_pad_right(value: str, width: int) -> str:
    padding = max(0, int(width) - _queue_display_width(value))
    return _queue_text(value) + (" " * padding)


# Gap rendered between adjacent table columns.
_QUEUE_COLUMN_GAP = "  "

# Smallest width each flexible column may shrink to under terminal-width
# pressure, and the order in which columns surrender space (least essential
# first). ``id`` shrinks last because it doubles as the ``queue cancel`` target.
_QUEUE_MIN_WIDTHS = {"detail": 6, "name": 8, "id": 8}
_QUEUE_SHRINK_ORDER = ("detail", "name", "id")
_QUEUE_HEADERS = {
    "status": "Status",
    "name": "Name",
    "detail": "Detail",
    "id": "ID",
    "elapsed": "Elapsed",
}


def _terminal_max_width() -> int | None:
    """Return the usable terminal width, or ``None`` when it cannot be detected.

    ``shutil.get_terminal_size`` honors ``COLUMNS`` first, then queries the
    attached terminal. When output is piped (no terminal) the fallback of ``0``
    is returned here as ``None`` so piped/redirected output keeps full-width
    columns and stays stable for downstream scripts.
    """

    try:
        columns = shutil.get_terminal_size(fallback=(0, 0)).columns
    except (ValueError, OSError):
        return None
    return columns if columns > 0 else None


def _fit_queue_widths(widths: dict[str, int], *, max_total: int | None) -> dict[str, int]:
    """Shrink flexible columns so the rendered row fits ``max_total`` columns.

    Columns are reduced in ``_QUEUE_SHRINK_ORDER`` down to ``_QUEUE_MIN_WIDTHS``;
    if the row still overflows after every column hits its floor the widths are
    left at their minimums (the row may wrap, but never silently misaligns).
    """

    if max_total is None:
        return widths
    gaps = _QUEUE_COLUMN_GAP * (len(widths) - 1)
    overflow = sum(widths.values()) + _queue_display_width(gaps) - max_total
    if overflow <= 0:
        return widths
    adjusted = dict(widths)
    for key in _QUEUE_SHRINK_ORDER:
        if overflow <= 0:
            break
        reducible = adjusted[key] - _QUEUE_MIN_WIDTHS[key]
        if reducible <= 0:
            continue
        cut = min(reducible, overflow)
        adjusted[key] -= cut
        overflow -= cut
    return adjusted


def _prepare_queue_table_rows(
    rows: Sequence[tuple[int, dict[str, Any]]],
    *,
    now: datetime | None = None,
) -> list[dict[str, str]]:
    prepared: list[dict[str, str]] = []
    resolved_now = now or _queue_table_now()
    for indent, item in rows:
        name = _queue_name_text(item)
        if int(indent) > 0:
            name = ("  " * int(indent)) + name
        item_id = normalize_text(item.get("activity_id")) or "-"
        prepared.append(
            {
                "status": _queue_status_icon(item),
                "name": name,
                "detail": _queue_detail_text(item),
                "id": item_id,
                "elapsed": _queue_elapsed_text(item, now=resolved_now),
            }
        )
    return prepared


def _queue_table_columns(*, include_id: bool) -> list[str]:
    # ``id`` is dropped for narrow surfaces (e.g. the Telegram ``/list``) where a
    # full activity id would wrap each row onto a second line.
    return ["status", "name", "detail"] + (["id"] if include_id else []) + ["elapsed"]


def _queue_table_header_width(key: str, prepared: Sequence[dict[str, str]]) -> int:
    return max(
        _queue_display_width(_QUEUE_HEADERS[key]),
        max((_queue_display_width(row[key]) for row in prepared), default=0),
    )


def _queue_table_widths(
    prepared: Sequence[dict[str, str]],
    columns: Sequence[str],
    *,
    max_width: int | None = None,
) -> dict[str, int]:
    # Soft caps keep wide values from dominating before terminal-fit shrinking.
    widths = {key: _queue_table_header_width(key, prepared) for key in columns}
    widths["detail"] = max(_queue_display_width(_QUEUE_HEADERS["detail"]), min(36, widths["detail"]))
    widths["name"] = max(_queue_display_width(_QUEUE_HEADERS["name"]), min(32, widths["name"]))
    widths["elapsed"] = max(_queue_display_width(_QUEUE_HEADERS["elapsed"]), 8)

    # ``status`` and ``elapsed`` are intrinsically narrow and fixed, so the
    # flexible text columns absorb any terminal-width shortfall.
    gap_width = _queue_display_width(_QUEUE_COLUMN_GAP) * (len(columns) - 1)
    fixed_width = widths["status"] + widths["elapsed"] + gap_width
    flexible_keys = [key for key in _QUEUE_SHRINK_ORDER if key in widths]
    flexible = _fit_queue_widths(
        {key: widths[key] for key in flexible_keys},
        max_total=None if max_width is None else max(0, max_width - fixed_width),
    )
    widths.update(flexible)
    return widths


def _render_queue_table_row(
    values: dict[str, str],
    *,
    columns: Sequence[str],
    widths: dict[str, int],
) -> str:
    return _QUEUE_COLUMN_GAP.join(
        _queue_pad_right(_queue_truncate(values[key], max_width=widths[key]), widths[key])
        for key in columns
    )


def queue_table_lines(
    rows: Sequence[tuple[int, dict[str, Any]]],
    *,
    now: datetime | None = None,
    max_width: int | None = None,
    include_id: bool = True,
) -> list[str]:
    prepared = _prepare_queue_table_rows(rows, now=now)
    columns = _queue_table_columns(include_id=include_id)
    widths = _queue_table_widths(prepared, columns, max_width=max_width)
    gap_width = _queue_display_width(_QUEUE_COLUMN_GAP) * (len(columns) - 1)

    def render_row(values: dict[str, str]) -> str:
        return _render_queue_table_row(values, columns=columns, widths=widths)

    lines = [
        render_row(_QUEUE_HEADERS),
        "─" * (sum(widths[key] for key in columns) + gap_width),
    ]
    lines.extend(render_row(row) for row in prepared)
    return lines


def queue_list_text_lines(
    rows: Sequence[tuple[int, dict[str, Any]]],
    *,
    active_simulations: int,
    now: datetime | None = None,
    max_width: int | None = None,
    include_id: bool = True,
    empty_message: str = "No matching activities.",
) -> list[str]:
    lines = [f"active_simulations: {int(active_simulations)}"]
    if not rows:
        lines.append(empty_message)
        return lines
    lines.extend(queue_table_lines(rows, now=now, max_width=max_width, include_id=include_id))
    return lines


def queue_clear_lines(payload: dict[str, Any]) -> list[str]:
    total_cleared = int(payload.get("total_cleared", 0) or 0)
    if total_cleared <= 0:
        return ["Nothing to clear."]

    lines = [f"Cleared {total_cleared} completed/failed/cancelled entries."]
    cleared = payload.get("cleared")
    if not isinstance(cleared, dict):
        return lines

    labels = (
        ("workflows", "workflows"),
        ("xtb_queue_entries", "xTB queue entries"),
        ("crest_queue_entries", "CREST queue entries"),
        ("orca_queue_entries", "ORCA queue entries"),
        ("orca_run_states", "ORCA run states"),
    )
    for key, label in labels:
        count = int(cleared.get(key, 0) or 0)
        if count > 0:
            lines.append(f"  {label}: {count}")
    return lines

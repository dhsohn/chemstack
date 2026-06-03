from __future__ import annotations

from pathlib import Path
from typing import List, Optional

from .input_blocks import (
    BLOCK_START_RE,
    GEOM_HEADER_RE,
    MOINP_RE,
)
from .input_blocks import (
    ensure_route_keywords as _ensure_route_keywords,
)
from .input_blocks import (
    find_block_range as _find_block_range,
)
from .input_blocks import (
    find_geometry_start as _find_geometry_start,
)
from .input_blocks import (
    find_route_idx as _find_route_idx,
)
from .input_blocks import (
    format_relative_or_absolute as _format_relative_or_absolute,
)
from .input_blocks import (
    geometry_range as _geometry_range,
)
from .input_blocks import (
    quote_orca_path as _quote_orca_path,
)
from .input_blocks import (
    replace_geometry_with_xyzfile as _replace_geometry_with_xyzfile,
)
from .input_blocks import (
    set_block_key_value as _set_block_key_value,
)
from .input_blocks import (
    set_moinp as _set_moinp,
)
from .resource_directives import (
    ends_pal_block as _ends_pal_block,
)
from .resource_directives import (
    ensure_submission_resource_request,
    maxcore_mb_per_core,
    read_resource_request_from_input,
)
from .resource_directives import (
    increase_maxcore as _increase_maxcore,
)
from .resource_directives import (
    read_maxcore as _read_maxcore,
)
from .resource_directives import (
    read_nprocs as _read_nprocs,
)
from .resource_directives import (
    read_nprocs_from_text as _read_nprocs_from_text,
)
from .resource_directives import (
    resource_request_from_lines as _resource_request_from_lines,
)
from .resource_directives import (
    set_maxcore as _set_maxcore,
)
from .retry_recipes import (
    RETRY_RECIPES as _RETRY_RECIPES,
)
from .retry_recipes import (
    apply_retry_recipe as _apply_retry_recipe,
)
from .retry_recipes import (
    retry_step_1 as _retry_step_1,
)
from .retry_recipes import (
    retry_step_2 as _retry_step_2,
)
from .retry_recipes import (
    retry_step_3 as _retry_step_3,
)
from .retry_recipes import (
    retry_step_4 as _retry_step_4,
)
from .retry_recipes import (
    set_geom_retry_keys as _set_geom_retry_keys,
)

__all__ = [
    "GEOM_HEADER_RE",
    "BLOCK_START_RE",
    "MOINP_RE",
    "ensure_submission_resource_request",
    "maxcore_mb_per_core",
    "prepare_checkpoint_restart_input",
    "read_resource_request_from_input",
    "rewrite_for_retry",
    "_RETRY_RECIPES",
    "_apply_retry_recipe",
    "_ends_pal_block",
    "_ensure_route_keywords",
    "_find_block_range",
    "_find_geometry_start",
    "_find_route_idx",
    "_format_relative_or_absolute",
    "_geometry_range",
    "_increase_maxcore",
    "_quote_orca_path",
    "_read_maxcore",
    "_read_nprocs",
    "_read_nprocs_from_text",
    "_replace_geometry_with_xyzfile",
    "_resource_request_from_lines",
    "_retry_step_1",
    "_retry_step_2",
    "_retry_step_3",
    "_retry_step_4",
    "_set_block_key_value",
    "_set_geom_retry_keys",
    "_set_maxcore",
    "_set_moinp",
]


def rewrite_for_retry(
    source_inp: Path, target_inp: Path, reaction_dir: Path, step: int
) -> List[str]:
    lines = source_inp.read_text(encoding="utf-8", errors="ignore").splitlines()
    actions: List[str] = []

    actions.extend(_apply_retry_recipe(lines, step))
    _apply_checkpoint_restart(lines, actions, source_inp, target_inp)
    _apply_geometry_restart(lines, actions, source_inp, target_inp, reaction_dir)

    target_inp.write_text("\n".join(lines).rstrip() + "\n", encoding="utf-8")
    return actions


def prepare_checkpoint_restart_input(
    source_inp: Path,
    target_inp: Path,
    reaction_dir: Path,
) -> tuple[Path | None, List[str]]:
    lines = source_inp.read_text(encoding="utf-8", errors="ignore").splitlines()
    actions: List[str] = []
    if not _apply_checkpoint_restart(lines, actions, source_inp, target_inp):
        return None, []

    _apply_geometry_restart(lines, actions, source_inp, target_inp, reaction_dir)
    target_inp.write_text("\n".join(lines).rstrip() + "\n", encoding="utf-8")
    return target_inp, actions


def _apply_checkpoint_restart(
    lines: List[str],
    actions: List[str],
    source_inp: Path,
    target_inp: Path,
) -> bool:
    checkpoint = _matching_checkpoint_gbw(source_inp)
    if checkpoint is None:
        return False
    if checkpoint.resolve() == target_inp.with_suffix(".gbw").resolve():
        actions.append(f"checkpoint_restart_skipped_same_basename:{checkpoint.name}")
        return False

    actions.append(f"checkpoint_restart_from_{checkpoint.name}")
    if _ensure_route_keywords(lines, ["MORead"]):
        actions.append("route_add_moread")
    if _set_moinp(lines, checkpoint, target_inp.parent):
        actions.append("moinp_set")
    return True


def _matching_checkpoint_gbw(source_inp: Path) -> Path | None:
    candidate = source_inp.with_suffix(".gbw")
    try:
        if candidate.exists() and candidate.stat().st_size > 0:
            return candidate
    except OSError:
        return None
    return None


def _apply_geometry_restart(
    lines: List[str],
    actions: List[str],
    source_inp: Path,
    target_inp: Path,
    reaction_dir: Path,
) -> None:
    geometry_file = _previous_attempt_xyz(source_inp)
    if geometry_file is None:
        actions.append("no_previous_xyz_file_found")
        geometry_file = _latest_geometry_file(reaction_dir)

    if geometry_file is None:
        actions.append("no_geometry_file_found")
    else:
        if _replace_geometry_with_xyzfile(lines, geometry_file, target_inp.parent):
            actions.append(f"geometry_restart_from_{geometry_file.name}")
        else:
            actions.append("geometry_restart_not_applied")


def _previous_attempt_xyz(source_inp: Path) -> Optional[Path]:
    candidate = source_inp.with_suffix(".xyz")
    if candidate.exists():
        return candidate
    return None


def _latest_geometry_file(reaction_dir: Path) -> Optional[Path]:
    candidates = {p.resolve(): p for p in reaction_dir.glob("*.xyz")}
    if not candidates:
        return None
    return max(candidates.values(), key=lambda p: p.stat().st_mtime_ns)

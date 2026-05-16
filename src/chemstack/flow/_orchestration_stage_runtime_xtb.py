from __future__ import annotations

import shutil
from pathlib import Path
from typing import Any

import yaml

from ._orchestration_stage_runtime_shared import (
    _call_engine_aware,
    _clear_submission_deferred_metadata,
    _load_contract_or_none,
    _manifest_override_mapping,
    _mark_submission_deferred,
    _orchestration_context,
    _submission_is_deferred,
)
from ._orchestration_stage_runtime_xtb_handoff import (
    _empty_xtb_handoff,
    _update_xtb_handoff_metadata,
    stage_has_xtb_candidates_impl,
    xtb_handoff_status_impl,
)
from ._orchestration_stage_runtime_xtb_inputs import (
    _materialize_xtb_override_xcontrol,
    _materialize_xtb_path_inputs,
    _materialize_xtb_stage_input,
    _stage_input_mapping,
    _stage_input_rank,
)
from ._orchestration_stage_runtime_xtb_path_jobs import (
    _base_xtb_path_manifest,
    _record_xtb_path_attempt,
    _record_xtb_path_job_metadata,
    _record_xtb_path_job_payload,
    _write_xtb_path_manifest,
    _write_xtb_recipe_xcontrol,
    ensure_xtb_job_dir_impl,
    write_xtb_path_job_impl,
)
from ._orchestration_stage_runtime_xtb_retry import (
    _xtb_path_job_dir,
    xtb_attempt_record_impl,
    xtb_attempt_rows_impl,
    xtb_current_attempt_number_impl,
    xtb_path_retry_limit_impl,
    xtb_retry_recipe_impl,
)
from ._orchestration_stage_runtime_xtb_submission import (
    _apply_xtb_submission_result,
    _record_xtb_submission_attempt,
    _submit_xtb_stage,
)
from ._orchestration_stage_runtime_xtb_sync import (
    _apply_xtb_contract,
    _load_xtb_contract,
    _maybe_retry_xtb_handoff,
    _xtb_output_artifacts,
    sync_xtb_stage_impl,
)
from .state import workflow_workspace_internal_engine_paths
from .xyz_utils import load_xyz_frames

__all__ = [
    "Any",
    "Path",
    "_call_engine_aware",
    "_clear_submission_deferred_metadata",
    "_load_contract_or_none",
    "_manifest_override_mapping",
    "_mark_submission_deferred",
    "_orchestration_context",
    "_submission_is_deferred",
    "_apply_xtb_contract",
    "_apply_xtb_submission_result",
    "_base_xtb_path_manifest",
    "_empty_xtb_handoff",
    "_load_xtb_contract",
    "_materialize_xtb_override_xcontrol",
    "_materialize_xtb_path_inputs",
    "_materialize_xtb_stage_input",
    "_maybe_retry_xtb_handoff",
    "_record_xtb_path_attempt",
    "_record_xtb_path_job_metadata",
    "_record_xtb_path_job_payload",
    "_record_xtb_submission_attempt",
    "_stage_input_mapping",
    "_stage_input_rank",
    "_submit_xtb_stage",
    "_update_xtb_handoff_metadata",
    "_write_xtb_path_manifest",
    "_write_xtb_recipe_xcontrol",
    "_xtb_output_artifacts",
    "_xtb_path_job_dir",
    "ensure_xtb_job_dir_impl",
    "stage_has_xtb_candidates_impl",
    "sync_xtb_stage_impl",
    "write_xtb_path_job_impl",
    "workflow_workspace_internal_engine_paths",
    "xtb_attempt_record_impl",
    "xtb_attempt_rows_impl",
    "xtb_current_attempt_number_impl",
    "xtb_handoff_status_impl",
    "xtb_path_retry_limit_impl",
    "xtb_retry_recipe_impl",
    "load_xyz_frames",
    "shutil",
    "yaml",
]

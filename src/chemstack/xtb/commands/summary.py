from __future__ import annotations

from typing import Any

from chemstack.core.commands.engine_summary import EngineSummarySpec
from chemstack.core.commands.engine_summary import cmd_summary as _cmd_engine_summary

from ..config import load_config
from ..job_locations import load_job_artifacts_for_cfg, resolve_job_location_for_cfg

_XTB_SUMMARY_SPEC = EngineSummarySpec(
    key_label="reaction_key",
    record_key_labels=("reaction_key", "molecule_key"),
    kind_label="job_type",
    count_label="candidate_count",
    optional_artifact_fields=(
        "selected_candidate_paths",
        "analysis_summary",
        "resource_request",
        "resource_actual",
    ),
)


def cmd_summary(args: Any) -> int:
    return _cmd_engine_summary(
        args,
        load_config_fn=load_config,
        resolve_job_location_for_cfg_fn=resolve_job_location_for_cfg,
        load_job_artifacts_for_cfg_fn=load_job_artifacts_for_cfg,
        spec=_XTB_SUMMARY_SPEC,
    )

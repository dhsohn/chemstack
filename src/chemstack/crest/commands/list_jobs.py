from __future__ import annotations

from typing import Any

from chemstack.core.commands import engine_list as _engine_list
from chemstack.core.commands import queue as _shared_queue
from chemstack.core.queue import list_queue

from ..config import load_config
from ..job_locations import runtime_roots_for_cfg

_CREST_LIST_SPEC = _engine_list.EngineListSpec(
    engine_label="CREST",
    header="QUEUE ID                    STATUS            PRI  JOB ID                      DIRECTORY",
    separator="-------------------------------------------------------------------------------------",
    columns=(
        _engine_list.EngineListColumn(lambda entry: entry.queue_id, width=27),
        _engine_list.EngineListColumn(lambda entry: _display_status(entry), width=16),
        _engine_list.EngineListColumn(lambda entry: entry.priority, width=4),
        _engine_list.EngineListColumn(lambda entry: entry.task_id, width=27),
        _engine_list.EngineListColumn(
            _engine_list.metadata_path_name_column("job_dir"),
        ),
    ),
)


def _display_status(entry: Any) -> str:
    return _shared_queue.display_status(entry)


def cmd_list(args: Any) -> int:
    return _engine_list.cmd_list(
        args,
        load_config_fn=load_config,
        runtime_roots_for_cfg_fn=runtime_roots_for_cfg,
        list_queue_fn=list_queue,
        spec=_CREST_LIST_SPEC,
    )

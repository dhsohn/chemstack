from __future__ import annotations

import json
from typing import Any

from chemstack.cli_common import _dependency, _shared_chemstack_config

from .operations import cancel_activity, list_activities


def cmd_activity_list(args: Any, *, deps: Any | None = None) -> int:
    shared_chemstack_config = _dependency(
        deps, "_shared_chemstack_config", _shared_chemstack_config
    )
    list_activities_fn = _dependency(deps, "list_activities", list_activities)

    shared_config = shared_chemstack_config(args)
    payload = list_activities_fn(
        workflow_root=getattr(args, "workflow_root", None),
        limit=int(getattr(args, "limit", 0) or 0),
        refresh=bool(getattr(args, "refresh", False)),
        crest_config=shared_config,
        xtb_config=shared_config,
        orca_config=shared_config,
    )
    if bool(getattr(args, "json", False)):
        print(json.dumps(payload, ensure_ascii=True, indent=2))
        return 0

    print(f"activity_count: {payload.get('count', 0)}")
    for item in payload.get("activities", []):
        print(
            f"- {item.get('activity_id', '-')}"
            f" engine={item.get('engine', '-')}"
            f" status={item.get('status', '-')}"
            f" label={item.get('label', '-')}"
            f" source={item.get('source', '-')}"
        )
    return 0


def cmd_activity_cancel(args: Any, *, deps: Any | None = None) -> int:
    shared_chemstack_config = _dependency(
        deps, "_shared_chemstack_config", _shared_chemstack_config
    )
    cancel_activity_fn = _dependency(deps, "cancel_activity", cancel_activity)
    shared_config = shared_chemstack_config(args)
    try:
        payload = cancel_activity_fn(
            target=getattr(args, "target"),
            workflow_root=getattr(args, "workflow_root", None),
            crest_config=shared_config,
            xtb_config=shared_config,
            orca_config=shared_config,
            orca_repo_root=getattr(args, "orca_repo_root", None),
        )
    except (LookupError, ValueError) as exc:
        print(f"error: {exc}")
        return 1

    if bool(getattr(args, "json", False)):
        print(json.dumps(payload, ensure_ascii=True, indent=2))
        return 0

    print(f"activity_id: {payload.get('activity_id', '-')}")
    print(f"engine: {payload.get('engine', '-')}")
    print(f"source: {payload.get('source', '-')}")
    print(f"label: {payload.get('label', '-')}")
    print(f"status: {payload.get('status', '-')}")
    print(f"cancel_target: {payload.get('cancel_target', '-')}")
    return 0

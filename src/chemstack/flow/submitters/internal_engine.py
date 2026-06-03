from __future__ import annotations

from argparse import Namespace as Namespace
from collections.abc import Callable as Callable
from collections.abc import Mapping as Mapping
from dataclasses import dataclass as dataclass
from dataclasses import field as field
from typing import Any as Any

from chemstack.core.queue import DuplicateQueueEntryError as DuplicateQueueEntryError
from chemstack.core.utils import normalize_text as normalize_text

from .internal_engine_builder import (
    InternalEngineSubmitter as InternalEngineSubmitter,
    build_internal_engine_module_submitter as build_internal_engine_module_submitter,
    build_internal_engine_submitter as build_internal_engine_submitter,
    submitter_deps_from_namespace as submitter_deps_from_namespace,
)
from .internal_engine_cancellation import (
    _cancel_failure_payload as _cancel_failure_payload,
    _cancel_success_payload as _cancel_success_payload,
    _direct_cancel_status as _direct_cancel_status,
    _queue_entry_status_text as _queue_entry_status_text,
    cancel_engine_target as cancel_engine_target,
    cancel_internal_engine_target as cancel_internal_engine_target,
)
from .internal_engine_models import (
    InternalEngineCommandResult as InternalEngineCommandResult,
    InternalEngineSubmitterDeps as InternalEngineSubmitterDeps,
    InternalEngineSubmitterSpec as InternalEngineSubmitterSpec,
    _key_value_stdout as _key_value_stdout,
    _stderr_with_exception as _stderr_with_exception,
    _text_fields as _text_fields,
    internal_call_argv as internal_call_argv,
)
from .internal_engine_submission import (
    _submission_failure_payload as _submission_failure_payload,
    _submission_success_payload as _submission_success_payload,
    queue_submission_status as queue_submission_status,
    submit_engine_job_dir as submit_engine_job_dir,
    submit_internal_engine_job_dir as submit_internal_engine_job_dir,
    transient_submission_block_reason as transient_submission_block_reason,
)

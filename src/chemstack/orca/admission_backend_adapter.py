from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

from . import admission_backend as _admission_backend


@dataclass(frozen=True)
class AdmissionBackendAdapter:
    AdmissionStoreCorruptError: Any
    atomic_write_json: Any
    _admission_path: Any
    _chem_core_admission_module: Any
    _normalize_slot: Any

    def _wrap_backend_corruption(self, exc: Exception) -> None:
        if exc.__class__.__name__ == "AdmissionStoreCorruptError":
            raise self.AdmissionStoreCorruptError(str(exc)) from exc

    def _to_chem_core_slot(self, slot: Any, *, backend: Any) -> Any:
        return _admission_backend.to_chem_core_slot(slot, backend=backend, deps=self)

    def _from_chem_core_slot(self, slot: object) -> Any:
        return _admission_backend.from_chem_core_slot(slot, deps=self)

    def load_slots(self, root: Path) -> list[Any]:
        return _admission_backend.load_slots(root, deps=self)

    def save_slots(self, root: Path, slots: list[Any]) -> None:
        _admission_backend.save_slots(root, slots, deps=self)

    def call_backend(
        self,
        root: Path,
        function_name: str,
        *args: Any,
        convert: Any = None,
        **kwargs: Any,
    ) -> Any | None:
        backend = self._chem_core_admission_module()
        if backend is None:
            return None
        backend_fn = getattr(backend, function_name, None)
        if not callable(backend_fn):
            return None
        try:
            result = backend_fn(root, *args, **kwargs)
        except Exception as exc:
            self._wrap_backend_corruption(exc)
            raise
        return convert(result) if convert is not None else result

    def backend_list_slots(self, root: Path, *, backend: Any) -> list[Any] | None:
        return _admission_backend.backend_list_slots(root, backend=backend, deps=self)

    def backend_reconcile_stale_slots(self, root: Path, *, backend: Any) -> int | None:
        return _admission_backend.backend_reconcile_stale_slots(root, backend=backend, deps=self)

    def backend_active_slot_count(self, root: Path, *, backend: Any) -> int | None:
        return _admission_backend.backend_active_slot_count(root, backend=backend, deps=self)

    def text_field(self, value: object) -> str:
        return _admission_backend.text_field(value)

    def build_reserved_slot(
        self,
        *,
        token: str,
        work_dir: str | None,
        queue_id: str | None,
        source: str,
        owner_pid: int,
        process_start_ticks: int | None,
        acquired_at: str,
        app_name: str | None,
        task_id: str | None,
        workflow_id: str | None,
        state: str,
    ) -> Any:
        return _admission_backend.build_reserved_slot(
            token=token,
            work_dir=work_dir,
            queue_id=queue_id,
            source=source,
            owner_pid=owner_pid,
            process_start_ticks=process_start_ticks,
            acquired_at=acquired_at,
            app_name=app_name,
            task_id=task_id,
            workflow_id=workflow_id,
            state=state,
            deps=self,
        )

    def activate_reserved_slot(
        self,
        backend: Any,
        request: Any,
        *,
        update_slot_metadata: Any,
    ) -> bool:
        try:
            updated = backend.activate_reserved_slot(
                request.root,
                request.token,
                state="active",
                work_dir=request.work_dir,
                queue_id=None if request.queue_id is None else self.text_field(request.queue_id),
                owner_pid=request.owner_pid,
                source=request.source,
            )
        except Exception as exc:
            self._wrap_backend_corruption(exc)
            raise
        if updated is None:
            return False
        if not request.has_metadata_update:
            return True
        return update_slot_metadata(
            request.root,
            request.token,
            queue_id=request.queue_id,
            app_name=request.app_name,
            task_id=request.task_id,
            workflow_id=request.workflow_id,
        )


__all__ = ["AdmissionBackendAdapter"]

from __future__ import annotations

import os
import resource
import shutil
import subprocess
from dataclasses import dataclass
from pathlib import Path
from typing import Any, TextIO

from chemstack.core.utils import now_utc_iso

from .config import AppConfig
from .commands._helpers import MANIFEST_FILE_NAME, job_mode, load_job_manifest, resource_request_from_manifest

_RETAINED_ENSEMBLE_CANDIDATES = (
    "crest_conformers.xyz",
    "crest_ensemble.xyz",
    "crest_rotamers.xyz",
    "crest_best.xyz",
)


@dataclass(frozen=True)
class CrestRunResult:
    status: str
    reason: str
    command: tuple[str, ...]
    exit_code: int
    started_at: str
    finished_at: str
    stdout_log: str
    stderr_log: str
    selected_input_xyz: str
    mode: str
    retained_conformer_count: int
    retained_conformer_paths: tuple[str, ...]
    manifest_path: str
    resource_request: dict[str, int]
    resource_actual: dict[str, int]


@dataclass
class CrestRunningJob:
    process: subprocess.Popen[str]
    command: tuple[str, ...]
    started_at: str
    stdout_log: str
    stderr_log: str
    stdout_handle: TextIO
    stderr_handle: TextIO
    selected_input_xyz: str
    mode: str
    manifest_path: str
    resource_request: dict[str, int]
    resource_actual: dict[str, int]
    job_dir: str


def _resolve_crest_executable(cfg: AppConfig) -> str:
    configured = str(cfg.paths.crest_executable).strip()
    if configured:
        path = Path(configured).expanduser().resolve()
        if not path.exists() or not path.is_file():
            raise ValueError(f"Configured CREST executable not found: {path}")
        return str(path)

    discovered = shutil.which("crest")
    if discovered:
        return discovered
    raise ValueError("CREST executable not configured and not found on PATH.")


def _resource_request_dict(cfg: AppConfig, manifest: dict[str, Any]) -> dict[str, int]:
    return resource_request_from_manifest(cfg, manifest)


def _resource_actual_dict(resource_request: dict[str, int]) -> dict[str, int]:
    cores = max(1, int(resource_request.get("max_cores", 1)))
    memory_gb = max(1, int(resource_request.get("max_memory_gb", 1)))
    return {
        "assigned_cores": cores,
        "memory_limit_gb": memory_gb,
        "omp_num_threads": cores,
        "openblas_num_threads": cores,
        "mkl_num_threads": cores,
        "numexpr_num_threads": cores,
    }


def _bool_flag(manifest: dict[str, Any], key: str) -> bool:
    value = manifest.get(key, False)
    if isinstance(value, bool):
        return value
    return str(value).strip().lower() in {"1", "true", "yes", "on"}


def _manifest_int(manifest: dict[str, Any], key: str) -> int | None:
    value = manifest.get(key)
    if value in (None, "", 0, "0"):
        return None
    if isinstance(value, str):
        stripped = value.strip()
        if not stripped:
            return None
        return int(stripped)
    if isinstance(value, (int, float)):
        return int(value)
    raise ValueError(f"Manifest field {key!r} must be an integer-compatible value.")


def _manifest_scalar_text(manifest: dict[str, Any], key: str) -> str | None:
    value = manifest.get(key)
    if value is None:
        return None
    if isinstance(value, str):
        stripped = value.strip()
        return stripped or None
    if isinstance(value, bool):
        return "true" if value else None
    if isinstance(value, (int, float)):
        return str(value)
    text = str(value).strip()
    return text or None


def _build_command(
    cfg: AppConfig,
    *,
    job_dir: Path,
    selected_xyz: Path,
    manifest: dict[str, Any],
) -> list[str]:
    resource_request = _resource_request_dict(cfg, manifest)
    command = [
        _resolve_crest_executable(cfg),
        selected_xyz.name,
        "--T",
        str(resource_request["max_cores"]),
    ]

    mode = job_mode(manifest)
    if mode == "nci":
        command.append("--nci")

    speed = str(manifest.get("speed", "")).strip().lower()
    if speed in {"quick", "squick", "mquick"}:
        command.append(f"--{speed}")

    if _bool_flag(manifest, "dry_run"):
        command.append("--dry")

    if _bool_flag(manifest, "keepdir"):
        command.append("--keepdir")

    if _bool_flag(manifest, "no_preopt"):
        command.append("--noopt")

    gfn = str(manifest.get("gfn", "")).strip().lower()
    if gfn in {"1", "gfn1"}:
        command.append("--gfn1")
    elif gfn in {"2", "gfn2"}:
        command.append("--gfn2")
    elif gfn in {"ff", "gfnff"}:
        command.append("--gfnff")
    elif gfn in {"2//ff", "gfn2//gfnff"}:
        command.append("--gfn2//gfnff")

    charge = _manifest_int(manifest, "charge")
    if charge is not None:
        command.extend(["--chrg", str(charge)])

    uhf = _manifest_int(manifest, "uhf")
    if uhf is not None:
        command.extend(["--uhf", str(uhf)])

    solvent_model = str(manifest.get("solvent_model", "")).strip().lower()
    solvent = str(manifest.get("solvent", "")).strip()
    if solvent and solvent_model in {"gbsa", "alpb"}:
        command.extend([f"--{solvent_model}", solvent])

    for manifest_key, option in (
        ("rthr", "--rthr"),
        ("ewin", "--ewin"),
        ("ethr", "--ethr"),
        ("bthr", "--bthr"),
        ("cluster", "--cluster"),
    ):
        value = _manifest_scalar_text(manifest, manifest_key)
        if value:
            command.extend([option, value])

    if _bool_flag(manifest, "esort"):
        command.append("--esort")

    scratch_dir = job_dir / ".crest_scratch"
    command.extend(["--scratch", str(scratch_dir)])

    return command


def _count_xyz_structures(path: Path) -> int:
    try:
        lines = path.read_text(encoding="utf-8", errors="ignore").splitlines()
    except OSError:
        return 0

    count = 0
    index = 0
    total = len(lines)
    while index < total:
        text = lines[index].strip()
        if not text:
            index += 1
            continue
        try:
            atom_count = int(text)
        except ValueError:
            break
        index += 1  # atom count
        if index < total:
            index += 1  # comment line
        index += atom_count
        count += 1
    return count


def _retained_outputs(job_dir: Path) -> tuple[int, tuple[str, ...]]:
    found: list[str] = []
    count = 0
    for name in _RETAINED_ENSEMBLE_CANDIDATES:
        path = job_dir / name
        if not path.exists():
            continue
        resolved = str(path.resolve())
        found.append(resolved)
        count = max(count, _count_xyz_structures(path))
    return count, tuple(found)


def _preexec_with_limits(max_memory_gb: int):
    limit_bytes = max(1, int(max_memory_gb)) * 1024 * 1024 * 1024

    def _apply() -> None:
        resource.setrlimit(resource.RLIMIT_AS, (limit_bytes, limit_bytes))

    return _apply


def start_crest_job(cfg: AppConfig, *, job_dir: Path, selected_xyz: Path) -> CrestRunningJob:
    manifest = load_job_manifest(job_dir)
    resource_request = _resource_request_dict(cfg, manifest)
    resource_actual = _resource_actual_dict(resource_request)
    command = _build_command(cfg, job_dir=job_dir, selected_xyz=selected_xyz, manifest=manifest)

    stdout_log = job_dir / "crest.stdout.log"
    stderr_log = job_dir / "crest.stderr.log"
    started_at = now_utc_iso()
    # Avoid nested BLAS/OpenMP oversubscription beyond the configured limit.
    env = {
        **os.environ,
        "OMP_NUM_THREADS": str(resource_request["max_cores"]),
        "OPENBLAS_NUM_THREADS": str(resource_request["max_cores"]),
        "MKL_NUM_THREADS": str(resource_request["max_cores"]),
        "NUMEXPR_NUM_THREADS": str(resource_request["max_cores"]),
    }

    stdout_handle = stdout_log.open("w", encoding="utf-8")
    stderr_handle = stderr_log.open("w", encoding="utf-8")
    process = subprocess.Popen(
        command,
        cwd=job_dir,
        env=env,
        text=True,
        stdout=stdout_handle,
        stderr=stderr_handle,
        stdin=subprocess.DEVNULL,
        start_new_session=True,
        preexec_fn=_preexec_with_limits(resource_request["max_memory_gb"]),
    )
    return CrestRunningJob(
        process=process,
        command=tuple(command),
        started_at=started_at,
        stdout_log=str(stdout_log.resolve()),
        stderr_log=str(stderr_log.resolve()),
        stdout_handle=stdout_handle,
        stderr_handle=stderr_handle,
        selected_input_xyz=str(selected_xyz.resolve()),
        mode=job_mode(manifest),
        manifest_path=str((job_dir / MANIFEST_FILE_NAME).resolve()) if (job_dir / MANIFEST_FILE_NAME).exists() else "",
        job_dir=str(job_dir.resolve()),
        resource_request=resource_request,
        resource_actual=resource_actual,
    )


def finalize_crest_job(
    running: CrestRunningJob,
    *,
    forced_status: str | None = None,
    forced_reason: str | None = None,
) -> CrestRunResult:
    try:
        running.stdout_handle.flush()
        running.stderr_handle.flush()
    finally:
        running.stdout_handle.close()
        running.stderr_handle.close()

    exit_code = running.process.poll()
    if exit_code is None:
        exit_code = running.process.wait()

    retained_count, retained_paths = _retained_outputs(Path(running.job_dir))
    finished_at = now_utc_iso()

    if forced_status is not None:
        status = forced_status
    else:
        status = "completed" if exit_code == 0 else "failed"

    if forced_reason is not None:
        reason = forced_reason
    else:
        reason = "completed" if exit_code == 0 else f"crest_exit_code_{exit_code}"

    return CrestRunResult(
        status=status,
        reason=reason,
        command=running.command,
        exit_code=int(exit_code),
        started_at=running.started_at,
        finished_at=finished_at,
        stdout_log=running.stdout_log,
        stderr_log=running.stderr_log,
        selected_input_xyz=running.selected_input_xyz,
        mode=running.mode,
        retained_conformer_count=retained_count,
        retained_conformer_paths=retained_paths,
        manifest_path=running.manifest_path,
        resource_request=running.resource_request,
        resource_actual=running.resource_actual,
    )

from __future__ import annotations

NORMAL_TERMINATION_NEEDLES: tuple[str, ...] = ("ORCA TERMINATED NORMALLY",)

ERROR_TERMINATION_NEEDLES: tuple[str, ...] = (
    "ORCA FINISHED BY ERROR TERMINATION",
    "ABORTING THE RUN",
    "ENDED PREMATURELY AND MAY HAVE CRASHED",
    "FATAL ERROR",
)


def has_normal_termination(text: str) -> bool:
    upper = text.upper()
    return any(needle in upper for needle in NORMAL_TERMINATION_NEEDLES)


def has_error_termination(text: str) -> bool:
    upper = text.upper()
    return any(needle in upper for needle in ERROR_TERMINATION_NEEDLES)


def coarse_orca_status(
    text: str,
    *,
    opt_converged: bool | None = None,
    wall_time_seconds: int | None = None,
) -> str:
    if has_normal_termination(text):
        return "failed" if opt_converged is False else "completed"
    if has_error_termination(text):
        return "failed"
    if wall_time_seconds is not None:
        return "failed"
    return "running"

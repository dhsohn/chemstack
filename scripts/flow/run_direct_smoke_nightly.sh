#!/usr/bin/env bash
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
SCRATCH_ROOT="$(cd "$REPO_ROOT/.." && pwd)/orca_scratch"
STAMP="$(date -u +%Y%m%d_%H%M%S)"
RUN_ROOT="$SCRATCH_ROOT/direct_smoke_nightly_${STAMP}"
LATEST_LINK="$SCRATCH_ROOT/direct_smoke_nightly_latest"
LATEST_SUMMARY_LINK="$SCRATCH_ROOT/direct_smoke_nightly_latest_summary.txt"

mkdir -p "$RUN_ROOT"

cd "$REPO_ROOT"
set +e
python scripts/run_direct_smoke_regression.py --run-root "$RUN_ROOT" --json "$@" | tee "$RUN_ROOT/stdout.json"
REGRESSION_STATUS=${PIPESTATUS[0]}
set -e

python - "$RUN_ROOT/summary.json" "$RUN_ROOT/summary.one_line.txt" "$REGRESSION_STATUS" <<'PY'
import json
import sys
from pathlib import Path

summary_path = Path(sys.argv[1])
output_path = Path(sys.argv[2])
regression_status = int(sys.argv[3])

payload = {}
if summary_path.exists():
    try:
        payload = json.loads(summary_path.read_text(encoding="utf-8"))
    except Exception:
        payload = {}

case_count = payload.get("case_count", 0)
success_count = payload.get("success_count", 0)
failure_count = payload.get("failure_count", 0)
category_counts = payload.get("category_success_counts") or payload.get("category_counts") or {}
category_text = ",".join(f"{key}:{value}" for key, value in sorted(category_counts.items())) or "-"

if regression_status == 0:
    status = "ok"
elif summary_path.exists():
    status = "failed"
else:
    status = f"exit_{regression_status}"

line = (
    f"status={status} "
    f"cases={case_count} "
    f"success={success_count} "
    f"failure={failure_count} "
    f"categories={category_text}"
)
output_path.write_text(line + "\n", encoding="utf-8")
print(line)
PY

ln -sfn "$RUN_ROOT/summary.one_line.txt" "$LATEST_SUMMARY_LINK"

ln -sfn "$RUN_ROOT" "$LATEST_LINK"
printf 'nightly_run_root: %s\n' "$RUN_ROOT"
printf 'latest_link: %s\n' "$LATEST_LINK"
printf 'latest_summary_link: %s\n' "$LATEST_SUMMARY_LINK"
exit "$REGRESSION_STATUS"

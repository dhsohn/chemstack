#!/usr/bin/env bash
set -euo pipefail

env_file="${CHEM_FLOW_ENV_FILE:-/etc/chemstack/flow-worker.env}"
script_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"

if [[ -f "${env_file}" ]]; then
  # shellcheck disable=SC1090
  set -a
  source "${env_file}"
  set +a
fi

repo_root="${CHEM_FLOW_CHEMSTACK_REPO_ROOT:-${script_root}}"
python_bin="${CHEM_FLOW_PYTHON:-${repo_root}/.venv/bin/python}"

args=(
  -m
  chemstack.flow.cli
  workflow
  worker
)

shared_config="${CHEM_FLOW_CONFIG:-}"
if [[ -n "${shared_config:-}" ]]; then
  args+=(--chemstack-config "${shared_config}")
fi

if [[ "${CHEM_FLOW_NO_SUBMIT:-0}" == "1" ]]; then
  args+=(--no-submit)
fi
if [[ "${CHEM_FLOW_REFRESH_REGISTRY:-0}" == "1" ]]; then
  args+=(--refresh-registry)
fi
if [[ "${CHEM_FLOW_REFRESH_EACH_CYCLE:-0}" == "1" ]]; then
  args+=(--refresh-each-cycle)
fi

if [[ -n "${CHEM_FLOW_MAX_CYCLES:-}" ]]; then
  args+=(--max-cycles "${CHEM_FLOW_MAX_CYCLES}")
fi
if [[ -n "${CHEM_FLOW_INTERVAL_SECONDS:-}" ]]; then
  args+=(--interval-seconds "${CHEM_FLOW_INTERVAL_SECONDS}")
fi
if [[ -n "${CHEM_FLOW_LOCK_TIMEOUT_SECONDS:-}" ]]; then
  args+=(--lock-timeout-seconds "${CHEM_FLOW_LOCK_TIMEOUT_SECONDS}")
fi

if [[ ! -x "${python_bin}" ]] && ! command -v "${python_bin}" >/dev/null 2>&1; then
  echo "error: python executable not found: ${python_bin}" >&2
  exit 1
fi

cd "${repo_root}"
if [[ -d "${repo_root}/src" ]]; then
  export PYTHONPATH="${repo_root}/src${PYTHONPATH:+:${PYTHONPATH}}"
fi

exec "${python_bin}" "${args[@]}"

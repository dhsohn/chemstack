#!/usr/bin/env python3
"""Validate chemstack runtime configuration before cutover.

Checks:
  1. Linux-only path validation is satisfied
  2. orca_executable exists and is executable
  3. allowed_root exists

Usage:
    python validate_runtime_config.py
    python validate_runtime_config.py --config /path/to/chemstack.yaml
"""
from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

# Add project paths so we can import the canonical chemstack packages
PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT / "src"))
sys.path.insert(0, str(PROJECT_ROOT))

from chemstack.orca.commands._helpers import default_config_path
from chemstack.orca.config import load_config


def main() -> int:
    parser = argparse.ArgumentParser(description="Validate chemstack runtime configuration")
    parser.add_argument("--config", default=None, help="Path to chemstack.yaml")
    args = parser.parse_args()

    config_path = args.config or default_config_path()
    print(f"Config file: {config_path}")

    # 1. Load and validate config (includes Linux-only path checks)
    try:
        cfg = load_config(config_path)
        print("  [PASS] Config loaded successfully")
        print(f"         allowed_root:  {cfg.runtime.allowed_root}")
        print(f"         orca_executable: {cfg.paths.orca_executable}")
    except ValueError as exc:
        print(f"  [FAIL] Config validation error: {exc}", file=sys.stderr)
        return 1
    except Exception as exc:
        print(f"  [FAIL] Config load error: {exc}", file=sys.stderr)
        return 1

    errors = 0

    # 2. Check orca_executable exists and is executable
    orca_path = Path(cfg.paths.orca_executable)
    if orca_path.exists():
        if os.access(str(orca_path), os.X_OK):
            print("  [PASS] orca_executable exists and is executable")
        else:
            print(f"  [FAIL] orca_executable exists but is NOT executable: {orca_path}", file=sys.stderr)
            errors += 1
    else:
        print(f"  [FAIL] orca_executable not found: {orca_path}", file=sys.stderr)
        errors += 1

    # 3. Check allowed_root exists
    root_path = Path(cfg.runtime.allowed_root)
    if root_path.exists() and root_path.is_dir():
        print("  [PASS] allowed_root exists and is a directory")
    else:
        print(f"  [FAIL] allowed_root not found or not a directory: {root_path}", file=sys.stderr)
        errors += 1

    # Summary
    if errors == 0:
        print("\n=== ALL CHECKS PASSED ===")
        return 0
    else:
        print(f"\n=== {errors} CHECK(S) FAILED ===")
        return 1


if __name__ == "__main__":
    raise SystemExit(main())

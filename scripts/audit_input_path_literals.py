#!/usr/bin/env python3
"""Scan .inp files for Windows absolute path literals that would fail under Linux ORCA.

Usage:
    python audit_input_path_literals.py /home/user/orca_runs
    python audit_input_path_literals.py /home/user/orca_runs --report report.json
"""
from __future__ import annotations

import argparse
import json
import re
import sys
from pathlib import Path
from typing import Any, Dict, List

# Patterns that indicate Windows absolute paths in .inp file content
WINDOWS_PATH_PATTERNS = [
    re.compile(r"[A-Za-z]:\\", re.IGNORECASE),       # C:\, D:\, etc.
    re.compile(r"/mnt/[a-zA-Z]/", re.IGNORECASE),    # /mnt/c/, /mnt/d/
]

def scan_file(inp_path: Path) -> List[Dict[str, Any]]:
    """Scan a single .inp file for Windows path literals. Returns list of findings."""
    findings: List[Dict[str, Any]] = []
    try:
        lines = inp_path.read_text(encoding="utf-8", errors="ignore").splitlines()
    except OSError:
        return findings

    for line_num, line in enumerate(lines, start=1):
        for pattern in WINDOWS_PATH_PATTERNS:
            match = pattern.search(line)
            if match:
                findings.append({
                    "file": str(inp_path),
                    "line": line_num,
                    "pattern": pattern.pattern,
                    "matched": match.group(),
                    "content": line.strip(),
                })
    return findings


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Audit .inp files for Windows absolute path literals"
    )
    parser.add_argument("root", help="Root directory to scan")
    parser.add_argument("--report", type=str, default=None, help="Write JSON report to this path")
    args = parser.parse_args()

    root = Path(args.root)
    if not root.exists():
        print(f"ERROR: Root directory not found: {root}", file=sys.stderr)
        return 1

    inp_files = sorted(root.rglob("*.inp"))
    if not inp_files:
        print(f"No .inp files found under {root}")
        return 0

    all_findings: List[Dict[str, Any]] = []
    for inp_file in inp_files:
        findings = scan_file(inp_file)
        all_findings.extend(findings)

    if all_findings:
        print(f"FOUND {len(all_findings)} Windows path literal(s) in {len(inp_files)} .inp files:\n")
        for f in all_findings:
            print(f"  {f['file']}:{f['line']}: {f['content']}")
        print(f"\nTotal: {len(all_findings)} findings. Resolve before cutover.")
    else:
        print(f"OK: No Windows path literals found in {len(inp_files)} .inp files.")

    if args.report:
        report = {
            "root": str(root),
            "files_scanned": len(inp_files),
            "total_findings": len(all_findings),
            "findings": all_findings,
        }
        Path(args.report).write_text(
            json.dumps(report, ensure_ascii=True, indent=2) + "\n",
            encoding="utf-8",
        )
        print(f"Report written to: {args.report}")

    return 1 if all_findings else 0


if __name__ == "__main__":
    raise SystemExit(main())

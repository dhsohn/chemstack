"""DFT calculation result index — manages structured metadata in a separate SQLite DB (dft.db).

Parses ORCA output files, stores them in the dft_calculations table,
and performs SQL searches with various filter conditions.
"""

from __future__ import annotations

import hashlib
import logging
import sqlite3
import threading
from pathlib import Path
from typing import Any

from core.dft_discovery import discover_orca_targets
from core.orca_parser import parse_orca_output

logger = logging.getLogger(__name__)

_SCHEMA_SQL = """\
CREATE TABLE IF NOT EXISTS dft_calculations (
    id                 INTEGER PRIMARY KEY AUTOINCREMENT,
    source_path        TEXT    NOT NULL UNIQUE,
    file_hash          TEXT    NOT NULL,
    mtime              REAL    NOT NULL,
    calc_type          TEXT    NOT NULL,
    method             TEXT    NOT NULL,
    basis_set          TEXT    NOT NULL DEFAULT '',
    charge             INTEGER DEFAULT 0,
    multiplicity       INTEGER DEFAULT 1,
    formula            TEXT    NOT NULL DEFAULT '',
    n_atoms            INTEGER DEFAULT 0,
    energy_hartree     REAL,
    energy_ev          REAL,
    energy_kcalmol     REAL,
    opt_converged      INTEGER,
    has_imaginary_freq INTEGER,
    lowest_freq_cm1    REAL,
    enthalpy           REAL,
    gibbs_energy       REAL,
    wall_time_seconds  INTEGER,
    status             TEXT    NOT NULL DEFAULT 'completed',
    indexed_at         TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_dft_method  ON dft_calculations(method);
CREATE INDEX IF NOT EXISTS idx_dft_formula ON dft_calculations(formula);
CREATE INDEX IF NOT EXISTS idx_dft_status  ON dft_calculations(status);
CREATE INDEX IF NOT EXISTS idx_dft_energy  ON dft_calculations(energy_hartree);
CREATE INDEX IF NOT EXISTS idx_dft_mtime   ON dft_calculations(mtime);
"""


def _normalize_status_override(status: str | None) -> str | None:
    normalized = str(status or "").strip().lower()
    if normalized in {"created", "pending", "running", "retrying"}:
        return "running"
    if normalized in {"completed", "failed", "cancelled"}:
        return normalized
    return None


class DFTIndex:
    """Manages a structured index of DFT calculation results."""

    def __init__(self) -> None:
        self._db: sqlite3.Connection | None = None
        self._db_path: str = ""
        self._lock = threading.Lock()

    def initialize(self, db_path: str) -> None:
        """Open the database and create the schema."""
        Path(db_path).parent.mkdir(parents=True, exist_ok=True)
        self._db_path = db_path
        self._db = sqlite3.connect(db_path, check_same_thread=False)
        self._db.row_factory = sqlite3.Row
        self._db.execute("PRAGMA journal_mode=WAL")
        self._db.executescript(_SCHEMA_SQL)
        self._db.commit()
        logger.info("dft_index_initialized: db_path=%s", db_path)

    def close(self) -> None:
        """Close the database connection."""
        if self._db is not None:
            self._db.close()
            self._db = None

    def _require_db(self) -> sqlite3.Connection:
        if self._db is None:
            raise RuntimeError("DFTIndex has not been initialized yet.")
        return self._db

    # ------------------------------------------------------------------
    # Indexing
    # ------------------------------------------------------------------

    def index_calculations(
        self,
        kb_dirs: list[str],
        *,
        max_file_size_mb: int = 64,
    ) -> dict[str, Any]:
        """Scan and index ORCA output files from kb_dirs.

        Incremental indexing based on file_hash: only re-parses changed files.

        Returns:
            {"indexed": int, "skipped": int, "removed": int, "failed": int, "total": int}
        """
        db = self._require_db()

        # Load existing index
        with self._lock:
            cursor = db.execute(
                "SELECT source_path, file_hash, status FROM dft_calculations"
            )
            existing: dict[str, tuple[str, str]] = {
                row["source_path"]: (row["file_hash"], str(row["status"]))
                for row in cursor
            }

        # Discover files (I/O-heavy, done outside the lock)
        max_bytes = max_file_size_mb * 1024 * 1024
        discovered: dict[str, tuple[str, str | None]] = {}  # path -> (hash, status_override)

        for kb_dir in kb_dirs:
            kb_path = Path(kb_dir)
            if not kb_path.is_dir():
                logger.warning("dft_kb_dir_not_found: path=%s", kb_dir)
                continue
            for target in discover_orca_targets(kb_path, max_bytes=max_bytes):
                fpath = target.path
                spath = str(fpath)
                h = hashlib.sha256()
                with open(fpath, "rb") as f:
                    for chunk in iter(lambda: f.read(65536), b""):
                        h.update(chunk)
                discovered[spath] = (
                    h.hexdigest()[:16],
                    _normalize_status_override(target.run_state_status),
                )

        # Detect changes
        to_index = {
            p: payload for p, payload in discovered.items()
            if existing.get(p) != (
                payload[0],
                payload[1] or "",
            )
        }
        to_remove = set(existing) - set(discovered)

        indexed = 0
        failed = 0
        removed = 0

        with self._lock:
            # Remove deleted files
            for rpath in to_remove:
                db.execute(
                    "DELETE FROM dft_calculations WHERE source_path = ?", (rpath,)
                )
                removed += 1

            # Index new/changed files
            for source_path, (_, status_override) in to_index.items():
                try:
                    result = parse_orca_output(source_path)
                    if status_override is not None:
                        result.status = status_override
                    self._upsert(db, result)
                    indexed += 1
                except Exception as exc:
                    logger.warning(
                        "dft_parse_failed: path=%s error=%s", source_path, exc,
                    )
                    failed += 1

            db.commit()

        total = self._count()
        logger.info(
            "dft_index_complete: indexed=%d skipped=%d removed=%d failed=%d total=%d",
            indexed, len(discovered) - len(to_index), removed, failed, total,
        )
        return {
            "indexed": indexed,
            "skipped": len(discovered) - len(to_index),
            "removed": removed,
            "failed": failed,
            "total": total,
        }

    def upsert_single(self, file_path: str, *, status_override: str | None = None) -> bool:
        """Parse and upsert a single file. Returns True on success."""
        db = self._require_db()
        try:
            result = parse_orca_output(file_path)
            normalized_override = _normalize_status_override(status_override)
            if normalized_override is not None:
                result.status = normalized_override
            with self._lock:
                self._upsert(db, result)
                db.commit()
            return True
        except Exception as exc:
            logger.warning("dft_upsert_failed: path=%s error=%s", file_path, exc)
            return False

    def _upsert(self, db: sqlite3.Connection, r: Any) -> None:
        """Upsert an OrcaResult into dft_calculations."""
        db.execute(
            """INSERT INTO dft_calculations (
                source_path, file_hash, mtime, calc_type, method, basis_set,
                charge, multiplicity, formula, n_atoms,
                energy_hartree, energy_ev, energy_kcalmol,
                opt_converged, has_imaginary_freq, lowest_freq_cm1,
                enthalpy, gibbs_energy, wall_time_seconds, status
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(source_path) DO UPDATE SET
                file_hash=excluded.file_hash,
                mtime=excluded.mtime,
                calc_type=excluded.calc_type,
                method=excluded.method,
                basis_set=excluded.basis_set,
                charge=excluded.charge,
                multiplicity=excluded.multiplicity,
                formula=excluded.formula,
                n_atoms=excluded.n_atoms,
                energy_hartree=excluded.energy_hartree,
                energy_ev=excluded.energy_ev,
                energy_kcalmol=excluded.energy_kcalmol,
                opt_converged=excluded.opt_converged,
                has_imaginary_freq=excluded.has_imaginary_freq,
                lowest_freq_cm1=excluded.lowest_freq_cm1,
                enthalpy=excluded.enthalpy,
                gibbs_energy=excluded.gibbs_energy,
                wall_time_seconds=excluded.wall_time_seconds,
                status=excluded.status,
                indexed_at=CURRENT_TIMESTAMP
            """,
            (
                r.source_path, r.file_hash, r.mtime, r.calc_type, r.method, r.basis_set,
                r.charge, r.multiplicity, r.formula, r.n_atoms,
                r.energy_hartree, r.energy_ev, r.energy_kcalmol,
                1 if r.opt_converged is True else (0 if r.opt_converged is False else None),
                1 if r.has_imaginary_freq is True else (0 if r.has_imaginary_freq is False else None),
                r.lowest_freq_cm1,
                r.enthalpy, r.gibbs_energy, r.wall_time_seconds, r.status,
            ),
        )

    def _count(self) -> int:
        db = self._require_db()
        with self._lock:
            cursor = db.execute("SELECT COUNT(*) FROM dft_calculations")
            row = cursor.fetchone()
        return row[0] if row else 0

    # ------------------------------------------------------------------
    # Query methods
    # ------------------------------------------------------------------

    def query(self, filters: dict[str, Any]) -> list[dict[str, Any]]:
        """Search calculation results with dynamic filter conditions.

        Supported filters:
            method, basis_set, calc_type, status, formula,
            energy_min, energy_max, opt_converged, has_imaginary_freq
        """
        db = self._require_db()
        conditions: list[str] = []
        params: list[Any] = []

        for col in ("method", "basis_set", "calc_type", "status", "formula"):
            if value := filters.get(col):
                conditions.append(f"{col} = ?")
                params.append(value)

        if "method_like" in filters:
            conditions.append("method LIKE ?")
            params.append(f"%{filters['method_like']}%")

        if "formula_like" in filters:
            conditions.append("formula LIKE ?")
            params.append(f"%{filters['formula_like']}%")

        if "energy_min" in filters:
            conditions.append("energy_hartree >= ?")
            params.append(filters["energy_min"])
        if "energy_max" in filters:
            conditions.append("energy_hartree <= ?")
            params.append(filters["energy_max"])

        if "opt_converged" in filters:
            conditions.append("opt_converged = ?")
            params.append(1 if filters["opt_converged"] else 0)

        if "has_imaginary_freq" in filters:
            conditions.append("has_imaginary_freq = ?")
            params.append(1 if filters["has_imaginary_freq"] else 0)

        where = " AND ".join(conditions) if conditions else "1=1"
        limit = min(int(filters.get("limit", 50)), 200)
        order = filters.get("order_by", "mtime DESC")

        # order_by whitelist
        allowed_orders = {
            "mtime DESC", "mtime ASC",
            "energy_hartree ASC", "energy_hartree DESC",
            "indexed_at DESC", "formula ASC",
        }
        if order not in allowed_orders:
            order = "mtime DESC"

        sql = f"SELECT * FROM dft_calculations WHERE {where} ORDER BY {order} LIMIT ?"
        params.append(limit)

        with self._lock:
            cursor = db.execute(sql, params)
            rows = cursor.fetchall()
        return [dict(row) for row in rows]

    def get_stats(self) -> dict[str, Any]:
        """Return overall index statistics."""
        db = self._require_db()
        with self._lock:
            stats: dict[str, Any] = {}

            cursor = db.execute("SELECT COUNT(*) FROM dft_calculations")
            row = cursor.fetchone()
            stats["total"] = row[0] if row else 0

            cursor = db.execute(
                "SELECT status, COUNT(*) as cnt FROM dft_calculations GROUP BY status"
            )
            stats["by_status"] = {row["status"]: row["cnt"] for row in cursor}

            cursor = db.execute(
                "SELECT method, COUNT(*) as cnt FROM dft_calculations "
                "GROUP BY method ORDER BY cnt DESC LIMIT 10"
            )
            stats["by_method"] = {row["method"]: row["cnt"] for row in cursor}

            cursor = db.execute(
                "SELECT calc_type, COUNT(*) as cnt FROM dft_calculations "
                "GROUP BY calc_type ORDER BY cnt DESC"
            )
            stats["by_calc_type"] = {row["calc_type"]: row["cnt"] for row in cursor}

            cursor = db.execute(
                "SELECT formula, COUNT(*) as cnt FROM dft_calculations "
                "GROUP BY formula ORDER BY cnt DESC LIMIT 10"
            )
            stats["top_formulas"] = {row["formula"]: row["cnt"] for row in cursor}

        return stats

    def get_recent(self, limit: int = 10) -> list[dict[str, Any]]:
        """Return the most recently modified calculation results."""
        return self.query({"order_by": "mtime DESC", "limit": limit})

    def get_lowest_energy(
        self,
        formula: str | None = None,
        limit: int = 5,
    ) -> list[dict[str, Any]]:
        """Return the calculation results with the lowest energy."""
        filters: dict[str, Any] = {
            "order_by": "energy_hartree ASC",
            "limit": limit,
        }
        if formula:
            filters["formula"] = formula
        return self.query(filters)

    def search_by_formula(self, formula: str) -> list[dict[str, Any]]:
        """Search by chemical formula (exact match + LIKE)."""
        exact = self.query({"formula": formula})
        if exact:
            return exact
        return self.query({"formula_like": formula})

    def get_for_comparison(
        self,
        formula: str | None = None,
        method: str | None = None,
    ) -> list[dict[str, Any]]:
        """Return data for comparative analysis, sorted by energy."""
        filters: dict[str, Any] = {
            "order_by": "energy_hartree ASC",
            "limit": 50,
        }
        if formula:
            filters["formula"] = formula
        if method:
            filters["method"] = method
        return self.query(filters)

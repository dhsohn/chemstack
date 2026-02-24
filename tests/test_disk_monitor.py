from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from core.disk_monitor import (
    DirUsage,
    DiskReport,
    FilesystemInfo,
    _dir_size,
    _get_filesystem_info,
    _top_subdirs,
    scan_disk_usage,
)


class TestDirSize(unittest.TestCase):
    def test_empty_dir(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            self.assertEqual(_dir_size(Path(td)), 0)

    def test_with_files(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            p = Path(td) / "data.txt"
            p.write_bytes(b"x" * 1000)
            size = _dir_size(Path(td))
            self.assertGreaterEqual(size, 1000)

    def test_nested_dirs(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            sub = Path(td) / "sub"
            sub.mkdir()
            (sub / "file.txt").write_bytes(b"y" * 500)
            size = _dir_size(Path(td))
            self.assertGreaterEqual(size, 500)

    def test_nonexistent(self) -> None:
        size = _dir_size(Path("/nonexistent_path_xyz"))
        self.assertEqual(size, 0)

    def test_symlink_not_followed(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            real = Path(td) / "real"
            real.mkdir()
            (real / "big.txt").write_bytes(b"z" * 2000)
            link = Path(td) / "link"
            link.symlink_to(real)
            # Should not count symlinked directory
            target = Path(td) / "target"
            target.mkdir()
            (target / "symlink_dir").symlink_to(real)
            size = _dir_size(target)
            self.assertEqual(size, 0)


class TestTopSubdirs(unittest.TestCase):
    def test_sorts_by_size(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            big = Path(td) / "big"
            small = Path(td) / "small"
            big.mkdir()
            small.mkdir()
            (big / "data.txt").write_bytes(b"x" * 2000)
            (small / "data.txt").write_bytes(b"y" * 100)

            top = _top_subdirs(Path(td), 10)
            self.assertEqual(len(top), 2)
            self.assertIn("big", top[0].path)
            self.assertGreater(top[0].size_bytes, top[1].size_bytes)

    def test_limit(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            for i in range(5):
                d = Path(td) / f"dir{i}"
                d.mkdir()
                (d / "f.txt").write_bytes(b"x" * (i + 1) * 100)
            top = _top_subdirs(Path(td), 3)
            self.assertEqual(len(top), 3)


class TestGetFilesystemInfo(unittest.TestCase):
    def test_returns_info(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            info = _get_filesystem_info(Path(td))
            self.assertIsNotNone(info)
            self.assertGreater(info.total_bytes, 0)
            self.assertGreaterEqual(info.usage_percent, 0)

    def test_nonexistent(self) -> None:
        info = _get_filesystem_info(Path("/nonexistent_xyz_abc"))
        self.assertIsNone(info)


class TestScanDiskUsage(unittest.TestCase):
    def test_below_threshold(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            ar = Path(td) / "allowed"
            org = Path(td) / "organized"
            ar.mkdir()
            org.mkdir()
            (ar / "data.txt").write_bytes(b"x" * 100)

            report = scan_disk_usage(str(ar), str(org), threshold_gb=1.0, top_n=5)
            self.assertFalse(report.threshold_exceeded)
            self.assertGreaterEqual(report.combined_bytes, 100)

    def test_above_threshold(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            ar = Path(td) / "allowed"
            org = Path(td) / "organized"
            ar.mkdir()
            org.mkdir()
            (ar / "data.txt").write_bytes(b"x" * 1024)

            # threshold smaller than 1 KB in GB = ~9.3e-7
            report = scan_disk_usage(str(ar), str(org), threshold_gb=1e-10, top_n=5)
            self.assertTrue(report.threshold_exceeded)

    def test_nonexistent_roots(self) -> None:
        report = scan_disk_usage("/nonexist_a", "/nonexist_b", threshold_gb=1.0, top_n=5)
        self.assertEqual(report.allowed_root_bytes, 0)
        self.assertEqual(report.organized_root_bytes, 0)
        self.assertFalse(report.threshold_exceeded)


if __name__ == "__main__":
    unittest.main()

"""Tests for pipeline.common.stale_file_cleaner module."""

import asyncio
import time
from pathlib import Path

import pytest

from pipeline.common.stale_file_cleaner import StaleFileCleaner, _remove_empty_directories


@pytest.fixture
def scan_dir(tmp_path):
    return tmp_path / "downloads"


@pytest.fixture
def cleaner(scan_dir):
    lock = asyncio.Lock()
    in_flight = set()
    return StaleFileCleaner(scan_dir, lock, in_flight, max_age_hours=1)


class TestStaleFileCleaner:
    @pytest.mark.asyncio
    async def test_cleanup_removes_old_files(self, scan_dir, cleaner):
        scan_dir.mkdir()
        old_file = scan_dir / "old.txt"
        old_file.write_text("old")
        # Set mtime to 2 hours ago
        old_mtime = time.time() - 7200
        import os

        os.utime(old_file, (old_mtime, old_mtime))

        await cleaner._cleanup()
        assert not old_file.exists()
        assert cleaner.total_removed == 1

    @pytest.mark.asyncio
    async def test_cleanup_keeps_recent_files(self, scan_dir, cleaner):
        scan_dir.mkdir()
        recent_file = scan_dir / "recent.txt"
        recent_file.write_text("recent")

        await cleaner._cleanup()
        assert recent_file.exists()
        assert cleaner.total_removed == 0

    @pytest.mark.asyncio
    async def test_cleanup_skips_in_flight(self, scan_dir):
        scan_dir.mkdir()
        task_dir = scan_dir / "task-123"
        task_dir.mkdir()
        old_file = task_dir / "file.txt"
        old_file.write_text("data")
        import os

        old_mtime = time.time() - 7200
        os.utime(old_file, (old_mtime, old_mtime))

        lock = asyncio.Lock()
        in_flight = {"task-123"}
        cleaner = StaleFileCleaner(scan_dir, lock, in_flight, max_age_hours=1)

        await cleaner._cleanup()
        assert old_file.exists()
        assert cleaner.total_removed == 0

    @pytest.mark.asyncio
    async def test_cleanup_nonexistent_dir(self, cleaner):
        # scan_dir doesn't exist yet
        await cleaner._cleanup()
        assert cleaner.total_removed == 0

    @pytest.mark.asyncio
    async def test_start_and_stop(self, cleaner):
        await cleaner.start()
        assert cleaner._task is not None
        await cleaner.stop()
        assert cleaner._task is None

    @pytest.mark.asyncio
    async def test_stop_when_not_started(self, cleaner):
        await cleaner.stop()  # should not raise

    @pytest.mark.asyncio
    async def test_cleanup_removes_empty_dirs_after_stale_file_removal(
        self, scan_dir, cleaner
    ):
        """Empty parent directories should be removed after stale files are cleaned."""
        scan_dir.mkdir()
        task_dir = scan_dir / "task-456"
        task_dir.mkdir()
        old_file = task_dir / "file.txt"
        old_file.write_text("data")
        import os

        old_mtime = time.time() - 7200
        os.utime(old_file, (old_mtime, old_mtime))

        await cleaner._cleanup()
        assert not old_file.exists()
        assert not task_dir.exists(), "Empty directory should have been removed"
        assert cleaner.total_removed == 1

    @pytest.mark.asyncio
    async def test_cleanup_removes_nested_empty_dirs(self, scan_dir, cleaner):
        """Nested empty directories should all be removed bottom-up."""
        scan_dir.mkdir()
        nested = scan_dir / "a" / "b" / "c"
        nested.mkdir(parents=True)
        old_file = nested / "file.txt"
        old_file.write_text("data")
        import os

        old_mtime = time.time() - 7200
        os.utime(old_file, (old_mtime, old_mtime))

        await cleaner._cleanup()
        assert not old_file.exists()
        assert not nested.exists()
        assert not (scan_dir / "a" / "b").exists()
        assert not (scan_dir / "a").exists()

    @pytest.mark.asyncio
    async def test_cleanup_preserves_non_empty_dirs(self, scan_dir, cleaner):
        """Directories still containing recent files should not be removed."""
        scan_dir.mkdir()
        task_dir = scan_dir / "task-789"
        task_dir.mkdir()
        old_file = task_dir / "old.txt"
        old_file.write_text("old")
        recent_file = task_dir / "recent.txt"
        recent_file.write_text("recent")
        import os

        old_mtime = time.time() - 7200
        os.utime(old_file, (old_mtime, old_mtime))

        await cleaner._cleanup()
        assert not old_file.exists()
        assert recent_file.exists()
        assert task_dir.exists(), "Dir with remaining files should be kept"

    @pytest.mark.asyncio
    async def test_cleanup_skips_in_flight_dirs_during_dir_removal(self, scan_dir):
        """Empty directories matching in-flight tasks should not be removed."""
        scan_dir.mkdir()
        task_dir = scan_dir / "task-active"
        task_dir.mkdir()
        # directory is empty but matches an in-flight task

        lock = asyncio.Lock()
        in_flight = {"task-active"}
        cleaner = StaleFileCleaner(scan_dir, lock, in_flight, max_age_hours=1)

        await cleaner._cleanup()
        assert task_dir.exists(), "In-flight task directory should be preserved"


class TestRemoveEmptyDirectories:
    def test_removes_empty_leaf_dirs(self, tmp_path):
        scan_dir = tmp_path / "root"
        scan_dir.mkdir()
        empty = scan_dir / "empty_dir"
        empty.mkdir()

        removed = _remove_empty_directories(scan_dir, set())
        assert removed == 1
        assert not empty.exists()

    def test_removes_nested_empty_dirs_bottom_up(self, tmp_path):
        scan_dir = tmp_path / "root"
        (scan_dir / "a" / "b" / "c").mkdir(parents=True)

        removed = _remove_empty_directories(scan_dir, set())
        assert removed == 3
        assert not (scan_dir / "a").exists()

    def test_skips_in_flight_dirs(self, tmp_path):
        scan_dir = tmp_path / "root"
        scan_dir.mkdir()
        active = scan_dir / "active-task"
        active.mkdir()

        removed = _remove_empty_directories(scan_dir, {"active-task"})
        assert removed == 0
        assert active.exists()

    def test_keeps_dirs_with_files(self, tmp_path):
        scan_dir = tmp_path / "root"
        scan_dir.mkdir()
        nonempty = scan_dir / "has_file"
        nonempty.mkdir()
        (nonempty / "data.txt").write_text("content")

        removed = _remove_empty_directories(scan_dir, set())
        assert removed == 0
        assert nonempty.exists()

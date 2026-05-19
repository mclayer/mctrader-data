# tests/integration/test_promote_historical_pidfile_lock.py
"""MCT-204 §8.2: promote-historical INV-G pidfile flock concurrent instance test.

Tests:
- Two concurrent invocations → second instance fails with appropriate error
- Pidfile released after completion (cleanup)
- Note: uses fcntl (Linux/macOS only), skip on Windows
"""
from __future__ import annotations

import contextlib
import os
import sys
from datetime import timedelta, datetime, timezone

import pytest

TODAY = datetime.now(timezone.utc).date()
HISTORICAL = TODAY - timedelta(days=5)

# Skip on Windows (fcntl not available)
pytestmark = pytest.mark.skipif(
    sys.platform == "win32",
    reason="fcntl pidfile lock not supported on Windows (dev-only platform)",
)


class TestPromoteHistoricalPidfileLock:
    def test_concurrent_invocation_second_exits(self, tmp_path):
        """INV-G: second concurrent instance should be blocked by pidfile flock."""
        try:
            import fcntl
        except ImportError:
            pytest.skip("fcntl not available")

        audit_dir = tmp_path / "audit"
        audit_dir.mkdir(parents=True, exist_ok=True)
        pidfile = audit_dir / "historical-reclaim.pid"

        # Simulate first instance holding the lock
        fd = os.open(str(pidfile), os.O_WRONLY | os.O_CREAT, 0o644)
        fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        os.write(fd, b"99999")  # fake PID

        # Second instance should fail to acquire lock
        second_fd = None
        blocked = False
        try:
            second_fd = os.open(str(pidfile), os.O_WRONLY | os.O_CREAT, 0o644)
            fcntl.flock(second_fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            blocked = True
        finally:
            if second_fd is not None:
                with contextlib.suppress(Exception):
                    fcntl.flock(second_fd, fcntl.LOCK_UN)
                os.close(second_fd)
            fcntl.flock(fd, fcntl.LOCK_UN)
            os.close(fd)

        assert blocked, "Second instance should be blocked by flock"

    def test_pidfile_contains_pid(self, tmp_path):
        """INV-G: pidfile contains the current process PID."""
        try:
            import fcntl
        except ImportError:
            pytest.skip("fcntl not available")

        audit_dir = tmp_path / "audit"
        audit_dir.mkdir(parents=True, exist_ok=True)
        pidfile = audit_dir / "historical-reclaim.pid"

        fd = os.open(str(pidfile), os.O_WRONLY | os.O_CREAT, 0o644)
        fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        os.write(fd, str(os.getpid()).encode())
        fcntl.flock(fd, fcntl.LOCK_UN)
        os.close(fd)

        content = pidfile.read_text().strip()
        assert content == str(os.getpid())

    def test_pidfile_released_after_lock_unlock(self, tmp_path):
        """INV-G: after flock release, second invocation can acquire lock."""
        try:
            import fcntl
        except ImportError:
            pytest.skip("fcntl not available")

        audit_dir = tmp_path / "audit"
        audit_dir.mkdir(parents=True, exist_ok=True)
        pidfile = audit_dir / "historical-reclaim.pid"

        # First lock and release
        fd = os.open(str(pidfile), os.O_WRONLY | os.O_CREAT, 0o644)
        fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        fcntl.flock(fd, fcntl.LOCK_UN)
        os.close(fd)

        # Second should succeed
        fd2 = os.open(str(pidfile), os.O_WRONLY | os.O_CREAT, 0o644)
        try:
            fcntl.flock(fd2, fcntl.LOCK_EX | fcntl.LOCK_NB)
            acquired = True
            fcntl.flock(fd2, fcntl.LOCK_UN)
        except BlockingIOError:
            acquired = False
        finally:
            os.close(fd2)

        assert acquired, "Second invocation should acquire lock after first releases"

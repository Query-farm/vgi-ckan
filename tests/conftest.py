"""Shared fixtures: the strict CKAN replay server."""

from __future__ import annotations

import subprocess
import sys
from collections.abc import Iterator
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parent.parent


@pytest.fixture(scope="session")
def mock_api() -> Iterator[str]:
    """Start the strict CKAN replay server (test/mock_api/server.py) and return its base URL."""
    proc = subprocess.Popen(
        [sys.executable, str(ROOT / "test" / "mock_api" / "server.py")],
        stdout=subprocess.PIPE,
        text=True,
        encoding="utf-8",
    )
    assert proc.stdout is not None
    line = proc.stdout.readline().strip()
    assert line.startswith("PORT:"), line
    yield f"http://127.0.0.1:{line.removeprefix('PORT:')}"
    proc.terminate()
    proc.wait(timeout=10)

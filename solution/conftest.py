"""Pytest configuration and root fixtures for the solution test suite.

Ensures solution/src is automatically added to sys.path during test execution,
allowing tests to be run from the repository root without manually setting PYTHONPATH.
"""

from pathlib import Path
import sys

# Add solution/src to Python import path
SRC_DIR = Path(__file__).resolve().parent / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))


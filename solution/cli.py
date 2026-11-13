"""
Convenience CLI launcher for Agentic Universe Interaction Ledger Pipeline.
Allows running:
  python solution/cli.py ingest data/drop_1
  python solution/cli.py report --out reports/
"""

import sys
from pathlib import Path

# Add solution/src to sys.path so ledger module is always importable
src_path = Path(__file__).parent / "src"
if str(src_path) not in sys.path:
    sys.path.insert(0, str(src_path))

from ledger.cli import main

if __name__ == "__main__":
    sys.exit(main())


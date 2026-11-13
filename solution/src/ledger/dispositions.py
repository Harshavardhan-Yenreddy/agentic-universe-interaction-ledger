"""
Catalogue for call dispositions loaded from dispositions.csv.
Defines terminal and success classifications per workflow.
"""

from __future__ import annotations

import csv
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Optional, Tuple


@dataclass(frozen=True)
class Disposition:
    workflow_id: str
    code: str
    label: str
    is_terminal: bool
    is_success: bool


class DispositionsCatalog:
    """Registry of allowed dispositions and their business semantics."""

    def __init__(self, catalog_path: Path | str = "assignment-a-data/schemas/dispositions.csv") -> None:
        self.catalog_path = Path(catalog_path)
        self._items: dict[tuple[str, str], Disposition] = {}
        self._load()

    def _load(self) -> None:
        if not self.catalog_path.is_file():
            raise FileNotFoundError(f"Dispositions catalog not found: {self.catalog_path}")

        with open(self.catalog_path, "r", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                wf = row["workflow_id"].strip()
                code = row["code"].strip()
                label = row["label"].strip()
                is_terminal = row["is_terminal"].strip().lower() == "true"
                is_success = row["is_success"].strip().lower() == "true"

                disp = Disposition(
                    workflow_id=wf,
                    code=code,
                    label=label,
                    is_terminal=is_terminal,
                    is_success=is_success,
                )
                self._items[(wf, code)] = disp

    def get(self, workflow_id: str, code: str) -> Optional[Disposition]:
        """Get disposition details by workflow and code, or None if unknown."""
        return self._items.get((workflow_id, code))

    def is_valid(self, workflow_id: str, code: str) -> bool:
        """Check if code exists in the catalogue for the given workflow."""
        return (workflow_id, code) in self._items

    def is_terminal(self, workflow_id: str, code: str) -> bool:
        """Check if disposition is terminal."""
        disp = self.get(workflow_id, code)
        return disp.is_terminal if disp else False

    def is_success(self, workflow_id: str, code: str) -> bool:
        """Check if disposition is marked as success."""
        disp = self.get(workflow_id, code)
        return disp.is_success if disp else False


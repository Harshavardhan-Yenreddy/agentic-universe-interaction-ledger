"""
Schema definition and header mapping for workflow lead schemas.
Loads motor_renewal.json and policy_feedback.json.
"""

from __future__ import annotations

import json
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional


def normalize_header(header: str) -> str:
    """
    Normalise a header per spec:
    Lower-case it, trim it, and replace every run of '_' or whitespace with a single space.
    """
    return re.sub(r'[\s_]+', ' ', header.lower().strip())


@dataclass(frozen=True)
class FieldDefinition:
    name: str
    type: str
    required: bool
    pii: bool
    reference: bool
    aliases: tuple[str, ...]
    values: tuple[str, ...]
    default: Optional[str]

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> FieldDefinition:
        return cls(
            name=data["name"],
            type=data.get("type", "string"),
            required=data.get("required", False),
            pii=data.get("pii", False),
            reference=data.get("reference", False),
            aliases=tuple(data.get("aliases", [])),
            values=tuple(data.get("values", [])),
            default=data.get("default"),
        )


@dataclass(frozen=True)
class WorkflowSchema:
    workflow_id: str
    description: str
    fields: tuple[FieldDefinition, ...]
    _header_map: dict[str, FieldDefinition]

    @classmethod
    def from_file(cls, path: Path | str) -> WorkflowSchema:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)

        fields = tuple(FieldDefinition.from_dict(fd) for fd in data.get("fields", []))
        header_map: dict[str, FieldDefinition] = {}

        for fd in fields:
            header_map[normalize_header(fd.name)] = fd
            for alias in fd.aliases:
                header_map[normalize_header(alias)] = fd

        return cls(
            workflow_id=data["workflow_id"],
            description=data.get("description", ""),
            fields=fields,
            _header_map=header_map,
        )

    def match_header(self, raw_header: str) -> Optional[FieldDefinition]:
        """Match a raw CSV header against schema field names or aliases."""
        return self._header_map.get(normalize_header(raw_header))

    @property
    def reference_field(self) -> FieldDefinition:
        """Return the schema field marked as reference: true."""
        for fd in self.fields:
            if fd.reference:
                return fd
        raise ValueError(f"Workflow {self.workflow_id} has no reference field defined")


class SchemaRegistry:
    """Registry that caches loaded workflow schemas."""

    def __init__(self, schemas_dir: Path | str = "assignment-a-data/schemas") -> None:
        self.schemas_dir = Path(schemas_dir)
        self._schemas: dict[str, WorkflowSchema] = {}

    def get(self, workflow_id: str) -> WorkflowSchema:
        if workflow_id not in self._schemas:
            schema_file = self.schemas_dir / f"{workflow_id}.json"
            if not schema_file.is_file():
                raise FileNotFoundError(f"Schema file not found for workflow '{workflow_id}': {schema_file}")
            self._schemas[workflow_id] = WorkflowSchema.from_file(schema_file)
        return self._schemas[workflow_id]


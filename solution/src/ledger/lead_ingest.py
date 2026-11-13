"""
Lead CSV ingestion pipeline.
Handles file discovery, header mapping, validation, PII encryption,
rejection audit logging, and duplicate merging.
"""

from __future__ import annotations

import csv
import hashlib
from collections import Counter
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from .database import DatabaseManager
from .schemas import SchemaRegistry, WorkflowSchema
from .security import SecurityManager
from .validation import (
    normalize_phone,
    parse_date,
    parse_decimal,
    parse_enum,
)


@dataclass
class LeadIngestionStats:
    """Statistics for lead CSV ingestion."""
    lead_rows_total: int = 0
    lead_rows_rejected: int = 0
    rejections_by_reason: Counter[str] = field(default_factory=Counter)
    duplicate_lead_rows_merged: int = 0
    field_warnings: int = 0
    files_processed: int = 0
    files_skipped: int = 0

    def merge(self, other: LeadIngestionStats) -> None:
        self.lead_rows_total += other.lead_rows_total
        self.lead_rows_rejected += other.lead_rows_rejected
        self.rejections_by_reason.update(other.rejections_by_reason)
        self.duplicate_lead_rows_merged += other.duplicate_lead_rows_merged
        self.field_warnings += other.field_warnings
        self.files_processed += other.files_processed
        self.files_skipped += other.files_skipped


@dataclass(frozen=True)
class DiscoveredLeadFile:
    drop_id: str
    workflow_id: str
    batch_id: str
    path: Path


def compute_file_sha256(path: Path) -> str:
    """Compute the SHA-256 hash of a file for idempotency tracking."""
    hasher = hashlib.sha256()
    with open(path, "rb") as f:
        while chunk := f.read(65536):
            hasher.update(chunk)
    return hasher.hexdigest()


def discover_lead_files(drop_dir: Path | str) -> list[DiscoveredLeadFile]:
    """
    Discover all lead CSV files in a drop directory.
    Expected layout: <drop_dir>/leads/<workflow_id>/<batch_id>.csv
    Files are returned in deterministic order: workflow_id, then batch filename.
    """
    drop_path = Path(drop_dir)
    drop_id = drop_path.name
    leads_root = drop_path / "leads"
    if not leads_root.is_dir():
        return []

    discovered: list[DiscoveredLeadFile] = []
    for wf_dir in sorted(leads_root.iterdir()):
        if not wf_dir.is_dir():
            continue
        workflow_id = wf_dir.name
        for csv_file in sorted(wf_dir.glob("*.csv")):
            if csv_file.is_file():
                batch_id = csv_file.stem
                discovered.append(
                    DiscoveredLeadFile(
                        drop_id=drop_id,
                        workflow_id=workflow_id,
                        batch_id=batch_id,
                        path=csv_file,
                    )
                )

    return discovered


def ingest_lead_csv(
    lead_file: DiscoveredLeadFile,
    db: DatabaseManager,
    security: SecurityManager,
    schema_registry: SchemaRegistry,
) -> LeadIngestionStats:
    """
    Ingest a single lead CSV file idempotently.
    
    Returns:
        LeadIngestionStats for this file.
    """
    stats = LeadIngestionStats()
    file_rel_path = str(lead_file.path).replace("\\", "/")
    file_hash = compute_file_sha256(lead_file.path)

    # 1. Idempotency Check
    existing_file = db.fetchone(
        "SELECT drop_id FROM ingested_files WHERE drop_id = ? AND file_path = ?;",
        (lead_file.drop_id, file_rel_path),
    )
    if existing_file:
        stats.files_skipped = 1
        return stats

    schema = schema_registry.get(lead_file.workflow_id)

    # 2. Read and parse CSV safely
    with open(lead_file.path, "r", encoding="utf-8") as f:
        reader = list(csv.reader(f))

    if not reader:
        # Empty file
        with db.transaction() as conn:
            conn.execute(
                """
                INSERT INTO ingested_files (drop_id, file_path, file_type, file_hash, record_count)
                VALUES (?, ?, 'leads', ?, 0);
                """,
                (lead_file.drop_id, file_rel_path, file_hash),
            )
        stats.files_processed = 1
        return stats

    header_row = reader[0]
    # Map CSV column index to FieldDefinition
    col_to_field: dict[int, Any] = {}
    for idx, h in enumerate(header_row):
        matched = schema.match_header(h)
        if matched is not None:
            col_to_field[idx] = matched

    # 3. Process data rows inside an atomic transaction
    with db.transaction() as conn:
        row_number = 0

        for raw_row in reader[1:]:
            # Blank rows: every cell empty or whitespace -> skipped and not counted
            if not any(c.strip() for c in raw_row):
                continue

            row_number += 1
            stats.lead_rows_total += 1

            # Extract row values
            raw_data: dict[str, str] = {}
            for idx, val in enumerate(raw_row):
                if idx in col_to_field:
                    fdef = col_to_field[idx]
                    raw_data[fdef.name] = val.strip()

            # Walk fields in schema order for validation and rejection
            rejected_reason: Optional[str] = None
            row_warnings = 0
            norm_values: dict[str, Any] = {}

            for fdef in schema.fields:
                fname = fdef.name
                ftype = fdef.type
                freq = fdef.required
                val = raw_data.get(fname, "")

                if not val:
                    if freq:
                        rejected_reason = f"missing_required:{fname}"
                        break
                    else:
                        norm_values[fname] = None
                        continue

                # Field type validations
                if ftype == "phone":
                    p = normalize_phone(val)
                    if not p:
                        rejected_reason = "invalid_phone"
                        break
                    norm_values[fname] = p
                elif ftype == "string":
                    norm_values[fname] = val
                elif ftype == "date":
                    d, is_valid = parse_date(val)
                    if not is_valid:
                        norm_values[fname] = None
                        row_warnings += 1
                    else:
                        norm_values[fname] = d
                elif ftype == "decimal":
                    dec, is_valid = parse_decimal(val)
                    if not is_valid:
                        norm_values[fname] = None
                        row_warnings += 1
                    else:
                        norm_values[fname] = dec
                elif ftype == "enum":
                    enum_val, is_valid = parse_enum(val, fdef.values)
                    if not is_valid:
                        norm_values[fname] = None
                        row_warnings += 1
                    else:
                        norm_values[fname] = enum_val

            # Handle Rejection vs Lead Creation / Merging
            if rejected_reason:
                stats.lead_rows_rejected += 1
                stats.rejections_by_reason[rejected_reason] += 1
                conn.execute(
                    """
                    INSERT INTO rejections (drop_id, workflow_id, batch_id, row_number, reason)
                    VALUES (?, ?, ?, ?, ?);
                    """,
                    (lead_file.drop_id, lead_file.workflow_id, lead_file.batch_id, row_number, rejected_reason),
                )
            else:
                # Count warnings only for non-rejected rows
                stats.field_warnings += row_warnings

                # Canonical phone and blind index hash
                canonical_phone = norm_values["phone"]
                phone_hash = security.hash_phone(canonical_phone, lead_file.workflow_id)
                phone_masked = security.mask_phone(canonical_phone)

                # Check if lead already exists in DB
                existing_lead = conn.execute(
                    "SELECT lead_id, batch_id FROM leads WHERE workflow_id = ? AND phone_hash = ?;",
                    (lead_file.workflow_id, phone_hash),
                ).fetchone()

                if existing_lead:
                    # Duplicate Lead -> Merge non-null fields
                    stats.duplicate_lead_rows_merged += 1
                    
                    # Build dynamic update statement for non-null fields
                    update_fields: list[str] = []
                    update_params: list[Any] = []

                    if norm_values.get("customer_name") is not None:
                        enc_name = security.encrypt(norm_values["customer_name"])
                        update_fields.append("customer_name_encrypted = ?")
                        update_params.append(enc_name)

                    ref_field = schema.reference_field.name
                    if norm_values.get(ref_field) is not None:
                        update_fields.append("external_ref = ?")
                        update_params.append(norm_values[ref_field])

                    for optional_col in ("policy_expiry", "order_date", "premium_amount", "product", "preferred_language"):
                        if norm_values.get(optional_col) is not None:
                            update_fields.append(f"{optional_col} = ?")
                            update_params.append(norm_values[optional_col])

                    update_fields.append("updated_at = CURRENT_TIMESTAMP")
                    update_params.extend([lead_file.workflow_id, phone_hash])

                    conn.execute(
                        f"UPDATE leads SET {', '.join(update_fields)} WHERE workflow_id = ? AND phone_hash = ?;",
                        update_params,
                    )
                else:
                    # New Lead -> Apply schema defaults to remaining nulls, then insert
                    for fdef in schema.fields:
                        if fdef.default is not None and norm_values.get(fdef.name) is None:
                            norm_values[fdef.name] = fdef.default

                    enc_name = security.encrypt(norm_values["customer_name"])
                    enc_phone = security.encrypt(canonical_phone)
                    ref_field = schema.reference_field.name
                    external_ref = norm_values[ref_field]

                    conn.execute(
                        """
                        INSERT INTO leads (
                            workflow_id, phone_hash, phone_encrypted, customer_name_encrypted,
                            phone_masked, external_ref, batch_id,
                            policy_expiry, order_date, premium_amount, product, preferred_language
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
                        """,
                        (
                            lead_file.workflow_id,
                            phone_hash,
                            enc_phone,
                            enc_name,
                            phone_masked,
                            external_ref,
                            lead_file.batch_id,
                            norm_values.get("policy_expiry"),
                            norm_values.get("order_date"),
                            norm_values.get("premium_amount"),
                            norm_values.get("product"),
                            norm_values.get("preferred_language", "en"),
                        ),
                    )

        # Record file as successfully ingested
        conn.execute(
            """
            INSERT INTO ingested_files (drop_id, file_path, file_type, file_hash, record_count)
            VALUES (?, ?, 'leads', ?, ?);
            """,
            (lead_file.drop_id, file_rel_path, file_hash, row_number),
        )

    stats.files_processed = 1
    return stats


def ingest_leads_drop(
    drop_dir: Path | str,
    db: DatabaseManager,
    security: SecurityManager,
    schema_registry: Optional[SchemaRegistry] = None,
) -> LeadIngestionStats:
    """
    Ingest all lead CSV files in a drop directory deterministically.
    Updates the data_quality table with lead metrics for this drop.
    """
    if schema_registry is None:
        schema_registry = SchemaRegistry()

    lead_files = discover_lead_files(drop_dir)
    drop_stats = LeadIngestionStats()
    drop_id = Path(drop_dir).name

    for lf in lead_files:
        f_stats = ingest_lead_csv(lf, db, security, schema_registry)
        drop_stats.merge(f_stats)

    # Persist or update drop-level data_quality row
    if drop_stats.files_processed > 0:
        import json
        with db.transaction() as conn:
            existing = conn.execute(
                "SELECT id FROM data_quality WHERE drop_id = ?;",
                (drop_id,),
            ).fetchone()

            reasons_json = json.dumps(dict(drop_stats.rejections_by_reason))

            if existing:
                conn.execute(
                    """
                    UPDATE data_quality
                    SET lead_rows_total = lead_rows_total + ?,
                        lead_rows_rejected = lead_rows_rejected + ?,
                        rejections_by_reason_json = ?,
                        duplicate_lead_rows_merged = duplicate_lead_rows_merged + ?,
                        field_warnings = field_warnings + ?,
                        updated_at = CURRENT_TIMESTAMP
                    WHERE drop_id = ?;
                    """,
                    (
                        drop_stats.lead_rows_total,
                        drop_stats.lead_rows_rejected,
                        reasons_json,
                        drop_stats.duplicate_lead_rows_merged,
                        drop_stats.field_warnings,
                        drop_id,
                    ),
                )
            else:
                conn.execute(
                    """
                    INSERT INTO data_quality (
                        drop_id, lead_rows_total, lead_rows_rejected, rejections_by_reason_json,
                        duplicate_lead_rows_merged, field_warnings
                    ) VALUES (?, ?, ?, ?, ?, ?);
                    """,
                    (
                        drop_id,
                        drop_stats.lead_rows_total,
                        drop_stats.lead_rows_rejected,
                        reasons_json,
                        drop_stats.duplicate_lead_rows_merged,
                        drop_stats.field_warnings,
                    ),
                )

    return drop_stats


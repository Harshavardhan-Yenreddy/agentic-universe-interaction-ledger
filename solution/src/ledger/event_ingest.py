"""
Event JSONL ingestion pipeline and interaction ledger population.
Handles file discovery, JSON validation, timestamp normalization to IST,
deduplication by event_id, PII-safe hashing, and orphan event management.
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from .database import DatabaseManager
from .dispositions import DispositionsCatalog
from .security import SecurityManager
from .validation import normalize_phone

IST = timezone(timedelta(hours=5, minutes=30))


@dataclass
class EventIngestionStats:
    """Statistics for event JSONL ingestion."""
    events_total: int = 0
    malformed_events: int = 0
    duplicate_events_dropped: int = 0
    unknown_disposition_events: int = 0
    valid_events_inserted: int = 0
    files_processed: int = 0
    files_skipped: int = 0

    def merge(self, other: EventIngestionStats) -> None:
        self.events_total += other.events_total
        self.malformed_events += other.malformed_events
        self.duplicate_events_dropped += other.duplicate_events_dropped
        self.unknown_disposition_events += other.unknown_disposition_events
        self.valid_events_inserted += other.valid_events_inserted
        self.files_processed += other.files_processed
        self.files_skipped += other.files_skipped


@dataclass(frozen=True)
class DiscoveredEventFile:
    drop_id: str
    path: Path


def compute_file_sha256(path: Path) -> str:
    """Compute the SHA-256 checksum of an event file for idempotency."""
    hasher = hashlib.sha256()
    with open(path, "rb") as f:
        while chunk := f.read(65536):
            hasher.update(chunk)
    return hasher.hexdigest()


def discover_event_files(drop_dir: Path | str) -> list[DiscoveredEventFile]:
    """
    Discover all event JSONL files under <drop_dir>/events/.
    Returned in deterministic alphabetical filename order.
    """
    drop_path = Path(drop_dir)
    drop_id = drop_path.name
    events_root = drop_path / "events"
    if not events_root.is_dir():
        return []

    discovered: list[DiscoveredEventFile] = []
    for f in sorted(events_root.glob("*.jsonl")):
        if f.is_file():
            discovered.append(DiscoveredEventFile(drop_id=drop_id, path=f))
    return discovered


def parse_event_timestamp(ts_str: str) -> tuple[str, str]:
    """
    Parse an event timestamp string into ISO-8601 IST string and IST date (YYYY-MM-DD).
    Per assignment spec:
    - UTC (...Z) -> convert to IST (+05:30)
    - Offset (...05:30) -> aware datetime converted to IST
    - Naive (YYYY-MM-DD HH:MM:SS) -> Naive means IST!
    """
    ts = ts_str.strip()
    if not ts:
        raise ValueError("Timestamp cannot be empty")

    if ts.endswith("Z"):
        dt = datetime.fromisoformat(ts.replace("Z", "+00:00")).astimezone(IST)
    elif "T" in ts and ("+" in ts or "-" in ts[10:]):
        dt = datetime.fromisoformat(ts).astimezone(IST)
    else:
        # Naive format or simple space separator -> naive means IST
        naive_dt = datetime.fromisoformat(ts)
        if naive_dt.tzinfo is None:
            dt = naive_dt.replace(tzinfo=IST)
        else:
            dt = naive_dt.astimezone(IST)

    ist_iso = dt.isoformat()
    ist_date = dt.strftime("%Y-%m-%d")
    return ist_iso, ist_date


def sanitize_payload(payload: Any) -> str:
    """
    Ensure the payload does not contain plaintext PII before serialization.
    """
    if not isinstance(payload, dict):
        return "{}"

    safe = dict(payload)
    # Remove any potential customer PII keys if present
    for pii_key in ("phone", "customer_name", "name", "contact"):
        safe.pop(pii_key, None)

    return json.dumps(safe, ensure_ascii=False)


def ingest_event_jsonl(
    event_file: DiscoveredEventFile,
    db: DatabaseManager,
    security: SecurityManager,
    catalog: DispositionsCatalog,
) -> EventIngestionStats:
    """
    Ingest a single event JSONL file idempotently within an atomic transaction.
    """
    stats = EventIngestionStats()
    file_rel_path = str(event_file.path).replace("\\", "/")
    file_hash = compute_file_sha256(event_file.path)

    # 1. Idempotency Check
    existing_file = db.fetchone(
        "SELECT drop_id FROM ingested_files WHERE drop_id = ? AND file_path = ?;",
        (event_file.drop_id, file_rel_path),
    )
    if existing_file:
        stats.files_skipped = 1
        return stats

    # 2. Read lines and process
    with open(event_file.path, "r", encoding="utf-8") as f:
        lines = f.readlines()

    seen_batch_ids: set[str] = set()

    with db.transaction() as conn:
        for line in lines:
            raw_line = line.strip()
            if not raw_line:
                continue

            stats.events_total += 1

            # Rule 1: Malformed check
            try:
                ev = json.loads(raw_line)
            except Exception:
                stats.malformed_events += 1
                continue

            if not isinstance(ev, dict):
                stats.malformed_events += 1
                continue

            event_id = ev.get("event_id")
            if not event_id or not isinstance(event_id, str) or not event_id.strip():
                stats.malformed_events += 1
                continue

            event_id = event_id.strip()

            # Rule 2: Duplicate check
            # Check within current batch
            if event_id in seen_batch_ids:
                stats.duplicate_events_dropped += 1
                continue

            # Check in database
            existing_event = conn.execute(
                "SELECT 1 FROM interaction_ledger WHERE event_id = ?;",
                (event_id,),
            ).fetchone()
            if existing_event:
                stats.duplicate_events_dropped += 1
                continue

            seen_batch_ids.add(event_id)

            # Validate basic event fields
            event_type = ev.get("event_type")
            workflow_id = ev.get("workflow_id")
            call_id = ev.get("call_id")
            occurred_at_raw = ev.get("occurred_at")

            if not event_type or not workflow_id or not call_id or not occurred_at_raw:
                stats.malformed_events += 1
                continue

            # Normalize timestamp to IST
            try:
                occurred_at_ist, date_ist = parse_event_timestamp(occurred_at_raw)
            except Exception:
                stats.malformed_events += 1
                continue

            # Phone & Blind Index Hash
            phone_hash: Optional[str] = None
            if "phone" in ev:
                raw_phone = ev["phone"]
                p_norm = normalize_phone(raw_phone)
                if p_norm is not None:
                    phone_hash = security.hash_phone(p_norm, workflow_id)

            # Rule 4: Unknown disposition check
            payload = ev.get("payload", {})
            if event_type in ("disposition.set", "disposition.override"):
                code = payload.get("disposition_code") if isinstance(payload, dict) else None
                if not code or not catalog.is_valid(workflow_id, str(code).strip()):
                    stats.unknown_disposition_events += 1

            safe_payload_json = sanitize_payload(payload)

            # Insert into interaction_ledger
            conn.execute(
                """
                INSERT INTO interaction_ledger (
                    event_id, drop_id, event_type, workflow_id, call_id, phone_hash,
                    occurred_at_ist, date_ist, payload_json
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?);
                """,
                (
                    event_id,
                    event_file.drop_id,
                    event_type,
                    workflow_id,
                    call_id,
                    phone_hash,
                    occurred_at_ist,
                    date_ist,
                    safe_payload_json,
                ),
            )
            stats.valid_events_inserted += 1

        # Record file as successfully ingested
        conn.execute(
            """
            INSERT INTO ingested_files (drop_id, file_path, file_type, file_hash, record_count)
            VALUES (?, ?, 'events', ?, ?);
            """,
            (event_file.drop_id, file_rel_path, file_hash, stats.events_total),
        )

    stats.files_processed = 1
    return stats


def count_orphan_events(db: DatabaseManager) -> int:
    """
    Count all events in interaction_ledger that cannot be linked to any valid lead.
    Linking rules:
    - Events with phone_hash link if (workflow_id, phone_hash) exists in leads.
    - Overrides (phone_hash is null) link if any other event with the same call_id links to a lead.
    """
    row = db.fetchone(
        """
        SELECT COUNT(*) as cnt
        FROM interaction_ledger e
        WHERE (
            e.phone_hash IS NOT NULL 
            AND NOT EXISTS (
                SELECT 1 FROM leads l 
                WHERE l.workflow_id = e.workflow_id AND l.phone_hash = e.phone_hash
            )
        ) OR (
            e.phone_hash IS NULL 
            AND NOT EXISTS (
                SELECT 1 FROM interaction_ledger e2
                JOIN leads l ON l.workflow_id = e2.workflow_id AND l.phone_hash = e2.phone_hash
                WHERE e2.call_id = e.call_id AND e2.phone_hash IS NOT NULL
            )
        );
        """
    )
    return row["cnt"] if row else 0


def count_linked_events(db: DatabaseManager) -> int:
    """Count all events in interaction_ledger that successfully link to a valid lead."""
    row = db.fetchone(
        """
        SELECT COUNT(*) as cnt
        FROM interaction_ledger e
        WHERE (
            e.phone_hash IS NOT NULL 
            AND EXISTS (
                SELECT 1 FROM leads l 
                WHERE l.workflow_id = e.workflow_id AND l.phone_hash = e.phone_hash
            )
        ) OR (
            e.phone_hash IS NULL 
            AND EXISTS (
                SELECT 1 FROM interaction_ledger e2
                JOIN leads l ON l.workflow_id = e2.workflow_id AND l.phone_hash = e2.phone_hash
                WHERE e2.call_id = e.call_id AND e2.phone_hash IS NOT NULL
            )
        );
        """
    )
    return row["cnt"] if row else 0


def ingest_events_drop(
    drop_dir: Path | str,
    db: DatabaseManager,
    security: SecurityManager,
    catalog: Optional[DispositionsCatalog] = None,
) -> EventIngestionStats:
    """
    Ingest all event JSONL files in a drop directory deterministically.
    Updates the data_quality table with event metrics for this drop.
    """
    if catalog is None:
        catalog = DispositionsCatalog()

    event_files = discover_event_files(drop_dir)
    drop_stats = EventIngestionStats()
    drop_id = Path(drop_dir).name

    for ef in event_files:
        f_stats = ingest_event_jsonl(ef, db, security, catalog)
        drop_stats.merge(f_stats)

    # Persist or update drop-level data_quality row
    if drop_stats.files_processed > 0:
        current_orphans = count_orphan_events(db)
        with db.transaction() as conn:
            existing = conn.execute(
                "SELECT id FROM data_quality WHERE drop_id = ?;",
                (drop_id,),
            ).fetchone()

            if existing:
                conn.execute(
                    """
                    UPDATE data_quality
                    SET events_total = events_total + ?,
                        malformed_events = malformed_events + ?,
                        duplicate_events_dropped = duplicate_events_dropped + ?,
                        unknown_disposition_events = unknown_disposition_events + ?,
                        orphan_events = ?,
                        updated_at = CURRENT_TIMESTAMP
                    WHERE drop_id = ?;
                    """,
                    (
                        drop_stats.events_total,
                        drop_stats.malformed_events,
                        drop_stats.duplicate_events_dropped,
                        drop_stats.unknown_disposition_events,
                        current_orphans,
                        drop_id,
                    ),
                )
            else:
                conn.execute(
                    """
                    INSERT INTO data_quality (
                        drop_id, events_total, malformed_events, duplicate_events_dropped,
                        unknown_disposition_events, orphan_events
                    ) VALUES (?, ?, ?, ?, ?, ?);
                    """,
                    (
                        drop_id,
                        drop_stats.events_total,
                        drop_stats.malformed_events,
                        drop_stats.duplicate_events_dropped,
                        drop_stats.unknown_disposition_events,
                        current_orphans,
                    ),
                )

    return drop_stats


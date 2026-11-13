"""
Comprehensive tests for Event JSONL Ingestion and Interaction Ledger.
Tests malformed line handling, deduplication, orphan handling,
timestamp parsing, late lead arrival resolution, and idempotency.
"""

import json
import os
import secrets
from pathlib import Path
import pytest

from ledger.config import Config
from ledger.database import DatabaseManager
from ledger.dispositions import DispositionsCatalog
from ledger.event_ingest import (
    DiscoveredEventFile,
    EventIngestionStats,
    count_linked_events,
    count_orphan_events,
    discover_event_files,
    ingest_event_jsonl,
    ingest_events_drop,
    parse_event_timestamp,
)
from ledger.lead_ingest import DiscoveredLeadFile, ingest_lead_csv, ingest_leads_drop
from ledger.schemas import SchemaRegistry
from ledger.security import SecurityManager


@pytest.fixture
def env_setup(tmp_path):
    """Set up DatabaseManager, SecurityManager, and DispositionsCatalog."""
    db_file = tmp_path / "test_ledger.db"
    db = DatabaseManager(db_file)
    db.initialize_schema()

    config = Config(
        encryption_key=secrets.token_bytes(32),
        hmac_salt=secrets.token_bytes(32),
        db_path=db_file,
    )
    security = SecurityManager(config)
    catalog = DispositionsCatalog("assignment-a-data/schemas/dispositions.csv")
    registry = SchemaRegistry("assignment-a-data/schemas")
    return db, security, catalog, registry, tmp_path


def create_jsonl(path: Path, lines: list[str]) -> Path:
    """Helper to write JSONL files for testing."""
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        for l in lines:
            f.write(l + "\n")
    return path


def test_timestamp_parsing():
    """Test parsing timestamps: UTC (Z), offset (+05:30), and naive (naive means IST)."""
    # 1. UTC Z format
    ist_iso1, date1 = parse_event_timestamp("2026-10-05T13:20:21Z")
    assert ist_iso1 == "2026-10-05T18:50:21+05:30"
    assert date1 == "2026-10-05"

    # 2. Offset format
    ist_iso2, date2 = parse_event_timestamp("2026-10-05T18:50:21+05:30")
    assert ist_iso2 == "2026-10-05T18:50:21+05:30"
    assert date2 == "2026-10-05"

    # 3. Naive format -> naive means IST!
    ist_iso3, date3 = parse_event_timestamp("2026-10-03 10:01:00")
    assert ist_iso3 == "2026-10-03T10:01:00+05:30"
    assert date3 == "2026-10-03"


def test_valid_event_types_insertion(env_setup):
    """Test parsing and inserting all standard event types."""
    db, security, catalog, registry, tmp = env_setup
    events_file = tmp / "events" / "events_001.jsonl"
    lines = [
        json.dumps({
            "event_id": "evt_001", "event_type": "call.initiated", "workflow_id": "motor_renewal",
            "call_id": "call_001", "occurred_at": "2026-10-01 10:00:00", "phone": "+91 98200 12345",
            "payload": {}
        }),
        json.dumps({
            "event_id": "evt_002", "event_type": "call.connected", "workflow_id": "motor_renewal",
            "call_id": "call_001", "occurred_at": "2026-10-01 10:00:15", "phone": "+91 98200 12345",
            "payload": {}
        }),
        json.dumps({
            "event_id": "evt_003", "event_type": "call.ended", "workflow_id": "motor_renewal",
            "call_id": "call_001", "occurred_at": "2026-10-01 10:02:00", "phone": "+91 98200 12345",
            "payload": {"duration_sec": 105, "end_reason": "completed"}
        }),
        json.dumps({
            "event_id": "evt_004", "event_type": "disposition.set", "workflow_id": "motor_renewal",
            "call_id": "call_001", "occurred_at": "2026-10-01 10:02:30", "phone": "+91 98200 12345",
            "payload": {"disposition_code": "renewed"}
        }),
        json.dumps({
            "event_id": "evt_005", "event_type": "disposition.override", "workflow_id": "motor_renewal",
            "call_id": "call_001", "occurred_at": "2026-10-01 12:00:00",
            "payload": {"disposition_code": "not_interested", "overridden_by": "sup_1", "reason": "qa_review"}
        }),
    ]
    create_jsonl(events_file, lines)
    f = DiscoveredEventFile("drop_1", events_file)
    stats = ingest_event_jsonl(f, db, security, catalog)

    assert stats.events_total == 5
    assert stats.malformed_events == 0
    assert stats.duplicate_events_dropped == 0
    assert stats.valid_events_inserted == 5

    # Verify event stored in interaction_ledger
    row = db.fetchone("SELECT * FROM interaction_ledger WHERE event_id = 'evt_001';")
    assert row is not None
    assert row["event_type"] == "call.initiated"
    assert row["call_id"] == "call_001"
    assert row["date_ist"] == "2026-10-01"

    # Verify override has null phone_hash
    row_override = db.fetchone("SELECT * FROM interaction_ledger WHERE event_id = 'evt_005';")
    assert row_override["phone_hash"] is None


def test_malformed_event_lines(env_setup):
    """Test handling of malformed lines: truncated JSON, missing event_id, empty dict."""
    db, security, catalog, registry, tmp = env_setup
    events_file = tmp / "events" / "events_001.jsonl"
    lines = [
        '{"event_id": "evt_001", "event_typ',                 # Truncated JSON
        '{"event_type": "call.initiated"}',                   # Missing event_id
        '{"event_id": "", "event_type": "call.initiated"}',   # Empty event_id
        'not a json at all',                                  # Non-JSON string
        json.dumps({                                          # Valid line
            "event_id": "evt_002", "event_type": "call.initiated", "workflow_id": "motor_renewal",
            "call_id": "call_002", "occurred_at": "2026-10-01 10:00:00", "phone": "9820012345",
            "payload": {}
        }),
    ]
    create_jsonl(events_file, lines)
    f = DiscoveredEventFile("drop_1", events_file)
    stats = ingest_event_jsonl(f, db, security, catalog)

    assert stats.events_total == 5
    assert stats.malformed_events == 4
    assert stats.valid_events_inserted == 1


def test_duplicate_events_within_and_across_drops(env_setup):
    """Test deduplication by event_id within a file and across multiple drops."""
    db, security, catalog, registry, tmp = env_setup

    # Drop 1 has evt_001 and duplicate evt_001
    f1 = tmp / "drop_1" / "events" / "events_001.jsonl"
    create_jsonl(f1, [
        json.dumps({"event_id": "evt_001", "event_type": "call.initiated", "workflow_id": "motor_renewal",
                    "call_id": "call_001", "occurred_at": "2026-10-01 10:00:00", "phone": "9820012345"}),
        json.dumps({"event_id": "evt_001", "event_type": "call.initiated", "workflow_id": "motor_renewal",
                    "call_id": "call_001", "occurred_at": "2026-10-01 10:00:00", "phone": "9820012345"}),
        json.dumps({"event_id": "evt_002", "event_type": "call.connected", "workflow_id": "motor_renewal",
                    "call_id": "call_001", "occurred_at": "2026-10-01 10:00:10", "phone": "9820012345"}),
    ])

    stats1 = ingest_event_jsonl(DiscoveredEventFile("drop_1", f1), db, security, catalog)
    assert stats1.events_total == 3
    assert stats1.duplicate_events_dropped == 1
    assert stats1.valid_events_inserted == 2

    # Drop 2 delivers evt_001 again and new evt_003
    f2 = tmp / "drop_2" / "events" / "events_002.jsonl"
    create_jsonl(f2, [
        json.dumps({"event_id": "evt_001", "event_type": "call.initiated", "workflow_id": "motor_renewal",
                    "call_id": "call_001", "occurred_at": "2026-10-01 10:00:00", "phone": "9820012345"}),
        json.dumps({"event_id": "evt_003", "event_type": "call.ended", "workflow_id": "motor_renewal",
                    "call_id": "call_001", "occurred_at": "2026-10-01 10:01:00", "phone": "9820012345"}),
    ])

    stats2 = ingest_event_jsonl(DiscoveredEventFile("drop_2", f2), db, security, catalog)
    assert stats2.events_total == 2
    assert stats2.duplicate_events_dropped == 1
    assert stats2.valid_events_inserted == 1

    total_in_ledger = db.fetchone("SELECT COUNT(*) as c FROM interaction_ledger;")["c"]
    assert total_in_ledger == 3


def test_unknown_dispositions(env_setup):
    """Test that disposition events with unrecognized codes increment unknown_disposition_events."""
    db, security, catalog, registry, tmp = env_setup
    f = tmp / "events" / "events_001.jsonl"
    create_jsonl(f, [
        # Unknown code in motor_renewal: "RENEWD" (misspelled)
        json.dumps({"event_id": "evt_001", "event_type": "disposition.set", "workflow_id": "motor_renewal",
                    "call_id": "call_001", "occurred_at": "2026-10-01 10:00:00", "phone": "9820012345",
                    "payload": {"disposition_code": "RENEWD"}}),
        # Valid code in motor_renewal: "renewed"
        json.dumps({"event_id": "evt_002", "event_type": "disposition.set", "workflow_id": "motor_renewal",
                    "call_id": "call_001", "occurred_at": "2026-10-01 10:00:00", "phone": "9820012345",
                    "payload": {"disposition_code": "renewed"}}),
    ])
    stats = ingest_event_jsonl(DiscoveredEventFile("drop_1", f), db, security, catalog)
    assert stats.unknown_disposition_events == 1
    assert stats.valid_events_inserted == 2


def test_orphan_resolution_on_later_lead_arrival(env_setup):
    """
    Critical requirement test:
    - Events arrive in drop_1 for customer A before their lead is uploaded.
    - Customer A's events are stored in interaction_ledger as orphans.
    - Customer A's lead CSV is uploaded in drop_2.
    - Customer A's events automatically resolve to the lead without mutating the ledger.
    """
    db, security, catalog, registry, tmp = env_setup

    # 1. Ingest event first in drop_1 (Customer phone: 9820012345)
    event_file = tmp / "drop_1" / "events" / "events_001.jsonl"
    create_jsonl(event_file, [
        json.dumps({"event_id": "evt_001", "event_type": "call.initiated", "workflow_id": "motor_renewal",
                    "call_id": "call_001", "occurred_at": "2026-10-01 10:00:00", "phone": "+91 98200 12345"}),
        json.dumps({"event_id": "evt_002", "event_type": "call.connected", "workflow_id": "motor_renewal",
                    "call_id": "call_001", "occurred_at": "2026-10-01 10:00:15", "phone": "+91 98200 12345"}),
        # Supervisor override with no phone
        json.dumps({"event_id": "evt_003", "event_type": "disposition.override", "workflow_id": "motor_renewal",
                    "call_id": "call_001", "occurred_at": "2026-10-01 11:00:00",
                    "payload": {"disposition_code": "renewed"}}),
    ])
    ingest_event_jsonl(DiscoveredEventFile("drop_1", event_file), db, security, catalog)

    # Currently, no leads exist in DB -> all 3 events must be orphans!
    assert count_orphan_events(db) == 3
    assert count_linked_events(db) == 0

    # 2. Later, in drop_2, customer lead is uploaded
    lead_file = tmp / "drop_2" / "leads" / "motor_renewal" / "batch_001.csv"
    lead_file.parent.mkdir(parents=True, exist_ok=True)
    with open(lead_file, "w", newline="", encoding="utf-8") as f:
        f.write("customer_name,phone,policy_number\n")
        f.write("Aarav Sharma,+91 98200 12345,MOT/001\n")

    ingest_lead_csv(DiscoveredLeadFile("drop_2", "motor_renewal", "batch_001", lead_file), db, security, registry)

    # Lead now exists -> all 3 events (including override via call_id) are linked!
    assert count_orphan_events(db) == 0
    assert count_linked_events(db) == 3


def test_idempotent_event_reingestion(env_setup):
    """Verify that re-ingesting an event file is completely idempotent."""
    db, security, catalog, registry, tmp = env_setup
    f = tmp / "drop_1" / "events" / "events_001.jsonl"
    create_jsonl(f, [
        json.dumps({"event_id": "evt_001", "event_type": "call.initiated", "workflow_id": "motor_renewal",
                    "call_id": "call_001", "occurred_at": "2026-10-01 10:00:00", "phone": "9820012345"}),
    ])
    ef = DiscoveredEventFile("drop_1", f)

    stats1 = ingest_event_jsonl(ef, db, security, catalog)
    assert stats1.files_processed == 1
    assert stats1.valid_events_inserted == 1

    # Second ingestion must skip
    stats2 = ingest_event_jsonl(ef, db, security, catalog)
    assert stats2.files_skipped == 1
    assert stats2.events_total == 0
    assert stats2.valid_events_inserted == 0

    cnt = db.fetchone("SELECT COUNT(*) as c FROM interaction_ledger;")["c"]
    assert cnt == 1


def test_integration_full_dataset_verification():
    """
    Integration verification on the actual synthetic dataset data/drop_1 and data/drop_2.
    Verifies exact ground truth:
    - Events total: 8,745
    - Malformed events: 8
    - Duplicate events dropped: 237
    - Valid unique events: 8,500
    - Final linked events: 8,038
    - Final orphan events: 462
    - Unknown disposition events: 19
    """
    if not Path("data/drop_1/events").is_dir() or not Path("data/drop_2/events").is_dir():
        pytest.skip("data/drop_1 or data/drop_2 events not found")

    db = DatabaseManager(":memory:")
    db.initialize_schema()

    config = Config(
        encryption_key=secrets.token_bytes(32),
        hmac_salt=secrets.token_bytes(32),
        db_path=":memory:",
    )
    security = SecurityManager(config)
    catalog = DispositionsCatalog("assignment-a-data/schemas/dispositions.csv")
    registry = SchemaRegistry("assignment-a-data/schemas")

    # Ingest Leads first
    lead_stats1 = ingest_leads_drop("data/drop_1", db, security, registry)
    lead_stats2 = ingest_leads_drop("data/drop_2", db, security, registry)
    assert db.fetchone("SELECT COUNT(*) as c FROM leads;")["c"] == 1892

    # Ingest Events
    event_stats1 = ingest_events_drop("data/drop_1", db, security, catalog)
    event_stats2 = ingest_events_drop("data/drop_2", db, security, catalog)

    assert event_stats1.events_total == 4247
    assert event_stats2.events_total == 4498
    assert event_stats1.events_total + event_stats2.events_total == 8745

    assert event_stats1.malformed_events == 4
    assert event_stats2.malformed_events == 4
    assert event_stats1.malformed_events + event_stats2.malformed_events == 8

    assert event_stats1.duplicate_events_dropped == 83
    assert event_stats2.duplicate_events_dropped == 154
    assert event_stats1.duplicate_events_dropped + event_stats2.duplicate_events_dropped == 237

    assert event_stats1.valid_events_inserted + event_stats2.valid_events_inserted == 8500
    assert db.fetchone("SELECT COUNT(*) as c FROM interaction_ledger;")["c"] == 8500

    assert event_stats1.unknown_disposition_events + event_stats2.unknown_disposition_events == 19

    # Verify final linked and orphan event counts
    orphans = count_orphan_events(db)
    linked = count_linked_events(db)
    assert orphans == 462
    assert linked == 8038
    assert orphans + linked == 8500


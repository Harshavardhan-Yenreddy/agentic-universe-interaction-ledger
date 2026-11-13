"""
Tests for Report Generation Engine.
Verifies all 5 report formats, exact headers, sorting, precision,
calculations, and PII masking.
"""

import csv
import json
import secrets
from pathlib import Path
import pytest

from ledger.config import Config
from ledger.database import DatabaseManager
from ledger.dispositions import DispositionsCatalog
from ledger.lead_ingest import DiscoveredLeadFile, ingest_lead_csv, ingest_leads_drop
from ledger.event_ingest import DiscoveredEventFile, ingest_event_jsonl, ingest_events_drop
from ledger.reports import (
    generate_all_reports,
    generate_batch_summary,
    generate_daily_overview,
    generate_data_quality_report,
    generate_lead_report,
    generate_rejections_report,
)
from ledger.schemas import SchemaRegistry
from ledger.security import SecurityManager


@pytest.fixture
def test_setup(tmp_path):
    """Set up in-memory DB and test managers."""
    db_file = tmp_path / "test.db"
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
    reports_dir = tmp_path / "reports"
    return db, security, catalog, registry, reports_dir, tmp_path


def test_empty_database_reports(test_setup):
    """Verify that reports on an empty database produce valid files with headers."""
    db, _, catalog, _, reports_dir, _ = test_setup
    reports = generate_all_reports(db, catalog, reports_dir)

    for name in ("batch_summary.csv", "daily_overview.csv", "lead_report.csv", "rejections.csv", "data_quality.json"):
        assert (reports_dir / name).is_file()

    # Check batch_summary.csv header
    with open(reports["batch_summary.csv"], "r", encoding="utf-8") as f:
        reader = list(csv.reader(f))
        assert len(reader) == 1
        assert reader[0] == ["workflow_id", "batch_id", "total_leads", "yet_to_reach_out",
                             "in_progress_telephony_failed", "in_progress_dialed", "completed"]

    # Check daily_overview.csv header
    with open(reports["daily_overview.csv"], "r", encoding="utf-8") as f:
        reader = list(csv.reader(f))
        assert len(reader) == 1
        assert reader[0] == ["workflow_id", "date", "total_calls", "connected_calls",
                             "connectivity_rate", "success_leads"]

    # Check lead_report.csv header
    with open(reports["lead_report.csv"], "r", encoding="utf-8") as f:
        reader = list(csv.reader(f))
        assert len(reader) == 1
        assert reader[0] == ["workflow_id", "external_ref", "batch_id", "phone_masked", "bucket",
                             "total_calls", "connected_calls", "last_call_at", "current_disposition", "ever_success"]

    # Check rejections.csv header
    with open(reports["rejections.csv"], "r", encoding="utf-8") as f:
        reader = list(csv.reader(f))
        assert len(reader) == 1
        assert reader[0] == ["workflow_id", "batch_id", "row_number", "reason"]

    # Check data_quality.json keys
    with open(reports["data_quality.json"], "r", encoding="utf-8") as f:
        data = json.load(f)
        assert data["lead_rows_total"] == 0
        assert data["events_total"] == 0
        assert data["orphan_events"] == 0


def test_batch_summary_bucket_aggregations(test_setup):
    """Test batch_summary.csv bucket calculations."""
    db, security, catalog, registry, reports_dir, tmp = test_setup

    # Lead 1: yet_to_reach_out (0 calls)
    # Lead 2: completed (call with terminal disposition: renewed)
    # Lead 3: in_progress_telephony_failed (only telephony_failed calls)
    # Lead 4: in_progress_dialed (call without terminal disposition: callback_requested)
    lead_file = tmp / "leads" / "motor_renewal" / "batch_001.csv"
    lead_file.parent.mkdir(parents=True, exist_ok=True)
    with open(lead_file, "w", newline="", encoding="utf-8") as f:
        f.write("customer_name,phone,policy_number\n")
        f.write("Lead 1,+919820010001,MOT/001\n")
        f.write("Lead 2,+919820010002,MOT/002\n")
        f.write("Lead 3,+919820010003,MOT/003\n")
        f.write("Lead 4,+919820010004,MOT/004\n")

    ingest_lead_csv(DiscoveredLeadFile("drop_1", "motor_renewal", "batch_001", lead_file), db, security, registry)

    event_file = tmp / "events" / "events_001.jsonl"
    event_file.parent.mkdir(parents=True, exist_ok=True)
    with open(event_file, "w", encoding="utf-8") as f:
        # Lead 2 calls:
        f.write(json.dumps({"event_id": "e1", "event_type": "call.initiated", "workflow_id": "motor_renewal",
                           "call_id": "c1", "occurred_at": "2026-10-01 10:00:00", "phone": "9820010002"}) + "\n")
        f.write(json.dumps({"event_id": "e2", "event_type": "call.connected", "workflow_id": "motor_renewal",
                           "call_id": "c1", "occurred_at": "2026-10-01 10:00:10", "phone": "9820010002"}) + "\n")
        f.write(json.dumps({"event_id": "e3", "event_type": "disposition.set", "workflow_id": "motor_renewal",
                           "call_id": "c1", "occurred_at": "2026-10-01 10:01:00", "phone": "9820010002",
                           "payload": {"disposition_code": "renewed"}}) + "\n")

        # Lead 3 calls (telephony failed):
        f.write(json.dumps({"event_id": "e4", "event_type": "call.initiated", "workflow_id": "motor_renewal",
                           "call_id": "c2", "occurred_at": "2026-10-01 11:00:00", "phone": "9820010003"}) + "\n")
        f.write(json.dumps({"event_id": "e5", "event_type": "call.telephony_failed", "workflow_id": "motor_renewal",
                           "call_id": "c2", "occurred_at": "2026-10-01 11:00:05", "phone": "9820010003",
                           "payload": {"reason": "sip_503"}}) + "\n")

        # Lead 4 calls (non-terminal: callback_requested):
        f.write(json.dumps({"event_id": "e6", "event_type": "call.initiated", "workflow_id": "motor_renewal",
                           "call_id": "c3", "occurred_at": "2026-10-01 12:00:00", "phone": "9820010004"}) + "\n")
        f.write(json.dumps({"event_id": "e7", "event_type": "disposition.set", "workflow_id": "motor_renewal",
                           "call_id": "c3", "occurred_at": "2026-10-01 12:01:00", "phone": "9820010004",
                           "payload": {"disposition_code": "callback_requested"}}) + "\n")

    ingest_event_jsonl(DiscoveredEventFile("drop_1", event_file), db, security, catalog)

    report_path = generate_batch_summary(db, catalog, reports_dir / "batch_summary.csv")
    with open(report_path, "r", encoding="utf-8") as f:
        reader = list(csv.DictReader(f))
        assert len(reader) == 1
        row = reader[0]
        assert row["workflow_id"] == "motor_renewal"
        assert row["batch_id"] == "batch_001"
        assert int(row["total_leads"]) == 4
        assert int(row["yet_to_reach_out"]) == 1
        assert int(row["completed"]) == 1
        assert int(row["in_progress_telephony_failed"]) == 1
        assert int(row["in_progress_dialed"]) == 1


def test_daily_overview_precision_and_success_leads(test_setup):
    """Test connectivity_rate formatting (4 decimals) and distinct success leads count."""
    db, security, catalog, registry, reports_dir, tmp = test_setup

    lead_file = tmp / "leads" / "motor_renewal" / "batch_001.csv"
    lead_file.parent.mkdir(parents=True, exist_ok=True)
    with open(lead_file, "w", newline="", encoding="utf-8") as f:
        f.write("customer_name,phone,policy_number\n")
        f.write("User A,+919820010001,MOT/A\n")
        f.write("User B,+919820010002,MOT/B\n")
        f.write("User C,+919820010003,MOT/C\n")

    ingest_lead_csv(DiscoveredLeadFile("drop_1", "motor_renewal", "batch_001", lead_file), db, security, registry)

    # Day 2026-10-01: 3 calls initiated, 1 connected -> rate = 1/3 = 0.3333
    # User A has 2 calls on that date, one renewed -> User A counts once in success_leads
    event_file = tmp / "events" / "events_001.jsonl"
    event_file.parent.mkdir(parents=True, exist_ok=True)
    with open(event_file, "w", encoding="utf-8") as f:
        # User A call 1 (connected & renewed)
        f.write(json.dumps({"event_id": "e1", "event_type": "call.initiated", "workflow_id": "motor_renewal",
                           "call_id": "c1", "occurred_at": "2026-10-01 10:00:00", "phone": "9820010001"}) + "\n")
        f.write(json.dumps({"event_id": "e2", "event_type": "call.connected", "workflow_id": "motor_renewal",
                           "call_id": "c1", "occurred_at": "2026-10-01 10:00:10", "phone": "9820010001"}) + "\n")
        f.write(json.dumps({"event_id": "e3", "event_type": "disposition.set", "workflow_id": "motor_renewal",
                           "call_id": "c1", "occurred_at": "2026-10-01 10:01:00", "phone": "9820010001",
                           "payload": {"disposition_code": "renewed"}}) + "\n")

        # User A call 2 (unanswered on same date)
        f.write(json.dumps({"event_id": "e4", "event_type": "call.initiated", "workflow_id": "motor_renewal",
                           "call_id": "c2", "occurred_at": "2026-10-01 14:00:00", "phone": "9820010001"}) + "\n")

        # User B call 3 (unanswered on same date)
        f.write(json.dumps({"event_id": "e5", "event_type": "call.initiated", "workflow_id": "motor_renewal",
                           "call_id": "c3", "occurred_at": "2026-10-01 15:00:00", "phone": "9820010002"}) + "\n")

    ingest_event_jsonl(DiscoveredEventFile("drop_1", event_file), db, security, catalog)

    report_path = generate_daily_overview(db, catalog, reports_dir / "daily_overview.csv")
    with open(report_path, "r", encoding="utf-8") as f:
        reader = list(csv.DictReader(f))
        assert len(reader) == 1
        row = reader[0]
        assert row["workflow_id"] == "motor_renewal"
        assert row["date"] == "2026-10-01"
        assert int(row["total_calls"]) == 3
        assert int(row["connected_calls"]) == 1
        assert row["connectivity_rate"] == "0.3333"
        assert int(row["success_leads"]) == 1


def test_lead_report_details_and_overrides(test_setup):
    """Test lead_report.csv: latest disposition, override precedence, ever_success."""
    db, security, catalog, registry, reports_dir, tmp = test_setup

    lead_file = tmp / "leads" / "motor_renewal" / "batch_001.csv"
    lead_file.parent.mkdir(parents=True, exist_ok=True)
    with open(lead_file, "w", newline="", encoding="utf-8") as f:
        f.write("customer_name,phone,policy_number\n")
        f.write("Test Customer,+919820012345,MOT/099\n")

    ingest_lead_csv(DiscoveredLeadFile("drop_1", "motor_renewal", "batch_001", lead_file), db, security, registry)

    event_file = tmp / "events" / "events_001.jsonl"
    event_file.parent.mkdir(parents=True, exist_ok=True)
    with open(event_file, "w", encoding="utf-8") as f:
        # Call 1: voice agent set not_interested
        f.write(json.dumps({"event_id": "e1", "event_type": "call.initiated", "workflow_id": "motor_renewal",
                           "call_id": "c1", "occurred_at": "2026-10-01 10:00:00", "phone": "9820012345"}) + "\n")
        f.write(json.dumps({"event_id": "e2", "event_type": "disposition.set", "workflow_id": "motor_renewal",
                           "call_id": "c1", "occurred_at": "2026-10-01 10:01:00", "phone": "9820012345",
                           "payload": {"disposition_code": "not_interested"}}) + "\n")
        # Supervisor override corrected c1 to renewed!
        f.write(json.dumps({"event_id": "e3", "event_type": "disposition.override", "workflow_id": "motor_renewal",
                           "call_id": "c1", "occurred_at": "2026-10-01 11:00:00",
                           "payload": {"disposition_code": "renewed", "overridden_by": "sup_1", "reason": "qa"}}) + "\n")

    ingest_event_jsonl(DiscoveredEventFile("drop_1", event_file), db, security, catalog)

    report_path = generate_lead_report(db, catalog, reports_dir / "lead_report.csv")
    with open(report_path, "r", encoding="utf-8") as f:
        reader = list(csv.DictReader(f))
        assert len(reader) == 1
        row = reader[0]
        assert row["workflow_id"] == "motor_renewal"
        assert row["external_ref"] == "MOT/099"
        assert row["batch_id"] == "batch_001"
        assert row["phone_masked"] == "+91******2345"
        assert row["bucket"] == "completed"
        assert int(row["total_calls"]) == 1
        assert row["last_call_at"] == "2026-10-01T10:00:00+05:30"
        # Overridden disposition took precedence over original set
        assert row["current_disposition"] == "renewed"
        assert row["ever_success"] == "true"


def test_full_dataset_reporting_verification(tmp_path):
    """
    Integration verification on actual synthetic dataset.
    Verifies that generate_all_reports produces exact required files and matches ground truth.
    """
    if not Path("data/drop_1").is_dir() or not Path("data/drop_2").is_dir():
        pytest.skip("Generated dataset not found")

    db_file = tmp_path / "full_test.db"
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

    ingest_leads_drop("data/drop_1", db, security, registry)
    ingest_leads_drop("data/drop_2", db, security, registry)
    ingest_events_drop("data/drop_1", db, security, catalog)
    ingest_events_drop("data/drop_2", db, security, catalog)

    reports_dir = tmp_path / "reports"
    reports = generate_all_reports(db, catalog, reports_dir)

    # 1. Verify data_quality.json
    with open(reports["data_quality.json"], "r", encoding="utf-8") as f:
        dq = json.load(f)
        assert dq["lead_rows_total"] == 2057
        assert dq["lead_rows_rejected"] == 108
        assert dq["duplicate_lead_rows_merged"] == 57
        assert dq["field_warnings"] == 122
        assert dq["events_total"] == 8745
        assert dq["malformed_events"] == 8
        assert dq["duplicate_events_dropped"] == 237
        assert dq["orphan_events"] == 462
        assert dq["unknown_disposition_events"] == 19

    # 2. Verify rejections.csv line count (1 header + 108 rows)
    with open(reports["rejections.csv"], "r", encoding="utf-8") as f:
        rejs = list(csv.reader(f))
        assert len(rejs) == 109
        # Verify strictly no customer names or full phone numbers in reasons
        for r in rejs[1:]:
            assert r[3].startswith("missing_required:") or r[3] == "invalid_phone"

    # 3. Verify lead_report.csv line count (1 header + 1892 leads)
    with open(reports["lead_report.csv"], "r", encoding="utf-8") as f:
        leads_rpt = list(csv.reader(f))
        assert len(leads_rpt) == 1893

    # 4. Verify batch_summary.csv sum of leads equals 1892
    with open(reports["batch_summary.csv"], "r", encoding="utf-8") as f:
        batches = list(csv.DictReader(f))
        assert len(batches) == 6  # 3 batches for motor_renewal + 3 for policy_feedback
        total_leads_sum = sum(int(b["total_leads"]) for b in batches)
        assert total_leads_sum == 1892
        completed_sum = sum(int(b["completed"]) for b in batches)
        yet_to_reach_sum = sum(int(b["yet_to_reach_out"]) for b in batches)
        in_prog_telephony_sum = sum(int(b["in_progress_telephony_failed"]) for b in batches)
        in_prog_dialed_sum = sum(int(b["in_progress_dialed"]) for b in batches)
        assert completed_sum + yet_to_reach_sum + in_prog_telephony_sum + in_prog_dialed_sum == 1892


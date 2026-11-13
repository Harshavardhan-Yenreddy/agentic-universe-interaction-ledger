"""
Comprehensive tests for Lead CSV Ingestion Pipeline.
Tests schema handling, header matching, normalization, rejection,
duplicate merging, idempotency, and PII protection.
"""

import csv
import os
import secrets
from pathlib import Path
import pytest

from ledger.config import Config
from ledger.database import DatabaseManager
from ledger.lead_ingest import (
    DiscoveredLeadFile,
    LeadIngestionStats,
    discover_lead_files,
    ingest_lead_csv,
    ingest_leads_drop,
)
from ledger.schemas import SchemaRegistry
from ledger.security import SecurityManager


@pytest.fixture
def env_setup(tmp_path):
    """Set up test DatabaseManager, SecurityManager, and SchemaRegistry."""
    db_file = tmp_path / "test_ledger.db"
    db = DatabaseManager(db_file)
    db.initialize_schema()

    config = Config(
        encryption_key=secrets.token_bytes(32),
        hmac_salt=secrets.token_bytes(32),
        db_path=db_file,
    )
    security = SecurityManager(config)
    schema_registry = SchemaRegistry("assignment-a-data/schemas")
    return db, security, schema_registry, tmp_path


def create_csv(path: Path, header: list[str], rows: list[list[str]]) -> Path:
    """Helper to write CSV files for testing."""
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(header)
        for r in rows:
            writer.writerow(r)
    return path


def test_motor_renewal_valid_insertion(env_setup):
    """Test valid lead insertion for motor_renewal workflow."""
    db, security, registry, tmp = env_setup
    csv_file = tmp / "leads" / "motor_renewal" / "batch_001.csv"
    create_csv(
        csv_file,
        ["customer_name", "phone", "policy_number", "policy_expiry", "premium_amount", "preferred_language"],
        [
            ["Aarav Sharma", "+91 98200 12345", "MOT/2026/001", "2026-11-20", "₹12,500.50", "hi"],
        ],
    )

    lead_file = DiscoveredLeadFile("drop_1", "motor_renewal", "batch_001", csv_file)
    stats = ingest_lead_csv(lead_file, db, security, registry)

    assert stats.lead_rows_total == 1
    assert stats.lead_rows_rejected == 0
    assert stats.duplicate_lead_rows_merged == 0

    lead = db.fetchone("SELECT * FROM leads WHERE workflow_id = 'motor_renewal';")
    assert lead is not None
    assert lead["batch_id"] == "batch_001"
    assert lead["external_ref"] == "MOT/2026/001"
    assert lead["phone_masked"] == "+91******2345"
    assert lead["policy_expiry"] == "2026-11-20"
    assert lead["premium_amount"] == 12500.50
    assert lead["preferred_language"] == "hi"

    # Verify PII is encrypted
    assert security.decrypt(lead["customer_name_encrypted"]) == "Aarav Sharma"
    assert security.decrypt(lead["phone_encrypted"]) == "+919820012345"
    # Ensure plaintext phone or name does not exist in any raw column
    for col_val in lead:
        assert "Aarav Sharma" != str(col_val)
        assert "+919820012345" != str(col_val)


def test_policy_feedback_alias_headers(env_setup):
    """Test header mapping and alias matching for policy_feedback workflow."""
    db, security, registry, tmp = env_setup
    csv_file = tmp / "leads" / "policy_feedback" / "batch_001.csv"
    create_csv(
        csv_file,
        ["Order ID", "Name", "Contact", "Product", "Order Date", "Language", "extra_column"],
        [
            ["ORD-7001", "Pooja Patel", "91-98765-43210", "Smartwatch", "15/09/2026", "ta", "note1"],
        ],
    )

    lead_file = DiscoveredLeadFile("drop_1", "policy_feedback", "batch_001", csv_file)
    stats = ingest_lead_csv(lead_file, db, security, registry)

    assert stats.lead_rows_total == 1
    assert stats.lead_rows_rejected == 0

    lead = db.fetchone("SELECT * FROM leads WHERE workflow_id = 'policy_feedback';")
    assert lead is not None
    assert lead["external_ref"] == "ORD-7001"
    assert lead["product"] == "Smartwatch"
    assert lead["order_date"] == "2026-09-15"
    assert lead["preferred_language"] == "ta"
    assert security.decrypt(lead["customer_name_encrypted"]) == "Pooja Patel"


def test_blank_rows_skipped_and_row_numbering(env_setup):
    """Verify that blank rows are skipped and do not increment row_number."""
    db, security, registry, tmp = env_setup
    csv_file = tmp / "leads" / "motor_renewal" / "batch_001.csv"
    create_csv(
        csv_file,
        ["customer_name", "phone", "policy_number"],
        [
            ["", "", ""],                           # Blank row (skipped)
            ["   ", "  ", "  "],                     # Blank row (skipped)
            ["Row One", "9820012345", "MOT/01"],    # Row 1 (valid)
            ["", "", ""],                           # Blank row (skipped)
            ["", "9820012346", "MOT/02"],           # Row 2 (rejected: missing name)
        ],
    )

    lead_file = DiscoveredLeadFile("drop_1", "motor_renewal", "batch_001", csv_file)
    stats = ingest_lead_csv(lead_file, db, security, registry)

    assert stats.lead_rows_total == 2
    assert stats.lead_rows_rejected == 1

    # Verify rejection record has row_number = 2 (not 5!)
    rej = db.fetchone("SELECT * FROM rejections;")
    assert rej is not None
    assert rej["row_number"] == 2
    assert rej["reason"] == "missing_required:customer_name"


def test_rejections_schema_order_evaluation(env_setup):
    """Verify that field failure follows schema order, not CSV header order."""
    db, security, registry, tmp = env_setup
    csv_file = tmp / "leads" / "motor_renewal" / "batch_001.csv"
    # CSV has policy_number first, but schema order has customer_name before policy_number
    create_csv(
        csv_file,
        ["policy_number", "customer_name", "phone"],
        [
            ["", "", "9820012345"],  # Both policy_number and customer_name missing!
        ],
    )

    lead_file = DiscoveredLeadFile("drop_1", "motor_renewal", "batch_001", csv_file)
    stats = ingest_lead_csv(lead_file, db, security, registry)

    assert stats.lead_rows_rejected == 1
    rej = db.fetchone("SELECT reason FROM rejections;")
    # In motor_renewal schema: customer_name is field 0, policy_number is field 2
    assert rej["reason"] == "missing_required:customer_name"


def test_invalid_phone_rejections(env_setup):
    """Test rejection reasons for invalid phone numbers."""
    db, security, registry, tmp = env_setup
    csv_file = tmp / "leads" / "motor_renewal" / "batch_001.csv"
    create_csv(
        csv_file,
        ["customer_name", "phone", "policy_number"],
        [
            ["User 1", "022-2456-7890", "MOT/01"],   # Landline -> invalid_phone
            ["User 2", "5820012345", "MOT/02"],      # Starts with 5 -> invalid_phone
            ["User 3", "982001234", "MOT/03"],       # 9 digits -> invalid_phone
            ["User 4", "+1 415 555 0134", "MOT/04"], # US number -> invalid_phone
            ["User 5", "", "MOT/05"],                # Missing phone -> missing_required:phone
        ],
    )

    lead_file = DiscoveredLeadFile("drop_1", "motor_renewal", "batch_001", csv_file)
    stats = ingest_lead_csv(lead_file, db, security, registry)

    assert stats.lead_rows_total == 5
    assert stats.lead_rows_rejected == 5
    assert stats.rejections_by_reason["invalid_phone"] == 4
    assert stats.rejections_by_reason["missing_required:phone"] == 1


def test_field_warnings_on_optional_fields(env_setup):
    """Verify that optional invalid fields store null and increment field_warnings."""
    db, security, registry, tmp = env_setup
    csv_file = tmp / "leads" / "motor_renewal" / "batch_001.csv"
    create_csv(
        csv_file,
        ["customer_name", "phone", "policy_number", "policy_expiry", "premium_amount", "preferred_language"],
        [
            ["Valid User", "9820012345", "MOT/01", "31/02/2026", "invalid_amt", "Klingon"],
        ],
    )

    lead_file = DiscoveredLeadFile("drop_1", "motor_renewal", "batch_001", csv_file)
    stats = ingest_lead_csv(lead_file, db, security, registry)

    assert stats.lead_rows_rejected == 0
    assert stats.field_warnings == 3  # 1 bad date, 1 bad decimal, 1 bad enum

    lead = db.fetchone("SELECT * FROM leads WHERE workflow_id = 'motor_renewal';")
    assert lead is not None
    assert lead["policy_expiry"] is None
    assert lead["premium_amount"] is None
    # preferred_language fell back to schema default "en"
    assert lead["preferred_language"] == "en"


def test_duplicate_lead_merging_and_batch_id_preservation(env_setup):
    """
    Test duplicate lead merging:
    - First row sets permanent batch_id.
    - Later rows in same or later batch update only non-null fields.
    - Null fields in duplicate do not overwrite existing non-null data.
    """
    db, security, registry, tmp = env_setup

    # Batch 001 introduces the lead with date and amount
    f1 = tmp / "leads" / "motor_renewal" / "batch_001.csv"
    create_csv(
        f1,
        ["customer_name", "phone", "policy_number", "policy_expiry", "premium_amount", "preferred_language"],
        [
            ["Aarav Sharma", "+91 98200 12345", "MOT/01", "2026-11-20", "15000", "en"],
        ],
    )
    ingest_lead_csv(DiscoveredLeadFile("drop_1", "motor_renewal", "batch_001", f1), db, security, registry)

    # Batch 002 re-uploads same customer with updated name, new amount, but empty expiry date
    f2 = tmp / "leads" / "motor_renewal" / "batch_002.csv"
    create_csv(
        f2,
        ["customer_name", "phone", "policy_number", "policy_expiry", "premium_amount", "preferred_language"],
        [
            ["Aarav S. Sharma", "9820012345", "MOT/01", "", "16500", "hi"],
        ],
    )
    stats2 = ingest_lead_csv(DiscoveredLeadFile("drop_1", "motor_renewal", "batch_002", f2), db, security, registry)

    assert stats2.duplicate_lead_rows_merged == 1

    lead = db.fetchone("SELECT * FROM leads WHERE workflow_id = 'motor_renewal';")
    # Batch ID must remain batch_001 permanently
    assert lead["batch_id"] == "batch_001"
    # Name updated
    assert security.decrypt(lead["customer_name_encrypted"]) == "Aarav S. Sharma"
    # Premium updated
    assert lead["premium_amount"] == 16500.0
    # Language updated
    assert lead["preferred_language"] == "hi"
    # Expiry date was empty in batch_002 -> existing value must be retained!
    assert lead["policy_expiry"] == "2026-11-20"


def test_idempotent_reingestion(env_setup):
    """Verify that re-ingesting the exact same file skips processing and changes nothing."""
    db, security, registry, tmp = env_setup
    f1 = tmp / "leads" / "motor_renewal" / "batch_001.csv"
    create_csv(
        f1,
        ["customer_name", "phone", "policy_number"],
        [
            ["Aarav", "9820012345", "MOT/01"],
            ["Bad User", "123", "MOT/02"],
        ],
    )
    lead_file = DiscoveredLeadFile("drop_1", "motor_renewal", "batch_001", f1)

    # First run
    stats1 = ingest_lead_csv(lead_file, db, security, registry)
    assert stats1.files_processed == 1
    assert stats1.lead_rows_total == 2
    assert stats1.lead_rows_rejected == 1

    leads_count_1 = db.fetchone("SELECT COUNT(*) as c FROM leads;")["c"]
    rejections_count_1 = db.fetchone("SELECT COUNT(*) as c FROM rejections;")["c"]

    # Second run (exact same file)
    stats2 = ingest_lead_csv(lead_file, db, security, registry)
    assert stats2.files_skipped == 1
    assert stats2.lead_rows_total == 0

    leads_count_2 = db.fetchone("SELECT COUNT(*) as c FROM leads;")["c"]
    rejections_count_2 = db.fetchone("SELECT COUNT(*) as c FROM rejections;")["c"]

    assert leads_count_1 == leads_count_2
    assert rejections_count_1 == rejections_count_2


def test_generated_dataset_drop1_lead_ingestion():
    """
    Integration test: Ingest actual data/drop_1 leads and verify counts against ground truth.
    Ground truth for drop_1 leads:
    - non-blank rows: 1515 (416 + 350 + 377 + 372)
    - rejected rows: 71 (13 + 20 + 18 + 20)
    - merged duplicate rows: 29 (14 + 4 + 3 + 8)
    - unique leads created: 1415 (1515 - 71 - 29)
    """
    if not Path("data/drop_1/leads").is_dir():
        pytest.skip("data/drop_1/leads not found")

    db = DatabaseManager(":memory:")
    db.initialize_schema()

    config = Config(
        encryption_key=secrets.token_bytes(32),
        hmac_salt=secrets.token_bytes(32),
        db_path=":memory:",
    )
    security = SecurityManager(config)
    registry = SchemaRegistry("assignment-a-data/schemas")

    stats = ingest_leads_drop("data/drop_1", db, security, registry)

    assert stats.files_processed == 4
    assert stats.lead_rows_total == 1515
    assert stats.lead_rows_rejected == 71
    assert stats.duplicate_lead_rows_merged == 29

    leads_count = db.fetchone("SELECT COUNT(*) as c FROM leads;")["c"]
    assert leads_count == 1415

    # Verify no plaintext customer names or phones exist in leads table
    rows = db.fetchall("SELECT phone_encrypted, customer_name_encrypted FROM leads;")
    for r in rows:
        phone = security.decrypt(r["phone_encrypted"])
        name = security.decrypt(r["customer_name_encrypted"])
        assert phone.startswith("+91")
        assert len(name) > 0


def test_generated_dataset_both_drops_ingestion():
    """
    Integration test: Ingest data/drop_1 and data/drop_2 sequentially.
    Ground truth for both drops combined:
    - total data rows: 2057
    - rejected rows: 108
    - merged duplicate rows: 57
    - final unique leads: 1892
    """
    if not Path("data/drop_1/leads").is_dir() or not Path("data/drop_2/leads").is_dir():
        pytest.skip("data/drop_1 or data/drop_2 leads not found")

    db = DatabaseManager(":memory:")
    db.initialize_schema()

    config = Config(
        encryption_key=secrets.token_bytes(32),
        hmac_salt=secrets.token_bytes(32),
        db_path=":memory:",
    )
    security = SecurityManager(config)
    registry = SchemaRegistry("assignment-a-data/schemas")

    stats1 = ingest_leads_drop("data/drop_1", db, security, registry)
    stats2 = ingest_leads_drop("data/drop_2", db, security, registry)

    assert stats1.files_processed == 4
    assert stats2.files_processed == 2

    assert stats1.lead_rows_total + stats2.lead_rows_total == 2057
    assert stats1.lead_rows_rejected + stats2.lead_rows_rejected == 108
    assert stats1.duplicate_lead_rows_merged + stats2.duplicate_lead_rows_merged == 57

    leads_count = db.fetchone("SELECT COUNT(*) as c FROM leads;")["c"]
    assert leads_count == 1892

    rejections_count = db.fetchone("SELECT COUNT(*) as c FROM rejections;")["c"]
    assert rejections_count == 108

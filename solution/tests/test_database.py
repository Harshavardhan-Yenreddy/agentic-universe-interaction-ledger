"""
Unit tests for SQLite database schema, constraints, and append-only triggers.
"""

import sqlite3
import pytest

from ledger.database import DatabaseManager


@pytest.fixture
def db():
    """Create an in-memory SQLite database initialized with our schema."""
    manager = DatabaseManager(":memory:")
    manager.initialize_schema()
    return manager


def test_schema_tables_exist(db):
    """Verify that all required tables and indexes are created successfully."""
    rows = db.fetchall(
        "SELECT name, type FROM sqlite_master WHERE type IN ('table', 'trigger') ORDER BY name;"
    )
    names = {r["name"] for r in rows}

    expected_tables = {
        "leads",
        "interaction_ledger",
        "ingested_files",
        "rejections",
        "data_quality",
    }
    expected_triggers = {
        "prevent_interaction_ledger_update",
        "prevent_interaction_ledger_delete",
    }
    assert expected_tables.issubset(names)
    assert expected_triggers.issubset(names)


def test_leads_unique_constraint(db):
    """Verify that (workflow_id, phone_hash) unique constraint is strictly enforced."""
    with db.transaction() as conn:
        conn.execute(
            """
            INSERT INTO leads (
                workflow_id, phone_hash, phone_encrypted, customer_name_encrypted,
                phone_masked, external_ref, batch_id
            ) VALUES (?, ?, ?, ?, ?, ?, ?);
            """,
            ("motor_renewal", "hash_001", "enc_p", "enc_n", "+91******1234", "REF-001", "batch_001"),
        )

    # Attempting to insert identical (workflow_id, phone_hash) must raise IntegrityError
    with pytest.raises(sqlite3.IntegrityError):
        with db.transaction() as conn:
            conn.execute(
                """
                INSERT INTO leads (
                    workflow_id, phone_hash, phone_encrypted, customer_name_encrypted,
                    phone_masked, external_ref, batch_id
                ) VALUES (?, ?, ?, ?, ?, ?, ?);
                """,
                ("motor_renewal", "hash_001", "enc_p2", "enc_n2", "+91******1234", "REF-002", "batch_002"),
            )


def test_leads_different_workflows_same_hash(db):
    """Verify that the same phone hash in a different workflow is permitted."""
    with db.transaction() as conn:
        conn.execute(
            """
            INSERT INTO leads (
                workflow_id, phone_hash, phone_encrypted, customer_name_encrypted,
                phone_masked, external_ref, batch_id
            ) VALUES (?, ?, ?, ?, ?, ?, ?);
            """,
            ("motor_renewal", "hash_001", "enc_p", "enc_n", "+91******1234", "MOT/01", "batch_001"),
        )
        conn.execute(
            """
            INSERT INTO leads (
                workflow_id, phone_hash, phone_encrypted, customer_name_encrypted,
                phone_masked, external_ref, batch_id
            ) VALUES (?, ?, ?, ?, ?, ?, ?);
            """,
            ("policy_feedback", "hash_001", "enc_p", "enc_n", "+91******1234", "ORD/01", "batch_001"),
        )

    count = db.fetchone("SELECT COUNT(*) as cnt FROM leads;")["cnt"]
    assert count == 2


def test_interaction_ledger_unique_event_id(db):
    """Verify that event_id primary key constraint prevents duplicate event IDs."""
    with db.transaction() as conn:
        conn.execute(
            """
            INSERT INTO interaction_ledger (
                event_id, drop_id, event_type, workflow_id, call_id, phone_hash,
                occurred_at_ist, date_ist, payload_json
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?);
            """,
            (
                "evt_001", "drop_1", "call.initiated", "motor_renewal", "call_001",
                "hash_001", "2026-10-01T10:00:00+05:30", "2026-10-01", "{}"
            ),
        )

    with pytest.raises(sqlite3.IntegrityError):
        with db.transaction() as conn:
            conn.execute(
                """
                INSERT INTO interaction_ledger (
                    event_id, drop_id, event_type, workflow_id, call_id, phone_hash,
                    occurred_at_ist, date_ist, payload_json
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?);
                """,
                (
                    "evt_001", "drop_2", "call.connected", "motor_renewal", "call_001",
                    "hash_001", "2026-10-01T10:01:00+05:30", "2026-10-01", "{}"
                ),
            )


def test_interaction_ledger_append_only_update_prevented(db):
    """Verify that the trigger blocks UPDATE on interaction_ledger."""
    with db.transaction() as conn:
        conn.execute(
            """
            INSERT INTO interaction_ledger (
                event_id, drop_id, event_type, workflow_id, call_id, phone_hash,
                occurred_at_ist, date_ist, payload_json
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?);
            """,
            (
                "evt_002", "drop_1", "call.initiated", "motor_renewal", "call_002",
                "hash_002", "2026-10-01T10:00:00+05:30", "2026-10-01", "{}"
            ),
        )

    with pytest.raises(sqlite3.DatabaseError, match="interaction_ledger is append-only: UPDATE"):
        with db.transaction() as conn:
            conn.execute("UPDATE interaction_ledger SET event_type = 'tampered' WHERE event_id = 'evt_002';")


def test_interaction_ledger_append_only_delete_prevented(db):
    """Verify that the trigger blocks DELETE on interaction_ledger."""
    with db.transaction() as conn:
        conn.execute(
            """
            INSERT INTO interaction_ledger (
                event_id, drop_id, event_type, workflow_id, call_id, phone_hash,
                occurred_at_ist, date_ist, payload_json
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?);
            """,
            (
                "evt_003", "drop_1", "call.initiated", "motor_renewal", "call_003",
                "hash_003", "2026-10-01T10:00:00+05:30", "2026-10-01", "{}"
            ),
        )

    with pytest.raises(sqlite3.DatabaseError, match="interaction_ledger is append-only: DELETE"):
        with db.transaction() as conn:
            conn.execute("DELETE FROM interaction_ledger WHERE event_id = 'evt_003';")


def test_rejections_unique_constraint(db):
    """Verify that rejections audit uniqueness constraint is enforced."""
    with db.transaction() as conn:
        conn.execute(
            """
            INSERT INTO rejections (drop_id, workflow_id, batch_id, row_number, reason)
            VALUES (?, ?, ?, ?, ?);
            """,
            ("drop_1", "motor_renewal", "batch_001", 1, "invalid_phone"),
        )

    with pytest.raises(sqlite3.IntegrityError):
        with db.transaction() as conn:
            conn.execute(
                """
                INSERT INTO rejections (drop_id, workflow_id, batch_id, row_number, reason)
                VALUES (?, ?, ?, ?, ?);
                """,
                ("drop_1", "motor_renewal", "batch_001", 1, "invalid_phone"),
            )


def test_ingested_files_idempotency(db):
    """Verify that ingested_files primary key constraint prevents duplicate tracking."""
    with db.transaction() as conn:
        conn.execute(
            """
            INSERT INTO ingested_files (drop_id, file_path, file_type, file_hash, record_count)
            VALUES (?, ?, ?, ?, ?);
            """,
            ("drop_1", "leads/motor_renewal/batch_001.csv", "leads", "hash123", 416),
        )

    with pytest.raises(sqlite3.IntegrityError):
        with db.transaction() as conn:
            conn.execute(
                """
                INSERT INTO ingested_files (drop_id, file_path, file_type, file_hash, record_count)
                VALUES (?, ?, ?, ?, ?);
                """,
                ("drop_1", "leads/motor_renewal/batch_001.csv", "leads", "hash123", 416),
            )


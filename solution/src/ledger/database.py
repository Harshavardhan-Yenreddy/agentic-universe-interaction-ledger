"""
Database layer for the Interaction Ledger using SQLite.
Implements the relational schema, constraints, triggers, and indices.
"""

from __future__ import annotations

import sqlite3
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Generator, Iterable, Optional

SCHEMA_SQL = """
-- 1. Ingested Files (Enforces Idempotency across drops and files)
CREATE TABLE IF NOT EXISTS ingested_files (
    drop_id TEXT NOT NULL,
    file_path TEXT NOT NULL,
    file_type TEXT NOT NULL CHECK(file_type IN ('leads', 'events')),
    file_hash TEXT NOT NULL,
    record_count INTEGER NOT NULL DEFAULT 0,
    ingested_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (drop_id, file_path)
);

-- 2. Leads Table (Encrypted PII, Deterministic Phone Index, Business Fields)
CREATE TABLE IF NOT EXISTS leads (
    lead_id INTEGER PRIMARY KEY AUTOINCREMENT,
    workflow_id TEXT NOT NULL,
    phone_hash TEXT NOT NULL,
    phone_encrypted TEXT NOT NULL,
    customer_name_encrypted TEXT NOT NULL,
    phone_masked TEXT NOT NULL,
    external_ref TEXT NOT NULL,
    batch_id TEXT NOT NULL,
    policy_expiry TEXT,
    order_date TEXT,
    premium_amount REAL,
    product TEXT,
    preferred_language TEXT NOT NULL DEFAULT 'en',
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (workflow_id, phone_hash)
);

CREATE INDEX IF NOT EXISTS idx_leads_external_ref ON leads(workflow_id, external_ref);
CREATE INDEX IF NOT EXISTS idx_leads_batch ON leads(workflow_id, batch_id);
CREATE INDEX IF NOT EXISTS idx_leads_phone_hash ON leads(phone_hash);

-- 3. Append-Only Interaction Ledger
CREATE TABLE IF NOT EXISTS interaction_ledger (
    event_id TEXT PRIMARY KEY,
    drop_id TEXT NOT NULL,
    event_type TEXT NOT NULL,
    workflow_id TEXT NOT NULL,
    call_id TEXT NOT NULL,
    phone_hash TEXT,
    occurred_at_ist TIMESTAMP NOT NULL,
    date_ist TEXT NOT NULL,
    payload_json TEXT NOT NULL DEFAULT '{}',
    ingested_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_ledger_call_id ON interaction_ledger(call_id);
CREATE INDEX IF NOT EXISTS idx_ledger_phone_hash ON interaction_ledger(workflow_id, phone_hash);
CREATE INDEX IF NOT EXISTS idx_ledger_date_ist ON interaction_ledger(workflow_id, date_ist);
CREATE INDEX IF NOT EXISTS idx_ledger_event_type ON interaction_ledger(event_type);

-- Triggers enforcing append-only immutability on interaction_ledger
CREATE TRIGGER IF NOT EXISTS prevent_interaction_ledger_update
BEFORE UPDATE ON interaction_ledger
BEGIN
    SELECT RAISE(FAIL, 'interaction_ledger is append-only: UPDATE operations are strictly prohibited');
END;

CREATE TRIGGER IF NOT EXISTS prevent_interaction_ledger_delete
BEFORE DELETE ON interaction_ledger
BEGIN
    SELECT RAISE(FAIL, 'interaction_ledger is append-only: DELETE operations are strictly prohibited');
END;

-- 4. Rejections Table (Audit log of rejected lead rows, strictly NO PII)
CREATE TABLE IF NOT EXISTS rejections (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    drop_id TEXT NOT NULL,
    workflow_id TEXT NOT NULL,
    batch_id TEXT NOT NULL,
    row_number INTEGER NOT NULL,
    reason TEXT NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (drop_id, workflow_id, batch_id, row_number)
);

CREATE INDEX IF NOT EXISTS idx_rejections_sort ON rejections(workflow_id, batch_id, row_number);

-- 5. Data Quality Ledger (Tracks metrics per drop)
CREATE TABLE IF NOT EXISTS data_quality (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    drop_id TEXT NOT NULL UNIQUE,
    lead_rows_total INTEGER NOT NULL DEFAULT 0,
    lead_rows_rejected INTEGER NOT NULL DEFAULT 0,
    rejections_by_reason_json TEXT NOT NULL DEFAULT '{}',
    duplicate_lead_rows_merged INTEGER NOT NULL DEFAULT 0,
    field_warnings INTEGER NOT NULL DEFAULT 0,
    events_total INTEGER NOT NULL DEFAULT 0,
    malformed_events INTEGER NOT NULL DEFAULT 0,
    duplicate_events_dropped INTEGER NOT NULL DEFAULT 0,
    orphan_events INTEGER NOT NULL DEFAULT 0,
    unknown_disposition_events INTEGER NOT NULL DEFAULT 0,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);
"""


class DatabaseManager:
    """Manages SQLite connections, transactions, and schema execution."""

    def __init__(self, db_path: Path | str) -> None:
        self.raw_path = str(db_path)
        self.is_memory = self.raw_path == ":memory:" or "mode=memory" in self.raw_path
        if self.is_memory:
            # Use unique shared in-memory database URI with keepalive connection
            import uuid
            self.uri = f"file:mem_{uuid.uuid4().hex}?mode=memory&cache=shared"
            self._keepalive = sqlite3.connect(self.uri, uri=True)
            self._keepalive.execute("PRAGMA foreign_keys = ON;")
            self.connection_target = self.uri
            self.use_uri = True
        else:
            self.db_path = Path(db_path)
            self.db_path.parent.mkdir(parents=True, exist_ok=True)
            self.connection_target = str(self.db_path)
            self.use_uri = False
            self._keepalive = None

    def get_connection(self) -> sqlite3.Connection:
        """Create and configure a new SQLite connection with recommended PRAGMAs."""
        conn = sqlite3.connect(
            self.connection_target,
            uri=self.use_uri,
            timeout=30.0,
        )
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA foreign_keys = ON;")
        if not self.is_memory:
            conn.execute("PRAGMA journal_mode = WAL;")
            conn.execute("PRAGMA synchronous = NORMAL;")
        conn.execute("PRAGMA busy_timeout = 5000;")
        return conn

    @contextmanager
    def transaction(self) -> Generator[sqlite3.Connection, None, None]:
        """Context manager providing an atomic database transaction."""
        conn = self.get_connection()
        try:
            yield conn
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

    def initialize_schema(self) -> None:
        """Execute the DDL schema to create tables, indexes, and triggers."""
        with self.transaction() as conn:
            conn.executescript(SCHEMA_SQL)

    def execute(self, query: str, params: Iterable[Any] = ()) -> int:
        """Execute a parameterized query and return the affected row count."""
        with self.transaction() as conn:
            cur = conn.execute(query, params)
            return cur.rowcount

    def fetchone(self, query: str, params: Iterable[Any] = ()) -> Optional[sqlite3.Row]:
        """Fetch a single row using a parameterized query."""
        conn = self.get_connection()
        try:
            cur = conn.execute(query, params)
            return cur.fetchone()
        finally:
            conn.close()

    def fetchall(self, query: str, params: Iterable[Any] = ()) -> list[sqlite3.Row]:
        """Fetch all rows using a parameterized query."""
        conn = self.get_connection()
        try:
            cur = conn.execute(query, params)
            return cur.fetchall()
        finally:
            conn.close()

    def close(self) -> None:
        """Close keepalive connection if active."""
        if self._keepalive is not None:
            try:
                self._keepalive.close()
            except Exception:
                pass
            self._keepalive = None

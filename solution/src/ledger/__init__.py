"""
Agentic Universe - Interaction Ledger & Reporting Pipeline
Solution package.
"""

from .config import Config, ConfigurationError
from .database import DatabaseManager
from .dispositions import Disposition, DispositionsCatalog
from .event_ingest import (
    DiscoveredEventFile,
    EventIngestionStats,
    count_linked_events,
    count_orphan_events,
    discover_event_files,
    ingest_event_jsonl,
    ingest_events_drop,
    parse_event_timestamp,
)
from .lead_ingest import (
    DiscoveredLeadFile,
    LeadIngestionStats,
    discover_lead_files,
    ingest_lead_csv,
    ingest_leads_drop,
)
from .reports import (
    generate_all_reports,
    generate_batch_summary,
    generate_daily_overview,
    generate_data_quality_report,
    generate_lead_report,
    generate_rejections_report,
)
from .schemas import FieldDefinition, SchemaRegistry, WorkflowSchema, normalize_header
from .security import SecurityError, SecurityManager, hash_phone, mask_phone
from .validation import (
    normalize_phone,
    parse_date,
    parse_decimal,
    parse_enum,
)

__all__ = [
    "Config",
    "ConfigurationError",
    "DatabaseManager",
    "SecurityManager",
    "SecurityError",
    "hash_phone",
    "mask_phone",
    "normalize_phone",
    "parse_date",
    "parse_decimal",
    "parse_enum",
    "FieldDefinition",
    "SchemaRegistry",
    "WorkflowSchema",
    "normalize_header",
    "Disposition",
    "DispositionsCatalog",
    "DiscoveredLeadFile",
    "LeadIngestionStats",
    "discover_lead_files",
    "ingest_lead_csv",
    "ingest_leads_drop",
    "DiscoveredEventFile",
    "EventIngestionStats",
    "discover_event_files",
    "ingest_event_jsonl",
    "ingest_events_drop",
    "parse_event_timestamp",
    "count_orphan_events",
    "count_linked_events",
    "generate_all_reports",
    "generate_batch_summary",
    "generate_daily_overview",
    "generate_lead_report",
    "generate_rejections_report",
    "generate_data_quality_report",
]

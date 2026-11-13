"""
Command Line Interface (CLI) for Agentic Universe Interaction Ledger Pipeline.
Commands:
  ingest <drop_dir>       Ingest lead CSVs and event JSONLs for a drop
  report --out <dir>      Generate the five required reports
  init-db                 Initialize SQLite database schema
  pipeline                Run full ingestion and report generation
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

from .config import Config, ConfigurationError
from .database import DatabaseManager
from .dispositions import DispositionsCatalog
from .event_ingest import count_linked_events, count_orphan_events, ingest_events_drop
from .lead_ingest import ingest_leads_drop
from .reports import generate_all_reports
from .schemas import SchemaRegistry
from .security import SecurityManager


def cmd_init_db(args: argparse.Namespace) -> int:
    """Initialize SQLite database tables, indexes, and triggers."""
    config = Config.from_env(db_path_override=args.db, allow_auto_generate=True)
    db = DatabaseManager(config.db_path)
    db.initialize_schema()
    print(f"[OK] Initialized database schema at: {config.db_path}")
    return 0


def cmd_ingest(args: argparse.Namespace) -> int:
    """Ingest leads and events for a given drop directory."""
    drop_dir = Path(args.drop_dir)
    if not drop_dir.is_dir():
        print(f"[ERROR] Drop directory not found: {drop_dir}", file=sys.stderr)
        return 1

    config = Config.from_env(db_path_override=args.db, allow_auto_generate=True)
    db = DatabaseManager(config.db_path)
    db.initialize_schema()

    security = SecurityManager(config)
    schema_registry = SchemaRegistry()
    catalog = DispositionsCatalog()

    print(f"=== Starting Ingestion: {drop_dir.name} ===")

    # 1. Lead Ingestion
    print(f"[*] Ingesting leads from {drop_dir / 'leads'}...")
    lead_stats = ingest_leads_drop(drop_dir, db, security, schema_registry)
    print(
        f"    Leads: {lead_stats.files_processed} files processed ({lead_stats.files_skipped} skipped), "
        f"{lead_stats.lead_rows_total} rows evaluated, "
        f"{lead_stats.lead_rows_rejected} rejected, "
        f"{lead_stats.duplicate_lead_rows_merged} duplicates merged, "
        f"{lead_stats.field_warnings} field warnings."
    )

    # 2. Event Ingestion
    print(f"[*] Ingesting events from {drop_dir / 'events'}...")
    event_stats = ingest_events_drop(drop_dir, db, security, catalog)
    print(
        f"    Events: {event_stats.files_processed} files processed ({event_stats.files_skipped} skipped), "
        f"{event_stats.events_total} lines evaluated, "
        f"{event_stats.valid_events_inserted} valid events stored, "
        f"{event_stats.malformed_events} malformed, "
        f"{event_stats.duplicate_events_dropped} duplicates dropped, "
        f"{event_stats.unknown_disposition_events} unknown dispositions."
    )

    # 3. Orphan status
    orphans = count_orphan_events(db)
    linked = count_linked_events(db)
    print(f"[*] Current Interaction Ledger State: {linked} linked events, {orphans} orphan events.")
    print(f"[OK] Ingestion complete for {drop_dir.name}\n")
    return 0


def cmd_report(args: argparse.Namespace) -> int:
    """Generate all five required reports into output directory."""
    config = Config.from_env(db_path_override=args.db, allow_auto_generate=True)
    db = DatabaseManager(config.db_path)
    catalog = DispositionsCatalog()
    out_dir = Path(args.out)

    print(f"=== Generating Reports into {out_dir} ===")
    reports = generate_all_reports(db, catalog, out_dir)

    for name, path in reports.items():
        print(f"  [+] {name} -> {path}")

    # Read and print summary from data_quality.json
    dq_file = reports["data_quality.json"]
    import json
    with open(dq_file, "r", encoding="utf-8") as f:
        dq_data = json.load(f)

    print("\n=== Data Quality Summary ===")
    print(f"  Total Lead Rows:            {dq_data.get('lead_rows_total', 0)}")
    print(f"  Rejected Lead Rows:         {dq_data.get('lead_rows_rejected', 0)}")
    print(f"  Duplicate Leads Merged:     {dq_data.get('duplicate_lead_rows_merged', 0)}")
    print(f"  Field Warnings:             {dq_data.get('field_warnings', 0)}")
    print(f"  Total Event Lines:          {dq_data.get('events_total', 0)}")
    print(f"  Malformed Events:           {dq_data.get('malformed_events', 0)}")
    print(f"  Duplicate Events Dropped:   {dq_data.get('duplicate_events_dropped', 0)}")
    print(f"  Current Orphan Events:      {dq_data.get('orphan_events', 0)}")
    print(f"  Unknown Dispositions:       {dq_data.get('unknown_disposition_events', 0)}")
    print(f"[OK] All 5 reports generated successfully.\n")
    return 0


def cmd_pipeline(args: argparse.Namespace) -> int:
    """Run full pipeline: ingest all specified drops and generate reports."""
    for drop in args.drops:
        drop_args = argparse.Namespace(drop_dir=drop, db=args.db)
        ret = cmd_ingest(drop_args)
        if ret != 0:
            return ret

    report_args = argparse.Namespace(out=args.out, db=args.db)
    return cmd_report(report_args)


def build_parser() -> argparse.ArgumentParser:
    """Build the argument parser for the pipeline CLI."""
    parser = argparse.ArgumentParser(
        prog="ledger",
        description="Agentic Universe - Interaction Ledger & Reporting Pipeline",
    )
    parser.add_argument("--db", default=None, help="Path to SQLite database file")

    subparsers = parser.add_subparsers(dest="command", required=True)

    # init-db
    init_parser = subparsers.add_parser("init-db", help="Initialize SQLite database schema")
    init_parser.set_defaults(func=cmd_init_db)

    # ingest
    ingest_parser = subparsers.add_parser("ingest", help="Ingest a drop directory (leads and events)")
    ingest_parser.add_argument("drop_dir", help="Path to drop directory (e.g. data/drop_1)")
    ingest_parser.set_defaults(func=cmd_ingest)

    # report
    report_parser = subparsers.add_parser("report", help="Generate all 5 contract reports")
    report_parser.add_argument("--out", default="reports", help="Directory where reports will be written")
    report_parser.set_defaults(func=cmd_report)

    # pipeline
    pipe_parser = subparsers.add_parser("pipeline", help="Run end-to-end ingestion and reporting")
    pipe_parser.add_argument("--drops", nargs="+", default=["data/drop_1", "data/drop_2"], help="Drop directories to ingest")
    pipe_parser.add_argument("--out", default="reports", help="Output directory for reports")
    pipe_parser.set_defaults(func=cmd_pipeline)

    return parser


def main(argv: Optional[list[str]] = None) -> int:
    """CLI entrypoint."""
    parser = build_parser()
    args = parser.parse_args(argv)
    try:
        return args.func(args)
    except ConfigurationError as e:
        print(f"[CONFIGURATION ERROR] {e}", file=sys.stderr)
        return 2
    except Exception as e:
        print(f"[FATAL ERROR] {e}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())

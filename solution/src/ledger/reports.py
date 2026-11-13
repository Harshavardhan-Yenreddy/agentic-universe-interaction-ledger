"""
Report Generation Engine for Interaction Ledger.
Produces the five exact business and quality reports matching the assignment contract:
1. batch_summary.csv
2. daily_overview.csv
3. lead_report.csv
4. rejections.csv
5. data_quality.json
"""

from __future__ import annotations

import csv
import json
from collections import Counter, defaultdict
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple

from .database import DatabaseManager
from .dispositions import DispositionsCatalog
from .event_ingest import count_orphan_events


def _fetch_call_aggregates(
    db: DatabaseManager,
    catalog: DispositionsCatalog,
) -> Tuple[
    dict[tuple[str, str], list[sqlite3.Row]],  # lead_calls: (wf, phone_hash) -> [call_row]
    set[str],                                  # connected_cids: set of call_ids
    set[str],                                  # failed_cids: set of call_ids
    dict[str, Optional[str]],                  # call_eff_disp: call_id -> effective_disposition
]:
    """
    Fetch and derive all call facts:
    - Initiated calls linked to a valid lead
    - Connected calls
    - Telephony failed calls
    - Effective disposition per call
    """
    # 1. Initiated calls linked to leads (a call is counted only if call.initiated is present and linked)
    calls = db.fetchall(
        """
        SELECT c.call_id, c.workflow_id, c.phone_hash, c.occurred_at_ist, c.date_ist
        FROM interaction_ledger c
        JOIN leads l ON l.workflow_id = c.workflow_id AND l.phone_hash = c.phone_hash
        WHERE c.event_type = 'call.initiated'
        ORDER BY c.occurred_at_ist ASC;
        """
    )

    # 2. Connected call IDs
    conns = db.fetchall("SELECT DISTINCT call_id FROM interaction_ledger WHERE event_type = 'call.connected';")
    connected_cids = {r["call_id"] for r in conns}

    # 3. Telephony failed call IDs
    fails = db.fetchall("SELECT DISTINCT call_id FROM interaction_ledger WHERE event_type = 'call.telephony_failed';")
    failed_cids = {r["call_id"] for r in fails}

    # 4. Supervisor overrides (takes latest by occurred_at)
    overrides = db.fetchall(
        """
        SELECT call_id, workflow_id, occurred_at_ist, payload_json
        FROM interaction_ledger
        WHERE event_type = 'disposition.override'
        ORDER BY occurred_at_ist DESC;
        """
    )
    call_overrides: dict[str, str] = {}
    for r in overrides:
        cid = r["call_id"]
        if cid in call_overrides:
            continue
        try:
            p = json.loads(r["payload_json"])
            code = p.get("disposition_code")
            if code and catalog.is_valid(r["workflow_id"], str(code).strip()):
                call_overrides[cid] = str(code).strip()
        except Exception:
            pass

    # 5. Agent dispositions (takes latest by occurred_at)
    sets = db.fetchall(
        """
        SELECT call_id, workflow_id, occurred_at_ist, payload_json
        FROM interaction_ledger
        WHERE event_type = 'disposition.set'
        ORDER BY occurred_at_ist DESC;
        """
    )
    call_sets: dict[str, str] = {}
    for r in sets:
        cid = r["call_id"]
        if cid in call_sets:
            continue
        try:
            p = json.loads(r["payload_json"])
            code = p.get("disposition_code")
            if code and catalog.is_valid(r["workflow_id"], str(code).strip()):
                call_sets[cid] = str(code).strip()
        except Exception:
            pass

    # 6. Effective disposition per call: latest override, else latest set, else None
    call_eff_disp: dict[str, Optional[str]] = {}
    for c in calls:
        cid = c["call_id"]
        if cid in call_overrides:
            call_eff_disp[cid] = call_overrides[cid]
        elif cid in call_sets:
            call_eff_disp[cid] = call_sets[cid]
        else:
            call_eff_disp[cid] = None

    # Group calls by lead (workflow_id, phone_hash)
    lead_calls: dict[tuple[str, str], list[Any]] = defaultdict(list)
    for c in calls:
        k = (c["workflow_id"], c["phone_hash"])
        lead_calls[k].append(c)

    return lead_calls, connected_cids, failed_cids, call_eff_disp


def _calculate_lead_bucket(
    workflow_id: str,
    calls_for_lead: list[Any],
    connected_cids: set[str],
    failed_cids: set[str],
    call_eff_disp: dict[str, Optional[str]],
    catalog: DispositionsCatalog,
) -> str:
    """
    Determine lead bucket in priority order:
    1. yet_to_reach_out: no calls.
    2. completed: any call's effective disposition is terminal (is_terminal=true).
    3. in_progress_telephony_failed: every call has a call.telephony_failed event.
    4. in_progress_dialed: everything else.
    """
    if not calls_for_lead:
        return "yet_to_reach_out"

    # Rule 2: any call's effective disposition is terminal
    for c in calls_for_lead:
        eff = call_eff_disp.get(c["call_id"])
        if eff and catalog.is_terminal(workflow_id, eff):
            return "completed"

    # Rule 3: every call has a telephony_failed event
    if all(c["call_id"] in failed_cids for c in calls_for_lead):
        return "in_progress_telephony_failed"

    # Rule 4: everything else
    return "in_progress_dialed"


def generate_batch_summary(
    db: DatabaseManager,
    catalog: DispositionsCatalog,
    out_path: Path | str,
) -> Path:
    """
    Generate batch_summary.csv sorted by workflow_id, batch_id.
    Header:
    workflow_id,batch_id,total_leads,yet_to_reach_out,in_progress_telephony_failed,in_progress_dialed,completed
    """
    out_file = Path(out_path)
    out_file.parent.mkdir(parents=True, exist_ok=True)

    leads = db.fetchall(
        "SELECT workflow_id, batch_id, phone_hash FROM leads ORDER BY workflow_id, batch_id;"
    )

    lead_calls, connected_cids, failed_cids, call_eff_disp = _fetch_call_aggregates(db, catalog)

    # Aggregate by (workflow_id, batch_id)
    summary: dict[tuple[str, str], Counter[str]] = defaultdict(Counter)

    for l in leads:
        wf = l["workflow_id"]
        batch = l["batch_id"]
        ph = l["phone_hash"]
        c_list = lead_calls.get((wf, ph), [])
        bucket = _calculate_lead_bucket(wf, c_list, connected_cids, failed_cids, call_eff_disp, catalog)

        summary[(wf, batch)]["total_leads"] += 1
        summary[(wf, batch)][bucket] += 1

    with open(out_file, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow([
            "workflow_id",
            "batch_id",
            "total_leads",
            "yet_to_reach_out",
            "in_progress_telephony_failed",
            "in_progress_dialed",
            "completed",
        ])
        for (wf, batch) in sorted(summary.keys()):
            counts = summary[(wf, batch)]
            writer.writerow([
                wf,
                batch,
                counts["total_leads"],
                counts["yet_to_reach_out"],
                counts["in_progress_telephony_failed"],
                counts["in_progress_dialed"],
                counts["completed"],
            ])

    return out_file


def generate_daily_overview(
    db: DatabaseManager,
    catalog: DispositionsCatalog,
    out_path: Path | str,
) -> Path:
    """
    Generate daily_overview.csv: one row per workflow per IST date that has at least one call.
    Sorted by workflow_id, date.
    Header:
    workflow_id,date,total_calls,connected_calls,connectivity_rate,success_leads
    """
    out_file = Path(out_path)
    out_file.parent.mkdir(parents=True, exist_ok=True)

    # 1. Fetch all calls linked to leads
    calls = db.fetchall(
        """
        SELECT c.call_id, c.workflow_id, c.phone_hash, c.occurred_at_ist, c.date_ist
        FROM interaction_ledger c
        JOIN leads l ON l.workflow_id = c.workflow_id AND l.phone_hash = c.phone_hash
        WHERE c.event_type = 'call.initiated'
        ORDER BY c.occurred_at_ist ASC;
        """
    )

    _, connected_cids, _, call_eff_disp = _fetch_call_aggregates(db, catalog)

    daily_data: dict[tuple[str, str], dict[str, Any]] = defaultdict(
        lambda: {"total_calls": 0, "connected_calls": 0, "success_leads": set()}
    )

    for c in calls:
        wf = c["workflow_id"]
        d = c["date_ist"]
        cid = c["call_id"]
        ph = c["phone_hash"]

        daily_data[(wf, d)]["total_calls"] += 1
        if cid in connected_cids:
            daily_data[(wf, d)]["connected_calls"] += 1

        eff = call_eff_disp.get(cid)
        if eff and catalog.is_success(wf, eff):
            daily_data[(wf, d)]["success_leads"].add(ph)

    with open(out_file, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow([
            "workflow_id",
            "date",
            "total_calls",
            "connected_calls",
            "connectivity_rate",
            "success_leads",
        ])
        for (wf, d) in sorted(daily_data.keys()):
            metrics = daily_data[(wf, d)]
            total = metrics["total_calls"]
            conn = metrics["connected_calls"]
            rate = conn / total if total > 0 else 0.0
            formatted_rate = f"{rate:.4f}"
            writer.writerow([
                wf,
                d,
                total,
                conn,
                formatted_rate,
                len(metrics["success_leads"]),
            ])

    return out_file


def generate_lead_report(
    db: DatabaseManager,
    catalog: DispositionsCatalog,
    out_path: Path | str,
) -> Path:
    """
    Generate lead_report.csv: one row per lead, sorted by workflow_id, external_ref.
    Header:
    workflow_id,external_ref,batch_id,phone_masked,bucket,total_calls,connected_calls,last_call_at,current_disposition,ever_success
    """
    out_file = Path(out_path)
    out_file.parent.mkdir(parents=True, exist_ok=True)

    leads = db.fetchall(
        """
        SELECT workflow_id, external_ref, batch_id, phone_masked, phone_hash
        FROM leads
        ORDER BY workflow_id ASC, external_ref ASC;
        """
    )

    lead_calls, connected_cids, failed_cids, call_eff_disp = _fetch_call_aggregates(db, catalog)

    with open(out_file, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow([
            "workflow_id",
            "external_ref",
            "batch_id",
            "phone_masked",
            "bucket",
            "total_calls",
            "connected_calls",
            "last_call_at",
            "current_disposition",
            "ever_success",
        ])

        for l in leads:
            wf = l["workflow_id"]
            ph = l["phone_hash"]
            c_list = lead_calls.get((wf, ph), [])
            # Sort calls chronologically by occurred_at_ist
            c_list.sort(key=lambda x: x["occurred_at_ist"])

            bucket = _calculate_lead_bucket(wf, c_list, connected_cids, failed_cids, call_eff_disp, catalog)
            total_calls = len(c_list)
            connected_calls = sum(1 for c in c_list if c["call_id"] in connected_cids)

            # last_call_at: latest call.initiated time in IST ISO-8601 (empty if 0 calls)
            last_call_at = c_list[-1]["occurred_at_ist"] if c_list else ""

            # current_disposition: effective disposition of most recent call (by initiated time) that has one
            current_disposition = ""
            for c in reversed(c_list):
                eff = call_eff_disp.get(c["call_id"])
                if eff:
                    current_disposition = eff
                    break

            # ever_success: whether any call's effective disposition has is_success=true
            ever_success = "true" if any(
                call_eff_disp.get(c["call_id"]) and catalog.is_success(wf, call_eff_disp[c["call_id"]])
                for c in c_list
            ) else "false"

            writer.writerow([
                wf,
                l["external_ref"],
                l["batch_id"],
                l["phone_masked"],
                bucket,
                total_calls,
                connected_calls,
                last_call_at,
                current_disposition,
                ever_success,
            ])

    return out_file


def generate_rejections_report(
    db: DatabaseManager,
    out_path: Path | str,
) -> Path:
    """
    Generate rejections.csv sorted by workflow_id, batch_id, row_number. Strictly NO PII.
    Header:
    workflow_id,batch_id,row_number,reason
    """
    out_file = Path(out_path)
    out_file.parent.mkdir(parents=True, exist_ok=True)

    rows = db.fetchall(
        """
        SELECT workflow_id, batch_id, row_number, reason
        FROM rejections
        ORDER BY workflow_id ASC, batch_id ASC, row_number ASC;
        """
    )

    with open(out_file, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["workflow_id", "batch_id", "row_number", "reason"])
        for r in rows:
            writer.writerow([
                r["workflow_id"],
                r["batch_id"],
                r["row_number"],
                r["reason"],
            ])

    return out_file


def generate_data_quality_report(
    db: DatabaseManager,
    out_path: Path | str,
) -> Path:
    """
    Generate data_quality.json containing exact required quality metrics.
    """
    out_file = Path(out_path)
    out_file.parent.mkdir(parents=True, exist_ok=True)

    rows = db.fetchall(
        """
        SELECT 
            lead_rows_total,
            lead_rows_rejected,
            rejections_by_reason_json,
            duplicate_lead_rows_merged,
            field_warnings,
            events_total,
            malformed_events,
            duplicate_events_dropped,
            unknown_disposition_events
        FROM data_quality;
        """
    )

    lead_rows_total = sum(r["lead_rows_total"] for r in rows)
    lead_rows_rejected = sum(r["lead_rows_rejected"] for r in rows)
    duplicate_lead_rows_merged = sum(r["duplicate_lead_rows_merged"] for r in rows)
    field_warnings = sum(r["field_warnings"] for r in rows)
    events_total = sum(r["events_total"] for r in rows)
    malformed_events = sum(r["malformed_events"] for r in rows)
    duplicate_events_dropped = sum(r["duplicate_events_dropped"] for r in rows)
    unknown_disposition_events = sum(r["unknown_disposition_events"] for r in rows)

    # Compute rejections_by_reason directly from rejections table for guaranteed consistency
    rej_rows = db.fetchall("SELECT reason FROM rejections;")
    rejections_by_reason = dict(sorted(Counter(r["reason"] for r in rej_rows).items()))

    # Calculate live orphan events
    orphan_events = count_orphan_events(db)

    metrics = {
        "lead_rows_total": lead_rows_total,
        "lead_rows_rejected": lead_rows_rejected,
        "rejections_by_reason": rejections_by_reason,
        "duplicate_lead_rows_merged": duplicate_lead_rows_merged,
        "field_warnings": field_warnings,
        "events_total": events_total,
        "malformed_events": malformed_events,
        "duplicate_events_dropped": duplicate_events_dropped,
        "orphan_events": orphan_events,
        "unknown_disposition_events": unknown_disposition_events,
    }

    with open(out_file, "w", encoding="utf-8") as f:
        json.dump(metrics, f, indent=2)

    return out_file


def generate_all_reports(
    db: DatabaseManager,
    catalog: Optional[DispositionsCatalog] = None,
    out_dir: Path | str = "reports",
) -> dict[str, Path]:
    """
    Generate all five required reports into out_dir.
    Returns mapping of report filename to Path.
    """
    if catalog is None:
        catalog = DispositionsCatalog()

    out_path = Path(out_dir)
    out_path.mkdir(parents=True, exist_ok=True)

    generated = {
        "batch_summary.csv": generate_batch_summary(db, catalog, out_path / "batch_summary.csv"),
        "daily_overview.csv": generate_daily_overview(db, catalog, out_path / "daily_overview.csv"),
        "lead_report.csv": generate_lead_report(db, catalog, out_path / "lead_report.csv"),
        "rejections.csv": generate_rejections_report(db, out_path / "rejections.csv"),
        "data_quality.json": generate_data_quality_report(db, out_path / "data_quality.json"),
    }
    return generated


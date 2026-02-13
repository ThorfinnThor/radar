from __future__ import annotations

import csv, json
from pathlib import Path
from typing import Any, Dict, List
from radar.roles import DEFAULT_ROLE_TITLES

def summarize_triggers(signals: List[Dict[str, Any]], max_items: int = 3) -> str:
    parts = []
    for s in signals[:max_items]:
        st = s.get("signal_type")
        title = (s.get("title") or "").strip()
        if title:
            parts.append(f"{st}: {title[:110]}")
        else:
            parts.append(f"{st}")
    return " | ".join(parts)

def export_ranked(rows: List[Dict[str, Any]], out_csv: Path, out_json: Path, top_n: int | None = None) -> None:
    out_csv = Path(out_csv)
    out_json = Path(out_json)
    out_csv.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = [
        "rank","account_name","fit","urgency","total","fit_reason","urgency_reason","urgency_source",
        "trigger_summary",
        "best_fit_trial_title","best_fit_trial_status","best_fit_trial_phase","best_fit_trial_url",
        "best_urgency_trial_title","best_urgency_trial_status","best_urgency_trial_phase","best_urgency_trial_url",
        "sec_matched_total","patent_matched_total",
        "target_roles",
    ]

    iter_rows = (rows[:top_n] if top_n else rows)

    # CSV
    with out_csv.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        for i, r in enumerate(iter_rows, start=1):
            w.writerow({
                "rank": i,
                "account_name": r.get("account_name"),
                "fit": r.get("fit"),
                "urgency": r.get("urgency"),
                "total": r.get("total"),
                "fit_reason": r.get("fit_reason"),
                "urgency_reason": r.get("urgency_reason"),
                "urgency_source": r.get("urgency_source"),
                "trigger_summary": r.get("trigger_summary"),
                "best_fit_trial_title": r.get("best_fit_trial_title"),
                "best_fit_trial_status": r.get("best_fit_trial_status"),
                "best_fit_trial_phase": r.get("best_fit_trial_phase"),
                "best_fit_trial_url": r.get("best_fit_trial_url"),
                "best_urgency_trial_title": r.get("best_urgency_trial_title"),
                "best_urgency_trial_status": r.get("best_urgency_trial_status"),
                "best_urgency_trial_phase": r.get("best_urgency_trial_phase"),
                "best_urgency_trial_url": r.get("best_urgency_trial_url"),
                "sec_matched_total": (r.get("sec") or {}).get("matched_total"),
                "patent_matched_total": (r.get("patents") or {}).get("matched_total"),
                "target_roles": json.dumps(r.get("target_roles") or DEFAULT_ROLE_TITLES),
            })

    # JSON (mirror the same top_n subset as CSV, and include ranks).
    json_rows: List[Dict[str, Any]] = []
    for i, r in enumerate(iter_rows, start=1):
        rr = dict(r)
        rr["rank"] = i
        json_rows.append(rr)
    out_json.write_text(json.dumps(json_rows, indent=2), encoding="utf-8")

def export_watchlist(rows: List[Dict[str, Any]], out_csv: Path, out_json: Path, top_n: int | None = None) -> None:
    out_csv = Path(out_csv)
    out_json = Path(out_json)
    out_csv.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = [
        "account_name","fit","urgency","total","fit_reason","urgency_reason","urgency_source",
        "trigger_summary",
        "sec_matched_total","sec_examples",
        "patent_matched_total","patent_examples",
        "target_roles",
    ]
    with out_csv.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        for r in (rows[:top_n] if top_n else rows):
            w.writerow({
                "account_name": r.get("account_name"),
                "fit": r.get("fit"),
                "urgency": r.get("urgency"),
                "total": r.get("total"),
                "fit_reason": r.get("fit_reason"),
                "urgency_reason": r.get("urgency_reason"),
                "urgency_source": r.get("urgency_source"),
                "trigger_summary": r.get("trigger_summary"),
                "sec_matched_total": (r.get("sec") or {}).get("matched_total"),
                "sec_examples": json.dumps((r.get("sec") or {}).get("examples")),
                "patent_matched_total": (r.get("patents") or {}).get("matched_total"),
                "patent_examples": json.dumps((r.get("patents") or {}).get("examples")),
                "target_roles": json.dumps(r.get("target_roles") or DEFAULT_ROLE_TITLES),
            })
    out_json.write_text(json.dumps(rows, indent=2), encoding="utf-8")

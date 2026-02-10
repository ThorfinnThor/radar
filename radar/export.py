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
        parts.append(f"{st}: {title}" if title else f"{st}")
    return " | ".join(parts)

def export_ranked(rows: List[Dict[str, Any]], out_csv: str, out_json: str, top_n: int = 40) -> None:
    Path(out_csv).parent.mkdir(parents=True, exist_ok=True)
    Path(out_json).parent.mkdir(parents=True, exist_ok=True)
    top = rows[:top_n]

    with open(out_csv, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=[
            "rank","company","total_score","fit_score","urgency_score","access_score",
            "trigger_summary","evidence_links","target_roles"
        ])
        w.writeheader()
        for i, r in enumerate(top, start=1):
            w.writerow({
                "rank": i,
                "company": r["company"],
                "total_score": f"{r['total_score']:.2f}",
                "fit_score": r["fit_score"],
                "urgency_score": r["urgency_score"],
                "access_score": r["access_score"],
                "trigger_summary": r["trigger_summary"],
                "evidence_links": " ; ".join(r["evidence_links"]),
                "target_roles": " | ".join(DEFAULT_ROLE_TITLES),
            })

    with open(out_json, "w", encoding="utf-8") as f:
        json.dump({"generated_rows": top, "target_roles": DEFAULT_ROLE_TITLES}, f, indent=2)

def export_watchlist(rows: List[Dict[str, Any]], out_csv: str, out_json: str) -> None:
    Path(out_csv).parent.mkdir(parents=True, exist_ok=True)
    Path(out_json).parent.mkdir(parents=True, exist_ok=True)

    with open(out_csv, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=[
            "company","total_score","fit_score","urgency_score","access_score",
            "trigger_summary","evidence_links","target_roles"
        ])
        w.writeheader()
        for r in rows:
            w.writerow({
                "company": r["company"],
                "total_score": f"{r['total_score']:.2f}",
                "fit_score": r["fit_score"],
                "urgency_score": r["urgency_score"],
                "access_score": r["access_score"],
                "trigger_summary": r["trigger_summary"],
                "evidence_links": " ; ".join(r["evidence_links"]),
                "target_roles": " | ".join(DEFAULT_ROLE_TITLES),
            })

    with open(out_json, "w", encoding="utf-8") as f:
        json.dump({"watchlist_rows": rows, "target_roles": DEFAULT_ROLE_TITLES}, f, indent=2)

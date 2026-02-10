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
            "trial_count","best_fit_trial_title","best_fit_trial_status","best_fit_trial_phases","best_fit_trial_last_update",
            "recent_job_hits","fit_reason","urgency_reason","urgency_source","access_reason","bonus_recent_trial","bonus_multi_trial",
            "trigger_summary","evidence_links","target_roles","job_hit_keywords","job_hit_titles"
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
                "trial_count": r.get("trial_count"),
                "best_fit_trial_title": r.get("best_fit_trial_title"),
                "best_fit_trial_status": r.get("best_fit_trial_status"),
                "best_fit_trial_phases": json.dumps(r.get("best_fit_trial_phases")) if r.get("best_fit_trial_phases") is not None else None,
                "best_fit_trial_last_update": r.get("best_fit_trial_last_update"),
                "recent_job_hits": r.get("recent_job_hits"),
                "fit_reason": r.get("fit_reason"),
                "urgency_reason": r.get("urgency_reason"),
                "urgency_source": r.get("urgency_source"),
                "access_reason": r.get("access_reason"),
                "bonus_recent_trial": r.get("bonus_recent_trial"),
                "bonus_multi_trial": r.get("bonus_multi_trial"),
                "trigger_summary": r["trigger_summary"],
                "evidence_links": " ; ".join(r["evidence_links"]),
                "target_roles": " | ".join(r.get("target_roles") or DEFAULT_ROLE_TITLES),
                "job_hit_keywords": json.dumps(r.get("job_hit_keywords")) if r.get("job_hit_keywords") is not None else None,
                "job_hit_titles": json.dumps(r.get("job_hit_titles")) if r.get("job_hit_titles") is not None else None,
            })

    with open(out_json, "w", encoding="utf-8") as f:
        json.dump({"generated_rows": top, "target_roles": DEFAULT_ROLE_TITLES}, f, indent=2)

def export_watchlist(rows: List[Dict[str, Any]], out_csv: str, out_json: str) -> None:
    Path(out_csv).parent.mkdir(parents=True, exist_ok=True)
    Path(out_json).parent.mkdir(parents=True, exist_ok=True)

    with open(out_csv, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=[
            "company","total_score","fit_score","urgency_score","access_score",
            "trial_count","best_fit_trial_title","best_fit_trial_status","best_fit_trial_phases","best_fit_trial_last_update",
            "recent_job_hits","fit_reason","urgency_reason","urgency_source","access_reason","bonus_recent_trial","bonus_multi_trial",
            "trigger_summary","evidence_links","target_roles","job_hit_keywords","job_hit_titles"
        ])
        w.writeheader()
        for r in rows:
            w.writerow({
                "company": r["company"],
                "total_score": f"{r['total_score']:.2f}",
                "fit_score": r["fit_score"],
                "urgency_score": r["urgency_score"],
                "access_score": r["access_score"],
                "trial_count": r.get("trial_count"),
                "best_fit_trial_title": r.get("best_fit_trial_title"),
                "best_fit_trial_status": r.get("best_fit_trial_status"),
                "best_fit_trial_phases": json.dumps(r.get("best_fit_trial_phases")) if r.get("best_fit_trial_phases") is not None else None,
                "best_fit_trial_last_update": r.get("best_fit_trial_last_update"),
                "recent_job_hits": r.get("recent_job_hits"),
                "fit_reason": r.get("fit_reason"),
                "urgency_reason": r.get("urgency_reason"),
                "urgency_source": r.get("urgency_source"),
                "access_reason": r.get("access_reason"),
                "bonus_recent_trial": r.get("bonus_recent_trial"),
                "bonus_multi_trial": r.get("bonus_multi_trial"),
                "trigger_summary": r["trigger_summary"],
                "evidence_links": " ; ".join(r["evidence_links"]),
                "target_roles": " | ".join(r.get("target_roles") or DEFAULT_ROLE_TITLES),
                "job_hit_keywords": json.dumps(r.get("job_hit_keywords")) if r.get("job_hit_keywords") is not None else None,
                "job_hit_titles": json.dumps(r.get("job_hit_titles")) if r.get("job_hit_titles") is not None else None,
            })

    with open(out_json, "w", encoding="utf-8") as f:
        json.dump({"watchlist_rows": rows, "target_roles": DEFAULT_ROLE_TITLES}, f, indent=2)

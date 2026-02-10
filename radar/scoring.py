from __future__ import annotations
from dataclasses import dataclass
from typing import Iterable, List, Dict, Any
import datetime as dt
from dateutil import parser as dateparser

CAR_T_TERMS = ["car-t", "car t", "chimeric antigen receptor", "tcr-t", "tcr t"]
TCE_TERMS   = ["t-cell engager", "t cell engager", "cd3 bispecific", "cd3", "bispecific"]

HIGH_URGENCY_STATUSES = {"RECRUITING", "NOT_YET_RECRUITING", "ACTIVE_NOT_RECRUITING"}

def _lower(x: str | None) -> str:
    return (x or "").lower()

def parse_date_maybe(s: str | None) -> dt.datetime | None:
    if not s:
        return None
    try:
        return dateparser.parse(s)
    except Exception:
        return None

def within_days(published_at: str | None, window_days: int) -> bool:
    d = parse_date_maybe(published_at)
    if not d:
        return False
    if not d.tzinfo:
        d = d.replace(tzinfo=dt.timezone.utc)
    now = dt.datetime.now(dt.timezone.utc)
    return (now - d) <= dt.timedelta(days=window_days)

def fit_from_trial_title(title: str | None) -> int:
    t = _lower(title)
    # stronger signals
    if any(term in t for term in ["car-t", "tcr-t", "chimeric antigen receptor"]):
        return 3
    if "t-cell engager" in t or "t cell engager" in t:
        return 3
    # bispecific is noisy; keep moderate unless CD3 is present
    if "bispecific" in t and "cd3" in t:
        return 3
    if "bispecific" in t or "cd3" in t:
        return 2
    return 1

def urgency_from_trial(overall_status: str | None, phases: list[str] | None, high_urgency_phases: list[str]) -> int:
    status = (overall_status or "").upper()
    phases_u = {p.upper() for p in (phases or [])}
    high_u = {p.upper() for p in (high_urgency_phases or [])}
    if status in HIGH_URGENCY_STATUSES and (phases_u & high_u):
        return 3
    if status in HIGH_URGENCY_STATUSES:
        return 2
    return 1

def urgency_from_jobs(relevant_recent_jobs: int, spike_threshold: int) -> int:
    if relevant_recent_jobs >= max(spike_threshold + 1, 3):
        return 3
    if relevant_recent_jobs >= spike_threshold:
        return 2
    if relevant_recent_jobs == 1:
        return 1
    return 0

def access_score(ats_known: bool, ats_known_points: int, default_points: int) -> int:
    return ats_known_points if ats_known else default_points

def compute_scores(
    account: Dict[str, Any],
    trials: List[Dict[str, Any]],
    job_signals: List[Dict[str, Any]],
    config: Dict[str, Any],
    company_in_watchlist: bool,
) -> Dict[str, float]:
    # Fit: best trial fit score
    fit = 0
    for t in trials[:10]:
        fit = max(fit, fit_from_trial_title(t.get("brief_title")))
    if fit == 0:
        fit = 1

    # Urgency: max(trial urgency, job urgency)
    high_urgency_phases = config.get("ctg", {}).get("high_urgency_phases", [])
    urg_trial = 0
    for t in trials[:10]:
        urg_trial = max(urg_trial, urgency_from_trial(t.get("overall_status"), t.get("phases"), high_urgency_phases))
    job_window = int(config.get("jobs", {}).get("recent_window_days", 45))
    spike_threshold = int(config.get("jobs", {}).get("spike_threshold", 2))
    relevant_recent_jobs = sum(1 for j in job_signals if within_days(j.get("published_at"), job_window))
    urg_jobs = urgency_from_jobs(relevant_recent_jobs, spike_threshold)
    urgency = float(max(urg_trial, urg_jobs))

    # Access: whether ATS info is known (company watchlist)
    acc_cfg = config.get("scoring", {}).get("access", {})
    access = float(access_score(
        company_in_watchlist,
        int(acc_cfg.get("ats_known_points", 2)),
        int(acc_cfg.get("default_points", 1)),
    ))

    # Weighted total
    w = config.get("scoring", {}).get("weights", {})
    total = float(fit) * float(w.get("fit", 1.0)) + float(urgency) * float(w.get("urgency", 1.0)) + float(access) * float(w.get("access", 1.0))

    return {
        "fit": float(fit),
        "urgency": float(urgency),
        "access": float(access),
        "total": float(total),
    }

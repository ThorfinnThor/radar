from __future__ import annotations
from typing import Any, Dict, List, Tuple
import datetime as dt
from dateutil import parser as dateparser

HIGH_URGENCY_STATUSES = {"RECRUITING", "NOT_YET_RECRUITING", "ACTIVE_NOT_RECRUITING"}

def parse_date_maybe(s: str | None) -> dt.datetime | None:
    if not s:
        return None
    try:
        d = dateparser.parse(s)
        if d and not d.tzinfo:
            d = d.replace(tzinfo=dt.timezone.utc)
        return d
    except Exception:
        return None

def within_days(published_at: str | None, window_days: int) -> bool:
    d = parse_date_maybe(published_at)
    if not d:
        return False
    now = dt.datetime.now(dt.timezone.utc)
    return (now - d) <= dt.timedelta(days=window_days)

def _lower(x: str | None) -> str:
    return (x or "").lower()

def fit_from_trial_title(title: str | None) -> Tuple[int, str]:
    t = _lower(title)
    if any(term in t for term in ["car-t", "tcr-t", "chimeric antigen receptor"]):
        return 3, "trial title contains CAR-T/TCR-T/chimeric antigen receptor"
    if "t-cell engager" in t or "t cell engager" in t:
        return 3, "trial title contains T-cell engager"
    if "bispecific" in t and "cd3" in t:
        return 3, "trial title contains bispecific + CD3"
    if "cd3" in t:
        return 2, "trial title contains CD3"
    if "bispecific" in t:
        return 2, "trial title contains bispecific"
    return 1, "no strong wedge keywords in trial titles"

def trial_urgency(overall_status: str | None, phases: list[str] | None, high_urgency_phases: list[str]) -> Tuple[int, str]:
    status = (overall_status or "").upper()
    phases_u = {p.upper() for p in (phases or [])}
    high_u = {p.upper() for p in (high_urgency_phases or [])}
    if status in HIGH_URGENCY_STATUSES and (phases_u & high_u):
        return 3, f"trial status {status} and phase in {sorted(phases_u & high_u)}"
    if status in HIGH_URGENCY_STATUSES:
        return 2, f"trial status {status}"
    return 1, f"trial status {status or 'UNKNOWN'}"

def job_urgency(relevant_recent_jobs: int, spike_threshold: int) -> Tuple[int, str]:
    if relevant_recent_jobs >= max(spike_threshold + 1, 3):
        return 3, f"{relevant_recent_jobs} relevant jobs in window (>=3)"
    if relevant_recent_jobs >= spike_threshold:
        return 2, f"{relevant_recent_jobs} relevant jobs in window (>=threshold {spike_threshold})"
    if relevant_recent_jobs == 1:
        return 1, "1 relevant job in window"
    return 0, "no relevant jobs in window"

def access_score(ats_known: bool, ats_known_points: int, default_points: int) -> Tuple[int, str]:
    if ats_known:
        return ats_known_points, "company on watchlist / ATS known"
    return default_points, "not on watchlist"

def compute_scores(
    trials: List[Dict[str, Any]],
    job_signals: List[Dict[str, Any]],
    config: Dict[str, Any],
    company_in_watchlist: bool,
) -> Dict[str, Any]:
    # ----- Fit -----
    best_fit = 1
    fit_reason = "no trials"
    best_fit_trial = None
    for t in trials[:50]:
        s, reason = fit_from_trial_title(t.get("brief_title"))
        if s > best_fit:
            best_fit = s
            fit_reason = reason
            best_fit_trial = t

    # ----- Urgency (trial vs jobs) -----
    high_urgency_phases = config.get("ctg", {}).get("high_urgency_phases", [])
    best_trial_urg = 0
    trial_urg_reason = "no trials"
    best_urg_trial = None
    for t in trials[:50]:
        s, reason = trial_urgency(t.get("overall_status"), t.get("phases"), high_urgency_phases)
        if s > best_trial_urg:
            best_trial_urg = s
            trial_urg_reason = reason
            best_urg_trial = t

    job_window = int(config.get("jobs", {}).get("recent_window_days", 45))
    spike_threshold = int(config.get("jobs", {}).get("spike_threshold", 2))
    relevant_recent_jobs = sum(1 for j in job_signals if within_days(j.get("published_at"), job_window))
    job_urg, job_urg_reason = job_urgency(relevant_recent_jobs, spike_threshold)

    if best_trial_urg >= job_urg:
        urgency = float(best_trial_urg)
        urgency_reason = trial_urg_reason
        urgency_source = "trial"
        best_urg_driver = best_urg_trial
    else:
        urgency = float(job_urg)
        urgency_reason = job_urg_reason
        urgency_source = "jobs"
        best_urg_driver = None

    # ----- Access -----
    acc_cfg = config.get("scoring", {}).get("access", {})
    access_points, access_reason = access_score(
        company_in_watchlist,
        int(acc_cfg.get("ats_known_points", 2)),
        int(acc_cfg.get("default_points", 1)),
    )
    access = float(access_points)

    # ----- Weighted total -----
    w = config.get("scoring", {}).get("weights", {})
    total = float(best_fit) * float(w.get("fit", 1.0)) + float(urgency) * float(w.get("urgency", 1.0)) + float(access) * float(w.get("access", 1.0))

    # ----- Tiebreakers -----
    tb = config.get("scoring", {}).get("tiebreakers", {})
    recent_days = int(tb.get("recent_trial_update_days", 90))
    recent_bonus = float(tb.get("recent_trial_bonus", 0.5))
    extra_per = float(tb.get("extra_trial_bonus_per_trial", 0.15))
    extra_cap = float(tb.get("extra_trial_bonus_cap", 0.6))

    any_recent = any(within_days(t.get("last_update_posted"), recent_days) for t in trials[:50])
    bonus_recent = recent_bonus if any_recent else 0.0
    bonus_multi = min(extra_cap, max(0.0, (len(trials) - 1) * extra_per))
    total += bonus_recent + bonus_multi

    details = {
        "trial_count": len(trials),
        "best_fit_trial": {
            "nct_id": (best_fit_trial or {}).get("nct_id"),
            "brief_title": (best_fit_trial or {}).get("brief_title"),
            "overall_status": (best_fit_trial or {}).get("overall_status"),
            "phases": (best_fit_trial or {}).get("phases"),
            "last_update_posted": (best_fit_trial or {}).get("last_update_posted"),
        } if best_fit_trial else None,
        "best_urgency_trial": {
            "nct_id": (best_urg_trial or {}).get("nct_id"),
            "brief_title": (best_urg_trial or {}).get("brief_title"),
            "overall_status": (best_urg_trial or {}).get("overall_status"),
            "phases": (best_urg_trial or {}).get("phases"),
            "last_update_posted": (best_urg_trial or {}).get("last_update_posted"),
        } if best_urg_trial else None,
        "jobs": {
            "recent_window_days": job_window,
            "spike_threshold": spike_threshold,
            "relevant_recent_jobs": relevant_recent_jobs,
        },
        "reasons": {
            "fit_reason": fit_reason,
            "urgency_reason": urgency_reason,
            "urgency_source": urgency_source,
            "access_reason": access_reason,
        },
        "bonuses": {
            "recent_trial_bonus": bonus_recent,
            "multi_trial_bonus": bonus_multi,
            "any_recent_trial_update": any_recent,
        },
        "weights": {
            "fit": float(w.get("fit", 1.0)),
            "urgency": float(w.get("urgency", 1.0)),
            "access": float(w.get("access", 1.0)),
        },
    }

    return {"fit": float(best_fit), "urgency": float(urgency), "access": float(access), "total": float(total), "details": details}

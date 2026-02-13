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

def within_days(date_str: str | None, window_days: int) -> bool:
    d = parse_date_maybe(date_str)
    if not d:
        return False
    now = dt.datetime.now(dt.timezone.utc)
    return (now - d).days <= window_days

def _match_keywords(text: str, keywords: List[str]) -> List[str]:
    t = (text or "").lower()
    hits = []
    for k in keywords:
        kk = (k or "").lower()
        if kk and kk in t:
            hits.append(k)
    return hits

def fit_from_trial_title(title: str | None, tcell_engager_molecules: List[str]) -> Tuple[int, str]:
    if not title:
        return 1, "missing title"
    t = title.lower()
    if "car-t" in t or "car t" in t or "chimeric antigen receptor" in t:
        return 5, "CAR-T trial"
    if "t-cell engager" in t or "t cell engager" in t or "cd3" in t or "bispecific" in t:
        return 4, "T cell engager / CD3 / bispecific trial"
    for m in tcell_engager_molecules:
        if m.lower() in t:
            return 4, f"molecule match: {m}"
    if "cell therapy" in t or "tcr" in t:
        return 3, "cell therapy / TCR trial"
    return 2, "immuno-oncology trial"

def trial_urgency(status: str | None, phases: List[str] | None, high_urgency_phases: List[str]) -> Tuple[int, str]:
    st = (status or "").upper().strip()
    ph = [p.upper().strip() for p in (phases or []) if p]
    if st in HIGH_URGENCY_STATUSES:
        if any(p in high_urgency_phases for p in ph):
            return 3, f"{st} in high urgency phase ({','.join(ph)})"
        return 2, f"{st}"
    if st:
        return 1, st
    return 0, "unknown status"

def other_signal_stats(signals: List[Dict[str, Any]], keywords: List[str], window_days: int, max_examples: int = 3) -> Dict[str, Any]:
    recent_total = 0
    matched_total = 0
    matched_keywords: set[str] = set()
    examples: List[Dict[str, Any]] = []

    for s in signals:
        if not within_days(s.get("published_at"), window_days):
            continue
        recent_total += 1
        blob = ""
        pj = s.get("payload_json")
        if pj:
            try:
                import json as _json
                blob = (_json.loads(pj) or {}).get("text_blob") or ""
            except Exception:
                blob = ""
        if not blob:
            blob = s.get("title") or ""
        hits = _match_keywords(blob, keywords)
        if not hits:
            continue
        matched_total += 1
        for h in hits:
            matched_keywords.add(h)
        if len(examples) < max_examples:
            examples.append({
                "signal_type": s.get("signal_type"),
                "title": s.get("title"),
                "evidence_url": s.get("evidence_url"),
                "published_at": s.get("published_at"),
                "matched_keywords": hits,
            })

    access = 0.0

    return {
        "recent_total": recent_total,
        "matched_total": matched_total,
        "matched_keywords": sorted(matched_keywords),
        "examples": examples,
    }

def other_urgency(matched_total: int) -> Tuple[int, str]:
    if matched_total >= 3:
        return 2, f"{matched_total} relevant signals in window (>=3)"
    if matched_total == 2:
        return 2, "2 relevant signals in window"
    if matched_total == 1:
        return 1, "1 relevant signal in window"
    return 0, "no relevant signals in window"

def compute_scores(
    trials: List[Dict[str, Any]],
    sec_signals: List[Dict[str, Any]],
    patent_signals: List[Dict[str, Any]],
    config: Dict[str, Any],
    company_in_watchlist: bool,
) -> Dict[str, Any]:
    # ----- Fit -----
    best_fit = 0
    fit_reason = "no trials"
    best_fit_trial = None
    for t in trials[:50]:
        mols = (config.get("ctg", {}) or {}).get("tcell_engager_molecules", [])
        s, reason = fit_from_trial_title(t.get("brief_title"), mols)
        if s > best_fit:
            best_fit = s
            fit_reason = reason
            best_fit_trial = t
        if best_fit_trial is None:
            best_fit_trial = t
            best_fit = s
            fit_reason = reason
    if not trials:
        best_fit = 1

    # If no trials, let SEC/patents contribute to fit modestly (so non-trial companies can rank)
    other_kw = (config.get("sec", {}) or {}).get("keywords", []) or (config.get("patents", {}) or {}).get("keywords", [])
    sec_stat = other_signal_stats(sec_signals, other_kw, window_days=int((config.get("sec", {}) or {}).get("recent_window_days", 90)))
    pat_stat = other_signal_stats(patent_signals, (config.get("patents", {}) or {}).get("keywords", []), window_days=int((config.get("patents", {}) or {}).get("recent_window_days", 365)))
    other_matched = int(sec_stat["matched_total"]) + int(pat_stat["matched_total"])
    if not trials and other_matched > 0:
        best_fit = max(best_fit, 2)
        fit_reason = "non-trial modality signals (SEC/patents)"

    # ----- Urgency -----
    high_urgency_phases = config.get("ctg", {}).get("high_urgency_phases", [])
    best_trial_urg = -1
    trial_urg_reason = "no trials"
    best_urg_trial = None
    for t in trials[:50]:
        s, reason = trial_urgency(t.get("overall_status"), t.get("phases"), high_urgency_phases)
        if s > best_trial_urg:
            best_trial_urg = s
            trial_urg_reason = reason
            best_urg_trial = t
    if best_trial_urg < 0:
        best_trial_urg = 0
        trial_urg_reason = "no trials"

    other_urg_score, other_urg_reason = other_urgency(other_matched)

    if trials and best_trial_urg >= other_urg_score:
        urgency = float(best_trial_urg)
        urgency_reason = trial_urg_reason
        urgency_source = "clinicaltrials"
    elif other_urg_score > 0:
        urgency = float(other_urg_score)
        urgency_reason = other_urg_reason
        urgency_source = "sec/patents"
    else:
        urgency = 0.0
        urgency_reason = "no trials and no relevant sec/patent signals"
        urgency_source = "none"

    # ----- Access -----
    # Job scraping was removed, so access is currently a lightweight, configurable constant.
    # This keeps the schema stable and leaves room for adding an outreach/relationship signal later.
    access_cfg = (config.get("scoring", {}) or {}).get("access", {}) or {}
    default_points = float(access_cfg.get("default_points", 0.0))
    access = default_points

    # ----- Total (weighted) -----
    scoring_cfg = config.get("scoring", {}) or {}
    weights = scoring_cfg.get("weights", {}) or {}
    w_fit = float(weights.get("fit", 1.0))
    w_urg = float(weights.get("urgency", 1.0))
    w_acc = float(weights.get("access", 1.0))

    watchlist_bonus = float(scoring_cfg.get("watchlist_bonus", 2.0 if company_in_watchlist else 0.0))
    wl_bonus_applied = watchlist_bonus if company_in_watchlist else 0.0

    total = (best_fit * w_fit) + (urgency * w_urg) + (access * w_acc) + wl_bonus_applied

    # Optional small tiebreakers (kept in config so you can tune without code changes)
    tb = scoring_cfg.get("tiebreakers", {}) or {}
    recent_days = int(tb.get("recent_trial_update_days", 0) or 0)
    recent_bonus = float(tb.get("recent_trial_bonus", 0.0) or 0.0)
    extra_per_trial = float(tb.get("extra_trial_bonus_per_trial", 0.0) or 0.0)
    extra_cap = float(tb.get("extra_trial_bonus_cap", 0.0) or 0.0)

    if recent_days > 0 and recent_bonus > 0.0:
        if any(within_days(t.get("last_update_posted"), recent_days) for t in trials[:50]):
            total += recent_bonus

    if extra_per_trial > 0.0 and extra_cap > 0.0 and trials:
        total += min(extra_cap, extra_per_trial * float(len(trials)))

    return {
        "fit": float(best_fit),
        "access": float(access),
        "urgency": float(urgency),
        "total": float(total),
        "fit_reason": fit_reason,
        "urgency_reason": urgency_reason,
        "urgency_source": urgency_source,
        "best_fit_trial": best_fit_trial,
        "best_urgency_trial": best_urg_trial,
        "details": {"trial_count": len(trials), "sec_matched": int(sec_stat["matched_total"]), "patent_matched": int(pat_stat["matched_total"])},
        "sec": sec_stat,
        "patents": pat_stat,
    }

from __future__ import annotations

import argparse
import json
import os
from typing import Any, Dict, List, Tuple

from radar.config import AppConfig
from radar import db
from radar.collectors import clinicaltrials as ctg
from radar.collectors import patentsview, sec_edgar
from radar.export import export_ranked, export_watchlist, summarize_triggers
from radar.role_recommender import recommend_roles
from radar.scoring import compute_scores


def normalize_account_name(name: str, aliases: Dict[str, str]) -> str:
    """Normalize company names using explicit alias mapping."""
    n = (name or "").strip()
    if not n:
        return "UNKNOWN"
    return aliases.get(n, n)


def ingest_trials(conn, cfg: AppConfig) -> Tuple[int, int, int]:
    """Ingest ClinicalTrials.gov studies as trial signals + study snapshots.

    Returns:
        (lead_sponsor_signals, collaborator_signals, collaborator_synthetic_studies)
    """
    aliases = cfg.aliases()
    keep_statuses = {s.upper() for s in cfg.ctg_keep_statuses()}
    keep_classes = {s.upper() for s in cfg.ctg_keep_sponsor_classes()}
    include_collabs = cfg.ctg_include_industry_collaborators()
    allow_other_lead_if_industry_collab = bool(
        cfg.config.get("ctg", {}).get("allow_non_industry_lead_with_industry_collab", False)
    )

    lead_ingested = 0
    collab_ingested = 0
    collab_attributed = 0

    for q in cfg.ctg_queries():
        ctg_cfg = cfg.config.get("ctg", {}) or {}
        studies = ctg.fetch_studies(
            cfg.ctg_base_url(),
            q,
            page_size=cfg.ctg_page_size(),
            max_pages=int(ctg_cfg.get("max_pages", 50)),
            max_studies=int(ctg_cfg.get("max_studies", 10000)),
        )
        for st in studies:
            nct_id, sig, blob = ctg.normalize_study(st)
            lead_name = normalize_account_name(sig.account_name, aliases)

            status = (blob.get("overall_status") or "").upper()
            if keep_statuses and status and status not in keep_statuses:
                continue

            lead_class = (blob.get("sponsor_class") or "").upper()
            lead_allowed = (not keep_classes) or (lead_class in keep_classes)
            collaborators = sig.payload.get("collaborators") or []
            has_industry_collab = any(((c.get("class") or "").upper() == "INDUSTRY") for c in collaborators)

            # If lead sponsor is not allowed (e.g., not INDUSTRY), optionally attribute to INDUSTRY collaborators.
            if (not lead_allowed) and (allow_other_lead_if_industry_collab and has_industry_collab):
                if include_collabs:
                    for c in collaborators:
                        cname = (c.get("name") or "").strip()
                        cclass = (c.get("class") or "").upper()
                        if not cname or cclass != "INDUSTRY":
                            continue
                        collab_name = normalize_account_name(cname, aliases)
                        collab_id = db.upsert_account(conn, collab_name, modality_tags=["car-t", "t-cell engager"])

                        db.insert_signal(
                            conn,
                            collab_id,
                            "trial_collaborator",
                            sig.source,
                            sig.title,
                            sig.evidence_url,
                            sig.published_at,
                            {"nct_id": nct_id, "lead_sponsor": lead_name, "lead_sponsor_class": lead_class},
                        )
                        collab_ingested += 1

                        # Store a synthetic study row keyed by (nct_id + collaborator) so collaborator accounts receive
                        # trial-based scoring without colliding with studies.nct_id PK.
                        if nct_id:
                            synth_id = f"{nct_id}::collab::{collab_name}".replace(" ", "_")[:240]
                            db.upsert_study(
                                conn,
                                synth_id,
                                collab_id,
                                brief_title=blob.get("brief_title") or "",
                                overall_status=blob.get("overall_status") or "",
                                phases=blob.get("phases") or [],
                                last_update_posted=blob.get("last_update_posted"),
                                sponsor_class="INDUSTRY_COLLAB",
                                study_url=blob.get("study_url"),
                                raw={
                                    "original_nct_id": nct_id,
                                    "lead_sponsor": lead_name,
                                    "raw": (blob.get("raw") or {}),
                                },
                            )
                            collab_attributed += 1
                # Skip ingesting the non-allowed lead sponsor.
                continue

            # Default behavior: only ingest lead sponsor if it passes class filters (e.g., INDUSTRY).
            if keep_classes and lead_class and lead_class not in keep_classes:
                continue

            lead_id = db.upsert_account(conn, lead_name, modality_tags=["car-t", "t-cell engager"])
            db.insert_signal(
                conn,
                lead_id,
                sig.signal_type,
                sig.source,
                sig.title,
                sig.evidence_url,
                sig.published_at,
                sig.payload,
            )

            if nct_id:
                db.upsert_study(
                    conn,
                    nct_id,
                    lead_id,
                    brief_title=blob.get("brief_title") or "",
                    overall_status=blob.get("overall_status") or "",
                    phases=blob.get("phases") or [],
                    last_update_posted=blob.get("last_update_posted"),
                    sponsor_class=blob.get("sponsor_class"),
                    study_url=blob.get("study_url"),
                    raw=blob.get("raw") or {},
                )

            lead_ingested += 1

            # Also ingest INDUSTRY collaborators as separate accounts (even when lead is allowed).
            if include_collabs:
                for c in collaborators:
                    cname = (c.get("name") or "").strip()
                    cclass = (c.get("class") or "").upper()
                    if not cname or cclass != "INDUSTRY":
                        continue
                    collab_name = normalize_account_name(cname, aliases)
                    collab_id = db.upsert_account(conn, collab_name, modality_tags=["car-t", "t-cell engager"])
                    db.insert_signal(
                        conn,
                        collab_id,
                        "trial_collaborator",
                        sig.source,
                        sig.title,
                        sig.evidence_url,
                        sig.published_at,
                        {"nct_id": nct_id, "lead_sponsor": lead_name, "lead_sponsor_class": lead_class},
                    )
                    collab_ingested += 1

    return lead_ingested, collab_ingested, collab_attributed


def ingest_sec_and_patents(conn, cfg: AppConfig) -> int:
    """Ingest SEC filings + patents for watchlist companies."""
    aliases = cfg.aliases()
    companies = cfg.companies_list()

    # SEC config
    sec_cfg_raw = cfg.config.get("sec", {}) or {}
    ua_env = sec_cfg_raw.get("user_agent_env", "SEC_USER_AGENT")
    user_agent = os.environ.get(ua_env) or "trigger-radar (set SEC_USER_AGENT with contact email)"
    sec_cfg = sec_edgar.SecConfig(
        user_agent=user_agent,
        keywords=sec_cfg_raw.get("keywords") or sec_edgar.DEFAULT_KEYWORDS,
        recent_window_days=int(sec_cfg_raw.get("recent_window_days", 90)),
        max_filings_per_company=int(sec_cfg_raw.get("max_filings_per_company", 5)),
        min_keyword_hits=int(sec_cfg_raw.get("min_keyword_hits", 1)),
        request_delay_s=float(sec_cfg_raw.get("request_delay_s", 0.25)),
    )

    # Patents config
    pat_raw = cfg.config.get("patents", {}) or {}
    pat_cfg = patentsview.PatentsConfig(
        keywords=pat_raw.get("keywords") or patentsview.DEFAULT_KEYWORDS,
        recent_window_days=int(pat_raw.get("recent_window_days", 365)),
        max_patents_per_company=int(pat_raw.get("max_patents_per_company", 10)),
        request_delay_s=float(pat_raw.get("request_delay_s", 0.25)),
    )

    ingested = 0

    for c in companies:
        name = c.get("name")
        if not name:
            continue
        account_name = normalize_account_name(name, aliases)
        account_id = db.upsert_account(conn, account_name, modality_tags=["car-t", "t-cell engager"])

        # SEC filings
        try:
            filings = sec_edgar.ingest_sec_filings(account_name, sec_cfg)
            for sig in filings:
                db.insert_signal(
                    conn,
                    account_id,
                    sig.signal_type,
                    sig.source,
                    sig.title,
                    sig.evidence_url,
                    sig.published_at,
                    sig.payload,
                )
                ingested += 1
        except Exception as e:
            print(f"[sec] WARN: ingest failed for {account_name}: {e}")

        # Patents
        try:
            pats = patentsview.ingest_patents(account_name, pat_cfg)
            for sig in pats:
                db.insert_signal(
                    conn,
                    account_id,
                    sig.signal_type,
                    sig.source,
                    sig.title,
                    sig.evidence_url,
                    sig.published_at,
                    sig.payload,
                )
                ingested += 1
        except Exception as e:
            print(f"[patents] WARN: ingest failed for {account_name}: {e}")

    return ingested


def update_scores_and_export(conn, cfg: AppConfig) -> None:
    """Compute scores across *all* ingested signals, then export ranked + watchlist views."""
    aliases = cfg.aliases()
    # Normalize watchlist names using the same alias mapping as ingestion.
    watchlist = {normalize_account_name(n, aliases) for n in cfg.company_names_set()}
    cur = conn.cursor()

    # Ensure watchlist companies exist as accounts even if they have zero signals yet.
    for c in cfg.companies_list():
        nm = (c.get("name") or "").strip()
        if nm:
            db.upsert_account(conn, normalize_account_name(nm, aliases), modality_tags=["car-t", "t-cell engager"])

    rows_out: List[Dict[str, Any]] = []
    watch_rows: List[Dict[str, Any]] = []

    for a in db.fetch_accounts(conn):
        account_id = int(a["account_id"])
        company = a["name"]

        # Trials are stored in studies table
        cur.execute(
            "SELECT * FROM studies WHERE account_id=? ORDER BY COALESCE(last_update_posted,'') DESC",
            (account_id,),
        )
        trials = [dict(r) for r in cur.fetchall()]
        for t in trials:
            try:
                t["phases"] = json.loads(t.get("phases_json") or "[]")
            except Exception:
                t["phases"] = []

        # SEC + patents signals
        sec_sigs = db.get_signals_for_account(conn, account_id, signal_type="sec_filing")
        patent_sigs = db.get_signals_for_account(conn, account_id, signal_type="patent_publication")

        scores = compute_scores(
            trials,
            sec_sigs,
            patent_sigs,
            cfg.config,
            company_in_watchlist=(normalize_account_name(company, aliases) in watchlist),
        )

        db.set_scores(conn, account_id, scores["fit"], scores["urgency"], scores["access"], scores["total"])

        all_sigs = db.get_signals_for_account(conn, account_id)
        evidence = [s["evidence_url"] for s in all_sigs[:8] if s.get("evidence_url")]
        roles = recommend_roles(all_sigs, max_roles=5)

        best_fit_trial = scores.get("best_fit_trial") or {}
        best_urg_trial = scores.get("best_urgency_trial") or {}

        row = {
            # Canonical keys used by export.py
            "account_name": company,
            "fit": scores.get("fit"),
            "urgency": scores.get("urgency"),
            "access": scores.get("access", 0.0),
            "total": scores.get("total"),
            "fit_reason": scores.get("fit_reason"),
            "urgency_reason": scores.get("urgency_reason"),
            "urgency_source": scores.get("urgency_source"),
            "trigger_summary": summarize_triggers(all_sigs),
            "best_fit_trial_title": best_fit_trial.get("brief_title"),
            "best_fit_trial_status": best_fit_trial.get("overall_status"),
            "best_fit_trial_phase": ",".join(best_fit_trial.get("phases") or []),
            "best_fit_trial_url": best_fit_trial.get("study_url"),
            "best_urgency_trial_title": best_urg_trial.get("brief_title"),
            "best_urgency_trial_status": best_urg_trial.get("overall_status"),
            "best_urgency_trial_phase": ",".join(best_urg_trial.get("phases") or []),
            "best_urgency_trial_url": best_urg_trial.get("study_url"),
            "sec": scores.get("sec"),
            "patents": scores.get("patents"),
            "target_roles": roles,
            # Backward compatible/debug keys
            "company": company,
            "total_score": scores.get("total"),
            "fit_score": scores.get("fit"),
            "urgency_score": scores.get("urgency"),
            "access_score": scores.get("access", 0.0),
            "evidence_links": evidence,
            "score_details": scores.get("details"),
        }

        require_signals = bool(cfg.config.get("export", {}).get("ranked_require_signals", False))
        if (not require_signals) or (len(all_sigs) > 0) or (len(trials) > 0):
            rows_out.append(row)
        if company in watchlist:
            watch_rows.append(row)

    rows_out.sort(key=lambda r: (r.get("total") or 0.0), reverse=True)
    watch_rows.sort(key=lambda r: (r.get("total") or 0.0), reverse=True)

    export_ranked(rows_out, cfg.export_csv_path(), cfg.export_json_path(), top_n=cfg.export_top_n())
    export_watchlist(watch_rows, cfg.export_watchlist_csv_path(), cfg.export_watchlist_json_path())

    print("[export] wrote ranked + watchlist exports")


def run_daily(cfg: AppConfig) -> None:
    """Legacy: ingest trials only."""
    conn = db.connect()
    db.migrate(conn)
    lead, collab, attributed = ingest_trials(conn, cfg)
    print(f"[daily] ingested lead-sponsor trial signals: {lead}")
    if cfg.ctg_include_industry_collaborators():
        print(f"[daily] ingested industry-collaborator signals: {collab}")
    if bool(cfg.config.get("ctg", {}).get("allow_non_industry_lead_with_industry_collab", False)):
        print(f"[daily] attributed non-industry leads to collaborators (synthetic studies): {attributed}")
    conn.close()


def run_weekly(cfg: AppConfig) -> None:
    """Legacy: ingest SEC + patents only (watchlist)."""
    conn = db.connect()
    db.migrate(conn)
    ing = ingest_sec_and_patents(conn, cfg)
    print(f"[weekly] ingested sec+patent signals: {ing}")
    update_scores_and_export(conn, cfg)
    conn.close()


def run_all(cfg: AppConfig) -> None:
    """Ingest trials + SEC + patents, then compute scores once across everything."""
    conn = db.connect()
    db.migrate(conn)

    lead, collab, attributed = ingest_trials(conn, cfg)
    print(f"[all] ingested lead-sponsor trial signals: {lead}")
    if cfg.ctg_include_industry_collaborators():
        print(f"[all] ingested industry-collaborator signals: {collab}")
    if bool(cfg.config.get("ctg", {}).get("allow_non_industry_lead_with_industry_collab", False)):
        print(f"[all] attributed non-industry leads to collaborators (synthetic studies): {attributed}")

    other = ingest_sec_and_patents(conn, cfg)
    print(f"[all] ingested sec+patent signals: {other}")

    update_scores_and_export(conn, cfg)
    conn.close()


def run_export_only(cfg: AppConfig) -> None:
    conn = db.connect()
    db.migrate(conn)
    update_scores_and_export(conn, cfg)
    conn.close()


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--mode", choices=["all", "daily", "weekly", "export-only"], required=True)
    args = ap.parse_args()

    cfg = AppConfig.load()
    if args.mode == "all":
        run_all(cfg)
    elif args.mode == "daily":
        run_daily(cfg)
    elif args.mode == "weekly":
        run_weekly(cfg)
    else:
        run_export_only(cfg)


if __name__ == "__main__":
    main()

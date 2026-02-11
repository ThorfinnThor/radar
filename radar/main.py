from __future__ import annotations
import argparse, json
from typing import Any, Dict, List

from radar.config import AppConfig
from radar import db
from radar.collectors import clinicaltrials as ctg
from radar.collectors import greenhouse, lever, workday
from radar.scoring import compute_scores
from radar.export import export_ranked, export_watchlist, summarize_triggers
from radar.role_recommender import recommend_roles

def normalize_account_name(name: str, aliases: Dict[str, str]) -> str:
    n = (name or "").strip()
    return aliases.get(n, n) if n else "UNKNOWN"


def _job_hit_details(job_sigs: List[Dict[str, Any]], keywords: List[str], window_days: int = 45, max_titles: int = 3) -> Dict[str, Any]:
    # Best-effort: job signals may have different title fields; we use signals table 'title'
    from radar.scoring import within_days
    recent = [j for j in job_sigs if within_days(j.get("published_at"), window_days)]
    titles = []
    matched = set()
    for j in recent:
        t = (j.get("title") or "").lower()
        for k in keywords:
            if k in t:
                matched.add(k)
        if j.get("title") and len(titles) < max_titles:
            titles.append(j.get("title"))
    return {"recent_job_hits": len(recent), "job_hit_titles": titles, "job_hit_keywords": sorted(matched)}
def run_daily(cfg: AppConfig) -> None:
    conn = db.connect()
    db.migrate(conn)

    if bool(cfg.config.get('jobs', {}).get('run_on_daily', False)):
        j_ing = ingest_jobs(conn, cfg)
        print(f"[daily] ingested job postings (all): {j_ing}")


    aliases = cfg.aliases()
    keep_statuses = {s.upper() for s in cfg.ctg_keep_statuses()}
    keep_classes = {s.upper() for s in cfg.ctg_keep_sponsor_classes()}
    include_collabs = cfg.ctg_include_industry_collaborators()
    allow_other_lead_if_industry_collab = bool(
        cfg.config.get("ctg", {}).get("allow_non_industry_lead_with_industry_collab", False)
    )

    ingested = 0
    collab_ingested = 0
    collab_attributed = 0

    for q in cfg.ctg_queries():
        studies = ctg.fetch_studies(cfg.ctg_base_url(), q, page_size=cfg.ctg_page_size())
        for st in studies:
            nct_id, sig, blob = ctg.normalize_study(st)
            lead_name = normalize_account_name(sig.account_name, aliases)

            status = (blob.get("overall_status") or "").upper()
            if keep_statuses and status and status not in keep_statuses:
                continue

            lead_class = (blob.get("sponsor_class") or "").upper()
            lead_allowed = (not keep_classes) or (lead_class in keep_classes)
            has_industry_collab = any(
                ((c.get("class") or "").upper() == "INDUSTRY")
                for c in (sig.payload.get("collaborators") or [])
            )

            # If lead sponsor is not INDUSTRY (or not in kept classes), but there is at least one
            # INDUSTRY collaborator, optionally keep the study by attributing it to collaborators.
            if (not lead_allowed) and (allow_other_lead_if_industry_collab and has_industry_collab):
                if include_collabs:
                    for c in (sig.payload.get("collaborators") or []):
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
                        # trial-based scoring, without violating the studies.nct_id primary key.
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
                # Skip lead sponsor ingestion entirely
                continue

            # Default behavior: only ingest lead sponsor if it passes class filters (e.g., INDUSTRY)
            if keep_classes and lead_class and lead_class not in keep_classes:
                continue

            lead_id = db.upsert_account(conn, lead_name, modality_tags=["car-t", "t-cell engager"])
            db.insert_signal(
                conn, lead_id, sig.signal_type, sig.source, sig.title, sig.evidence_url, sig.published_at, sig.payload
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

            ingested += 1

            # Also ingest INDUSTRY collaborators as separate accounts (even when lead is INDUSTRY).
            if include_collabs:
                for c in (sig.payload.get("collaborators") or []):
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

    print(f"[daily] ingested lead-sponsor trial signals: {ingested}")
    if include_collabs:
        print(f"[daily] ingested industry-collaborator signals: {collab_ingested}")
    if allow_other_lead_if_industry_collab:
        print(f"[daily] attributed non-industry leads to collaborators (synthetic studies): {collab_attributed}")
    conn.close()



def ingest_jobs(conn, cfg: AppConfig) -> int:
    """Ingest job postings for watchlist companies via their ATS."""

    aliases = cfg.aliases()
    companies = cfg.companies_list()
    ingested = 0

    for c in companies:
        name = c.get("name")
        if not name:
            continue
        account_name = normalize_account_name(name, aliases)
        ats = (c.get("ats") or "").lower().strip()

        account_id = db.upsert_account(conn, account_name, modality_tags=["car-t", "t-cell engager"])

        try:
            if ats == "greenhouse":
                token = c.get("greenhouse_board_token")
                if not token:
                    continue
                jobs = greenhouse.fetch_jobs(token)
                for j in jobs:
                    sig = greenhouse.normalize_job(j, account_name, board_token=token)
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

            elif ats == "lever":
                token = c.get("lever_account")
                if not token:
                    continue
                jobs = lever.fetch_jobs(token)
                for j in jobs:
                    sig = lever.normalize_job(j, account_name)
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

            elif ats == "workday":
                tenant = c.get("tenant")
                wd_host = c.get("wd_host")
                site = c.get("site")
                if not tenant or not wd_host or not site:
                    continue
                jobs = workday.fetch_jobs(tenant=tenant, site=site, wd_host=wd_host, limit=50, max_pages=20)
                for j in jobs:
                    sig = workday.normalize_job(j, account_name, tenant=tenant, site=site, wd_host=wd_host)
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
            print(f"[jobs] WARN: failed jobs ingest for {account_name}: {e}")

    return ingested

def run_weekly(cfg: AppConfig) -> None:
    conn = db.connect()
    db.migrate(conn)

    aliases = cfg.aliases()
    companies = cfg.companies_list()

    ingested = 0

    # 1) Ingest public jobs JSON if present (recommended)
    public_jobs_path = (cfg.config.get("jobs", {}) or {}).get("public_jobs_json_path", "public/jobs.json")
    if public_jobs_path and os.path.exists(public_jobs_path):
        try:
            from radar.collectors import jobs_json as jobs_json_collector
            sigs = jobs_json_collector.ingest_jobs_json(public_jobs_path)
            for sig in sigs:
                acct = normalize_account_name(sig.account_name, aliases)
                account_id = db.upsert_account(conn, acct, modality_tags=["car-t", "t-cell engager"])
                db.insert_signal(conn, account_id, sig.signal_type, sig.source, sig.title, sig.evidence_url, sig.published_at, sig.payload)
                ingested += 1
            print(f"[weekly] ingested job postings from {public_jobs_path}: {len(sigs)}")
        except Exception as e:
            print(f"[weekly] WARN: failed ingest public jobs JSON {public_jobs_path}: {e}")
    else:
        print(f"[weekly] public jobs JSON not found at {public_jobs_path} (skip)")

    # 2) Optional: also ingest from ATS sources defined in companies.yaml
    for c in companies:
        name = c.get("name")
        if not name:
            continue
        account_name = normalize_account_name(name, aliases)
        ats = (c.get("ats") or "").lower().strip()

        if not ats:
            continue

        account_id = db.upsert_account(conn, account_name, modality_tags=["car-t", "t-cell engager"])

        try:
            if ats == "greenhouse":
                token = c.get("greenhouse_board_token")
                if not token:
                    continue
                jobs = greenhouse.fetch_jobs(token)
                for j in jobs:
                    sig = greenhouse.normalize_job(j, account_name, board_token=token)
                    db.insert_signal(conn, account_id, sig.signal_type, sig.source, sig.title, sig.evidence_url, sig.published_at, sig.payload)
                    ingested += 1

            elif ats == "lever":
                token = c.get("lever_account")
                if not token:
                    continue
                jobs = lever.fetch_jobs(token)
                for j in jobs:
                    sig = lever.normalize_job(j, account_name)
                    db.insert_signal(conn, account_id, sig.signal_type, sig.source, sig.title, sig.evidence_url, sig.published_at, sig.payload)
                    ingested += 1

            elif ats == "workday":
                tenant = c.get("tenant")
                wd_host = c.get("wd_host")
                site = c.get("site")
                if not tenant or not wd_host or not site:
                    continue
                jobs = workday.fetch_jobs(tenant=tenant, site=site, wd_host=wd_host, limit=50, max_pages=20)
                for j in jobs:
                    sig = workday.normalize_job(j, account_name, tenant=tenant, site=site, wd_host=wd_host)
                    db.insert_signal(conn, account_id, sig.signal_type, sig.source, sig.title, sig.evidence_url, sig.published_at, sig.payload)
                    ingested += 1

        except Exception as e:
            print(f"[weekly] WARN: failed jobs ingest for {account_name}: {e}")

    print(f"[weekly] ingested job postings (all): {ingested}")

    update_scores_and_export(conn, cfg)
    conn.close()


def update_scores_and_export(conn, cfg: AppConfig) -> None:
    watchlist = cfg.company_names_set()
    cur = conn.cursor()
    keywords = [k.lower() for k in cfg.job_keywords()]
    job_window = int(cfg.config.get('jobs', {}).get('recent_window_days', 45))

    # Ensure watchlist companies exist as accounts even if they have zero signals yet.
    for c in cfg.companies_list():
        nm = (c.get("name") or "").strip()
        if nm:
            db.upsert_account(conn, nm, modality_tags=["car-t","t-cell engager"])

    rows_out: List[Dict[str, Any]] = []
    watch_rows: List[Dict[str, Any]] = []

    for a in db.fetch_accounts(conn):
        account_id = int(a["account_id"])
        company = a["name"]

        cur.execute("SELECT * FROM studies WHERE account_id=? ORDER BY COALESCE(last_update_posted,'') DESC", (account_id,))
        trials = [dict(r) for r in cur.fetchall()]
        for t in trials:
            try:
                t["phases"] = json.loads(t.get("phases_json") or "[]")
            except Exception:
                t["phases"] = []

        cur.execute("SELECT * FROM signals WHERE account_id=? AND signal_type='job_posting' ORDER BY COALESCE(published_at, created_at) DESC", (account_id,))
        job_sigs = [dict(r) for r in cur.fetchall()]

        scores = compute_scores(trials, job_sigs, cfg.config, company_in_watchlist=(company in watchlist))
        hit = _job_hit_details(job_sigs, keywords, window_days=job_window)
        db.set_scores(conn, account_id, scores["fit"], scores["urgency"], scores["access"], scores["total"])

        cur.execute("SELECT * FROM signals WHERE account_id=? ORDER BY COALESCE(published_at, created_at) DESC", (account_id,))
        all_sigs = [dict(r) for r in cur.fetchall()]

        evidence = [s["evidence_url"] for s in all_sigs[:8] if s.get("evidence_url")]
        roles = recommend_roles(all_sigs, max_roles=5)
        row = {
            "company": company,
            "total_score": scores["total"],
            "fit_score": scores["fit"],
            "urgency_score": scores["urgency"],
            "access_score": scores["access"],
            "trigger_summary": summarize_triggers(all_sigs),
            "evidence_links": evidence,
            "target_roles": roles,
            "trial_count": scores.get("details", {}).get("trial_count"),
            "recent_job_hits": scores.get("details", {}).get("jobs", {}).get("relevant_recent_jobs"),
            "best_fit_trial_title": (scores.get("details", {}).get("best_fit_trial") or {}).get("brief_title"),
            "best_fit_trial_status": (scores.get("details", {}).get("best_fit_trial") or {}).get("overall_status"),
            "best_fit_trial_phases": (scores.get("details", {}).get("best_fit_trial") or {}).get("phases"),
            "best_fit_trial_last_update": (scores.get("details", {}).get("best_fit_trial") or {}).get("last_update_posted"),
            "best_urgency_trial_title": (scores.get("details", {}).get("best_urgency_trial") or {}).get("brief_title"),
            "best_urgency_trial_status": (scores.get("details", {}).get("best_urgency_trial") or {}).get("overall_status"),
            "best_urgency_trial_phases": (scores.get("details", {}).get("best_urgency_trial") or {}).get("phases"),
            "best_urgency_trial_last_update": (scores.get("details", {}).get("best_urgency_trial") or {}).get("last_update_posted"),
            "fit_reason": (scores.get("details", {}).get("reasons", {}) or {}).get("fit_reason"),
            "urgency_reason": (scores.get("details", {}).get("reasons", {}) or {}).get("urgency_reason"),
            "urgency_source": (scores.get("details", {}).get("reasons", {}) or {}).get("urgency_source"),
            "access_reason": (scores.get("details", {}).get("reasons", {}) or {}).get("access_reason"),
            "bonus_recent_trial": (scores.get("details", {}).get("bonuses", {}) or {}).get("recent_trial_bonus"),
            "bonus_multi_trial": (scores.get("details", {}).get("bonuses", {}) or {}).get("multi_trial_bonus"),
            "job_hit_titles": hit.get("job_hit_titles"),
            "job_hit_keywords": hit.get("job_hit_keywords"),
            "score_details": scores.get("details"),
            "target_roles": roles,
        }
        rows_out.append(row)
        if company in watchlist:
            watch_rows.append(row)

    rows_out.sort(key=lambda r: r["total_score"], reverse=True)
    watch_rows.sort(key=lambda r: r["total_score"], reverse=True)

    export_ranked(rows_out, cfg.export_csv_path(), cfg.export_json_path(), top_n=cfg.export_top_n())
    export_watchlist(watch_rows, cfg.export_watchlist_csv_path(), cfg.export_watchlist_json_path())

    print(f"[export] wrote ranked + watchlist exports")

def run_export_only(cfg: AppConfig) -> None:
    conn = db.connect()
    db.migrate(conn)
    update_scores_and_export(conn, cfg)
    conn.close()

def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--mode", choices=["daily","weekly","export-only"], required=True)
    args = ap.parse_args()
    cfg = AppConfig.load()
    if args.mode == "daily":
        run_daily(cfg)
    elif args.mode == "weekly":
        run_weekly(cfg)
    else:
        run_export_only(cfg)

if __name__ == "__main__":
    main()

from __future__ import annotations
import argparse
from typing import Any, Dict, List
from radar.config import AppConfig
from radar import db
from radar.collectors import clinicaltrials as ctg
from radar.collectors import greenhouse, lever
from radar.scoring import compute_scores
from radar.export import export_top_accounts, summarize_triggers

def normalize_account_name(name: str, aliases: Dict[str, str]) -> str:
    n = (name or "").strip()
    if not n:
        return "UNKNOWN"
    return aliases.get(n, n)

def run_daily(cfg: AppConfig) -> None:
    conn = db.connect()
    db.migrate(conn)

    aliases = cfg.aliases()
    keep_statuses = set([s.upper() for s in cfg.ctg_keep_statuses()])
    base_url = cfg.ctg_base_url()
    page_size = cfg.ctg_page_size()

    total = 0
    for q in cfg.ctg_queries():
        studies = ctg.fetch_studies(base_url, q, page_size=page_size)
        for st in studies:
            nct_id, sig, study_blob = ctg.normalize_study(st)
            account_name = normalize_account_name(sig.account_name, aliases)
            # Optional filter: overall status keep list
            overall_status = (study_blob.get("overall_status") or "").upper()
            if keep_statuses and overall_status and overall_status not in keep_statuses:
                continue

            account_id = db.upsert_account(conn, account_name, modality_tags=["car-t","t-cell engager"])
            db.insert_signal(conn, account_id, sig.signal_type, sig.source, sig.title, sig.evidence_url, sig.published_at, sig.payload)

            if nct_id:
                db.upsert_study(
                    conn,
                    nct_id=nct_id,
                    account_id=account_id,
                    brief_title=study_blob.get("brief_title") or "",
                    overall_status=study_blob.get("overall_status") or "",
                    phases=study_blob.get("phases") or [],
                    last_update_posted=study_blob.get("last_update_posted"),
                    study_url=study_blob.get("study_url"),
                    raw=study_blob.get("raw") or {},
                )
            total += 1

    print(f"[daily] ingested trial signals: {total}")
    conn.close()

def run_weekly(cfg: AppConfig) -> None:
    conn = db.connect()
    db.migrate(conn)

    aliases = cfg.aliases()
    keywords = [k.lower() for k in cfg.job_keywords()]
    companies = cfg.companies_list()

    ingested = 0
    for c in companies:
        name = c.get("name")
        if not name:
            continue
        account_name = normalize_account_name(name, aliases)
        ats = (c.get("ats") or "").lower().strip()
        account_id = db.upsert_account(conn, account_name, modality_tags=["car-t","t-cell engager"])

        try:
            if ats == "greenhouse":
                token = c.get("greenhouse_board_token")
                if not token:
                    continue
                jobs = greenhouse.fetch_jobs(token)
                for j in jobs:
                    title = (j.get("title") or "")
                    if any(k in title.lower() for k in keywords):
                        sig = greenhouse.normalize_job(j, account_name)
                        db.insert_signal(conn, account_id, sig.signal_type, sig.source, sig.title, sig.evidence_url, sig.published_at, sig.payload)
                        ingested += 1

            elif ats == "lever":
                token = c.get("lever_account")
                if not token:
                    continue
                jobs = lever.fetch_jobs(token)
                for j in jobs:
                    title = (j.get("text") or "")
                    if any(k in title.lower() for k in keywords):
                        sig = lever.normalize_job(j, account_name)
                        db.insert_signal(conn, account_id, sig.signal_type, sig.source, sig.title, sig.evidence_url, sig.published_at, sig.payload)
                        ingested += 1
        except Exception as e:
            print(f"[weekly] WARN: failed jobs ingest for {account_name}: {e}")

    print(f"[weekly] ingested job signals: {ingested}")

    # Update scores for all accounts and export Top-N
    update_scores_and_export(conn, cfg)
    conn.close()

def update_scores_and_export(conn, cfg: AppConfig) -> None:
    accounts = db.fetch_accounts(conn)
    watchlist = cfg.company_names_set()
    aliases = cfg.aliases()

    rows_out: List[Dict[str, Any]] = []

    # Prepare a small helper query per account for trials + job signals
    cur = conn.cursor()
    for a in accounts:
        account_id = int(a["account_id"])
        company = a["name"]

        cur.execute("SELECT * FROM studies WHERE account_id=? ORDER BY COALESCE(last_update_posted,'') DESC", (account_id,))
        trials = [dict(r) for r in cur.fetchall()]
        for t in trials:
            try:
                import json
                t["phases"] = json.loads(t.get("phases_json") or "[]")
            except Exception:
                t["phases"] = []

        cur.execute("SELECT * FROM signals WHERE account_id=? AND signal_type='job_posting' ORDER BY COALESCE(published_at, created_at) DESC", (account_id,))
        job_sigs = [dict(r) for r in cur.fetchall()]

        scores = compute_scores(
            account=dict(a),
            trials=trials,
            job_signals=job_sigs,
            config=cfg.config,
            company_in_watchlist=(company in watchlist),
        )
        db.set_scores(conn, account_id, scores["fit"], scores["urgency"], scores["access"], scores["total"])

        # Evidence links and trigger summary
        cur.execute("SELECT * FROM signals WHERE account_id=? ORDER BY COALESCE(published_at, created_at) DESC", (account_id,))
        all_sigs = [dict(r) for r in cur.fetchall()]
        evidence_links = []
        for s in all_sigs[:6]:
            if s.get("evidence_url"):
                evidence_links.append(s["evidence_url"])

        rows_out.append({
            "company": company,
            "total_score": scores["total"],
            "fit_score": scores["fit"],
            "urgency_score": scores["urgency"],
            "access_score": scores["access"],
            "trigger_summary": summarize_triggers(all_sigs),
            "evidence_links": evidence_links,
        })

    rows_out.sort(key=lambda r: r["total_score"], reverse=True)

    export_top_accounts(
        rows=rows_out,
        out_csv=cfg.export_csv_path(),
        out_json=cfg.export_json_path(),
        top_n=cfg.export_top_n(),
    )
    print(f"[export] wrote {cfg.export_csv_path()} and {cfg.export_json_path()}")

def run_export_only(cfg: AppConfig) -> None:
    conn = db.connect()
    db.migrate(conn)
    update_scores_and_export(conn, cfg)
    conn.close()

def main():
    ap = argparse.ArgumentParser(description="Trigger Radar")
    ap.add_argument("--mode", choices=["daily", "weekly", "export-only"], required=True)
    args = ap.parse_args()

    cfg = AppConfig.load()

    if args.mode == "daily":
        run_daily(cfg)
    elif args.mode == "weekly":
        run_weekly(cfg)
    elif args.mode == "export-only":
        run_export_only(cfg)

if __name__ == "__main__":
    main()

from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from radar.models import NormalizedSignal

def _iso(dt: Any) -> Optional[str]:
    if dt is None:
        return None
    if isinstance(dt, (int, float)):
        if dt > 10_000_000_000:
            dt = dt / 1000.0
        return datetime.fromtimestamp(dt, tz=timezone.utc).isoformat()
    if isinstance(dt, str):
        s = dt.strip()
        return s or None
    return None

def load_jobs(path: str) -> List[Dict[str, Any]]:
    with open(path, "r", encoding="utf-8") as f:
        data = json.load(f)
    if isinstance(data, list):
        return data
    if isinstance(data, dict) and isinstance(data.get("jobs"), list):
        return data["jobs"]
    raise ValueError("jobs.json must be a list of job objects or an object with a 'jobs' list")

def normalize_job(job: Dict[str, Any]) -> NormalizedSignal:
    company = job.get("company") or {}
    company_name = (company.get("name") or job.get("companyName") or "").strip() or "Unknown"

    title = (job.get("title") or "").strip()
    desc = ""
    d = job.get("description") or {}
    if isinstance(d, dict):
        desc = (d.get("text") or d.get("html") or "").strip()
    else:
        desc = str(d)

    posted_at = _iso(job.get("postedAt") or job.get("posted_at") or job.get("createdAt") or job.get("created_at"))
    scraped_at = _iso(job.get("scrapedAt") or job.get("scraped_at") or job.get("updatedAt") or job.get("updated_at"))
    published_at = posted_at or scraped_at

    url = job.get("url") or job.get("jobUrl") or job.get("applyUrl") or job.get("apply_url")
    apply_url = job.get("applyUrl") or job.get("apply_url")
    evidence = apply_url or url

    req_id = job.get("reqId") or job.get("req_id") or job.get("id")
    loc = job.get("location") or job.get("locations") or job.get("primaryLocation")

    payload = {
        "title": title,
        "req_id": req_id,
        "company": company,
        "location": loc,
        "url": url,
        "apply_url": apply_url,
        "posted_at": posted_at,
        "scraped_at": scraped_at,
        "text_blob": f"{title}\n{desc}".strip(),
        "raw": job,
    }

    return NormalizedSignal(
        account_name=company_name,
        signal_type="job_posting",
        source="jobs_json",
        title=title or None,
        evidence_url=evidence,
        published_at=published_at,
        payload=payload,
    )

def ingest_jobs_json(path: str) -> List[NormalizedSignal]:
    jobs = load_jobs(path)
    return [normalize_job(j) for j in jobs]

from __future__ import annotations
import requests
from typing import Any, Dict, List
from radar.models import NormalizedSignal

def fetch_jobs(tenant: str, site: str, wd_host: str, limit: int = 50, max_pages: int = 20) -> List[Dict[str, Any]]:
    base = f"https://{tenant}.{wd_host}.myworkdayjobs.com"
    url = f"{base}/wday/cxs/{tenant}/{site}/jobs"
    jobs: List[Dict[str, Any]] = []
    offset = 0
    headers = {"Content-Type": "application/json", "Accept": "application/json"}
    for _ in range(max_pages):
        payload = {"limit": limit, "offset": offset, "searchText": "", "appliedFacets": {}}
        r = requests.post(url, headers=headers, json=payload, timeout=60)
        r.raise_for_status()
        data = r.json()
        postings = data.get("jobPostings") or []
        if not postings:
            break
        jobs.extend(postings)
        if len(postings) < limit:
            break
        offset += limit
    return jobs

def normalize_job(job: Dict[str, Any], company_name: str, tenant: str, site: str, wd_host: str, source: str = "workday") -> NormalizedSignal:
    title = job.get("title") or job.get("externalTitle") or job.get("postedTitle")
    external_path = job.get("externalPath") or job.get("externalUrl")
    if isinstance(external_path, str) and external_path.startswith("/"):
        evidence_url = f"https://{tenant}.{wd_host}.myworkdayjobs.com{external_path}"
    elif isinstance(external_path, str) and external_path.startswith("http"):
        evidence_url = external_path
    else:
        evidence_url = f"https://{tenant}.{wd_host}.myworkdayjobs.com/{site}"
    posted_on = job.get("postedOn") or job.get("postedDate")
    return NormalizedSignal(
        account_name=company_name,
        signal_type="job_posting",
        source=source,
        title=title,
        evidence_url=evidence_url,
        published_at=str(posted_on) if posted_on is not None else None,
        payload=job,
    )

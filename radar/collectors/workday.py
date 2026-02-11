from __future__ import annotations
import requests
from typing import Any, Dict, List, Optional
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

def _job_detail_url(tenant: str, site: str, wd_host: str, external_path: str) -> Optional[str]:
    if not external_path or "/job/" not in external_path:
        return None
    slug = external_path.split("/job/", 1)[1].lstrip("/")
    if not slug:
        return None
    return f"https://{tenant}.{wd_host}.myworkdayjobs.com/wday/cxs/{tenant}/{site}/job/{slug}"

def fetch_job_detail(tenant: str, site: str, wd_host: str, external_path: str) -> Optional[Dict[str, Any]]:
    url = _job_detail_url(tenant, site, wd_host, external_path)
    if not url:
        return None
    try:
        r = requests.get(url, headers={"Accept": "application/json"}, timeout=60)
        if r.status_code != 200:
            return None
        return r.json()
    except Exception:
        return None

def normalize_job(job: Dict[str, Any], company_name: str, tenant: str, site: str, wd_host: str, source: str = "workday") -> NormalizedSignal:
    title = job.get("title") or job.get("externalTitle") or job.get("postedTitle") or ""

    external_path = job.get("externalPath") or ""
    if isinstance(external_path, str) and external_path.startswith("/"):
        evidence_url = f"https://{tenant}.{wd_host}.myworkdayjobs.com{external_path}"
    else:
        evidence_url = f"https://{tenant}.{wd_host}.myworkdayjobs.com/{site}"

    posted_on = job.get("postedOn") or job.get("postedDate")

    detail = fetch_job_detail(tenant, site, wd_host, external_path) if external_path else None
    description = ""
    if isinstance(detail, dict):
        jpi = detail.get("jobPostingInfo") or {}
        if isinstance(jpi, dict):
            description = jpi.get("jobDescription") or jpi.get("externalDescription") or ""
        if not description:
            description = detail.get("jobDescription") or ""

    payload = {
        "title": title,
        "posted_on": posted_on,
        "external_path": external_path,
        "detail": detail,
        "text_blob": f"{title}\n{description}".strip(),
    }

    return NormalizedSignal(
        account_name=company_name,
        signal_type="job_posting",
        source=source,
        title=title or None,
        evidence_url=evidence_url,
        published_at=str(posted_on) if posted_on is not None else None,
        payload=payload,
    )

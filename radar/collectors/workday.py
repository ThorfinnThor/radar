from __future__ import annotations

import re
import requests
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import urlparse

from radar.models import NormalizedSignal

UA = (
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
)

def _browser_headers(origin: str, referer: str) -> Dict[str, str]:
    return {
        "Accept": "application/json, text/plain, */*",
        "Content-Type": "application/json;charset=UTF-8",
        "Origin": origin,
        "Referer": referer,
        "User-Agent": UA,
        "Accept-Language": "en-US,en;q=0.9",
        "X-Requested-With": "XMLHttpRequest",
    }

def _discover_cxs_base(landing_url: str, timeout: int = 60) -> Optional[Tuple[str, str]]:
    """Return (cxs_base, origin) where cxs_base is:
       https://<host>/wday/cxs/<tenant>/<site>
    """
    try:
        r = requests.get(
            landing_url,
            headers={"User-Agent": UA, "Accept": "text/html"},
            timeout=timeout,
            allow_redirects=True,
        )
        if r.status_code != 200:
            return None

        final = r.url
        host = urlparse(final).netloc
        origin = f"https://{host}"
        html = r.text

        # Absolute API URL in HTML
        m = re.search(r"https://[^\s\"']+/wday/cxs/[^\s\"']+/[^\s\"']+", html)
        if m:
            u = m.group(0).split("?")[0].rstrip("/")
            u = re.sub(r"/jobs/?$", "", u)
            return (u, origin)

        # Relative API path in HTML
        m2 = re.search(r"/wday/cxs/([^/]+)/([^/\"'\?]+)", html)
        if m2:
            tenant2, site2 = m2.group(1), m2.group(2)
            return (f"{origin}/wday/cxs/{tenant2}/{site2}", origin)

        return None
    except Exception:
        return None

def discover_cxs_base(tenant: str, site: str, wd_host: str) -> Tuple[str, str]:
    """Best-effort discovery of the correct CXS base.
    Falls back to provided tenant/site if discovery fails.
    """
    base_host = f"https://{tenant}.{wd_host}.myworkdayjobs.com"
    candidates = [
        f"{base_host}/{site}",
        f"{base_host}/en-US/{site}",
        f"{base_host}/en-us/{site}",
    ]
    for u in candidates:
        got = _discover_cxs_base(u)
        if got:
            return got[0], got[1]
    return f"{base_host}/wday/cxs/{tenant}/{site}", base_host

def fetch_jobs(tenant: str, site: str, wd_host: str, limit: int = 50, max_pages: int = 20) -> List[Dict[str, Any]]:
    cxs_base, origin = discover_cxs_base(tenant=tenant, site=site, wd_host=wd_host)
    jobs_url = f"{cxs_base}/jobs"
    referer = f"{origin}/{site}"
    headers = _browser_headers(origin=origin, referer=referer)

    jobs: List[Dict[str, Any]] = []
    offset = 0
    for _ in range(max_pages):
        payload = {"limit": limit, "offset": offset, "searchText": "", "appliedFacets": {}}
        r = requests.post(jobs_url, headers=headers, json=payload, timeout=60)
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

def _job_detail_url(cxs_base: str, external_path: str) -> Optional[str]:
    if not external_path or "/job/" not in external_path:
        return None
    slug = external_path.split("/job/", 1)[1].lstrip("/")
    if not slug:
        return None
    return f"{cxs_base}/job/{slug}"

def fetch_job_detail(cxs_base: str, origin: str, site: str, external_path: str) -> Optional[Dict[str, Any]]:
    url = _job_detail_url(cxs_base, external_path)
    if not url:
        return None
    referer = f"{origin}/{site}"
    headers = _browser_headers(origin=origin, referer=referer)
    try:
        r = requests.get(url, headers=headers, timeout=60)
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

    cxs_base, origin = discover_cxs_base(tenant=tenant, site=site, wd_host=wd_host)
    detail = fetch_job_detail(cxs_base=cxs_base, origin=origin, site=site, external_path=external_path) if external_path else None

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

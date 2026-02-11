from __future__ import annotations

from typing import Any, Dict, List, Optional
from urllib.parse import urlencode

import requests

from radar.models import NormalizedSignal

UA = (
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
)

def _headers(has_body: bool) -> Dict[str, str]:
    h = {
        "user-agent": UA,
        "accept": "application/json,text/plain,*/*",
        "accept-language": "de-DE,de;q=0.9,en-US;q=0.8,en;q=0.7",
    }
    if has_body:
        h["content-type"] = "application/json"
    return h

def _base(host: str, tenant: str, site: str) -> str:
    return f"https://{host}/wday/cxs/{tenant}/{site}"

def _try_json(method: str, url: str, *, body: Optional[Dict[str, Any]] = None, timeout: int = 60) -> Optional[Dict[str, Any]]:
    try:
        if method == "GET":
            r = requests.get(url, headers=_headers(False), timeout=timeout)
        else:
            r = requests.post(url, headers=_headers(True), json=body, timeout=timeout)
        if not r.ok:
            return None
        return r.json()
    except Exception:
        return None

def _get_page(list_url: str, offset: int, limit: int, search_text: str) -> Optional[Dict[str, Any]]:
    # Variant A: GET with query params
    urlA = f"{list_url}?{urlencode({'offset': offset, 'limit': limit, 'searchText': search_text})}"
    a = _try_json("GET", urlA)
    if a and a.get("jobPostings"):
        return a

    # Variant B: POST with JSON body
    b = _try_json("POST", list_url, body={"appliedFacets": {}, "searchText": search_text, "limit": limit, "offset": offset})
    if b and b.get("jobPostings"):
        return b

    # Variant C: POST with "query" instead of searchText (some tenants)
    c = _try_json("POST", list_url, body={"appliedFacets": {}, "query": search_text, "limit": limit, "offset": offset})
    if c and c.get("jobPostings"):
        return c

    return None

def fetch_jobs(tenant: str, site: str, wd_host: str, limit: int = 50, max_pages: int = 20, search_text: str = "") -> List[Dict[str, Any]]:
    # host matches your config pattern: <tenant>.<wd_host>.myworkdayjobs.com
    host = f"{tenant}.{wd_host}.myworkdayjobs.com"
    base = _base(host, tenant, site)
    list_url = f"{base}/jobs"

    jobs: List[Dict[str, Any]] = []
    offset = 0
    for _ in range(max_pages):
        page = _get_page(list_url, offset=offset, limit=limit, search_text=search_text)
        if not page:
            # raise a helpful error with status/body from a direct POST attempt
            r = requests.post(list_url, headers=_headers(True), json={"appliedFacets": {}, "searchText": search_text, "limit": limit, "offset": offset}, timeout=60)
            msg = (r.text or "")[:300]
            raise requests.HTTPError(f"Workday CXS failed: HTTP {r.status_code} {list_url} :: {msg}")
        postings = page.get("jobPostings") or []
        if not postings:
            break
        jobs.extend(postings)
        if len(postings) < limit:
            break
        offset += limit
    return jobs

def _detail_url(tenant: str, site: str, wd_host: str, external_path: str) -> Optional[str]:
    # external_path e.g. "/Careers/job/Location/Title_JR-0000"
    if not external_path or "/job/" not in external_path:
        return None
    slug = external_path.split("/job/", 1)[1].lstrip("/")
    if not slug:
        return None
    host = f"{tenant}.{wd_host}.myworkdayjobs.com"
    return f"https://{host}/wday/cxs/{tenant}/{site}/job/{slug}"

def fetch_job_detail(tenant: str, site: str, wd_host: str, external_path: str) -> Optional[Dict[str, Any]]:
    url = _detail_url(tenant, site, wd_host, external_path)
    if not url:
        return None
    try:
        r = requests.get(url, headers=_headers(False), timeout=60)
        if r.status_code != 200:
            return None
        return r.json()
    except Exception:
        return None

def normalize_job(job: Dict[str, Any], company_name: str, tenant: str, site: str, wd_host: str, source: str = "workday") -> NormalizedSignal:
    title = job.get("title") or job.get("externalTitle") or job.get("postedTitle") or ""

    external_path = job.get("externalPath") or ""
    host = f"{tenant}.{wd_host}.myworkdayjobs.com"
    if isinstance(external_path, str) and external_path.startswith("/"):
        evidence_url = f"https://{host}{external_path}"
    else:
        evidence_url = f"https://{host}/{site}"

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

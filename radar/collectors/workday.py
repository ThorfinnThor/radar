from __future__ import annotations

import json
import os
import subprocess
from typing import Any, Dict, List, Optional

from radar.models import NormalizedSignal

def _node_bin() -> str:
    return os.environ.get("NODE_BIN", "node")

def _script_path() -> str:
    # scripts/workday_scrape.mjs relative to repo root; in package context, use this file's location
    here = os.path.dirname(os.path.dirname(os.path.dirname(__file__)))  # .../radar/collectors/ -> repo/radar
    repo = os.path.dirname(here)
    return os.path.join(repo, "scripts", "workday_scrape.mjs")

def fetch_jobs(tenant: str, site: str, wd_host: str, limit: int = 50, max_pages: int = 20) -> List[Dict[str, Any]]:
    """Fetch Workday job postings using the bundled Node scraper (more tenant-compatible)."""
    host = f"{tenant}.{wd_host}.myworkdayjobs.com"
    script = _script_path()
    cmd = [
        _node_bin(),
        script,
        "--company", tenant,
        "--host", host,
        "--tenant", tenant,
        "--site", site,
        "--pageSize", str(limit),
        "--maxPages", str(max_pages),
        "--fetchDetails", "true",
    ]
    try:
        p = subprocess.run(cmd, capture_output=True, text=True, check=True, timeout=180)
    except subprocess.CalledProcessError as e:
        out = (e.stderr or e.stdout or "")[:500]
        raise RuntimeError(f"Workday node scraper failed: {out}") from e
    except subprocess.TimeoutExpired as e:
        raise RuntimeError("Workday node scraper timed out") from e

    data = json.loads(p.stdout)
    jobs = data.get("jobs") or []
    # Convert to a structure that normalize_job expects (keep keys we need)
    postings: List[Dict[str, Any]] = []
    for j in jobs:
        postings.append({
            "title": j.get("title"),
            "postedOn": j.get("postedOn"),
            "externalPath": j.get("externalPath"),
            "_description": j.get("description") or "",
            "_origin": (data.get("discovered") or {}).get("origin"),
            "_tenant2": (data.get("discovered") or {}).get("tenant"),
            "_site2": (data.get("discovered") or {}).get("site"),
        })
    return postings

def normalize_job(job: Dict[str, Any], company_name: str, tenant: str, site: str, wd_host: str, source: str = "workday") -> NormalizedSignal:
    title = job.get("title") or job.get("externalTitle") or job.get("postedTitle") or ""
    external_path = job.get("externalPath") or ""
    origin = job.get("_origin") or f"https://{tenant}.{wd_host}.myworkdayjobs.com"
    if isinstance(external_path, str) and external_path.startswith("/"):
        evidence_url = f"{origin}{external_path}"
    else:
        evidence_url = f"{origin}/{site}"

    posted_on = job.get("postedOn") or job.get("postedDate")
    description = job.get("_description") or ""

    payload = {
        "title": title,
        "posted_on": posted_on,
        "external_path": external_path,
        "detail": None,
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

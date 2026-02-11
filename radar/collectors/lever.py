from __future__ import annotations
import requests
from typing import Any, Dict, List
from radar.models import NormalizedSignal

def fetch_jobs(lever_account: str) -> List[Dict[str, Any]]:
    url = f"https://api.lever.co/v0/postings/{lever_account}?mode=json"
    r = requests.get(url, timeout=60)
    r.raise_for_status()
    return r.json()

def normalize_job(job: Dict[str, Any], company_name: str, source: str = "lever") -> NormalizedSignal:
    title = job.get("text") or ""
    description = job.get("description") or ""
    payload = dict(job)
    payload["text_blob"] = f"{title}
{description}".strip()
    return NormalizedSignal(
        account_name=company_name,
        signal_type="job_posting",
        source=source,
        title=title or None,
        evidence_url=job.get("hostedUrl"),
        published_at=str(job.get("createdAt") or job.get("updatedAt") or ""),
        payload=payload,
    )

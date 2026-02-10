from __future__ import annotations
import requests
from typing import Any, Dict, List
from radar.models import NormalizedSignal

def fetch_jobs(board_token: str) -> List[Dict[str, Any]]:
    url = f"https://boards-api.greenhouse.io/v1/boards/{board_token}/jobs"
    r = requests.get(url, timeout=60)
    r.raise_for_status()
    return r.json().get("jobs", [])

def normalize_job(job: Dict[str, Any], company_name: str, source: str = "greenhouse") -> NormalizedSignal:
    return NormalizedSignal(
        account_name=company_name,
        signal_type="job_posting",
        source=source,
        title=job.get("title"),
        evidence_url=job.get("absolute_url"),
        published_at=job.get("updated_at") or job.get("created_at"),
        payload=job,
    )

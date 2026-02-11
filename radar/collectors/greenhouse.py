from __future__ import annotations
import requests
from typing import Any, Dict, List, Optional
from radar.models import NormalizedSignal

def fetch_jobs(board_token: str) -> List[Dict[str, Any]]:
    url = f"https://boards-api.greenhouse.io/v1/boards/{board_token}/jobs"
    r = requests.get(url, timeout=60)
    r.raise_for_status()
    return r.json().get("jobs", [])

def fetch_job_detail(board_token: str, job_id: int) -> Optional[Dict[str, Any]]:
    # Greenhouse job detail endpoint
    url = f"https://boards-api.greenhouse.io/v1/boards/{board_token}/jobs/{job_id}"
    try:
        r = requests.get(url, timeout=60)
        if r.status_code != 200:
            return None
        return r.json()
    except Exception:
        return None

def normalize_job(job: Dict[str, Any], company_name: str, board_token: str, source: str = "greenhouse") -> NormalizedSignal:
    title = job.get("title") or ""
    job_id = job.get("id")
    detail = fetch_job_detail(board_token, job_id) if job_id is not None else None
    content = ""
    if isinstance(detail, dict):
        content = detail.get("content") or ""
    payload = dict(job)
    payload["detail"] = detail
    payload["text_blob"] = f"{title}
{content}".strip()
    return NormalizedSignal(
        account_name=company_name,
        signal_type="job_posting",
        source=source,
        title=title or None,
        evidence_url=job.get("absolute_url"),
        published_at=job.get("updated_at") or job.get("created_at"),
        payload=payload,
    )

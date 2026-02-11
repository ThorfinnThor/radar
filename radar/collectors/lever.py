from __future__ import annotations
import requests
from typing import Any, Dict, List
from radar.models import NormalizedSignal

import datetime as dt


def _ms_to_iso(ms: int | str | None) -> str | None:
    if ms is None:
        return None
    try:
        ms_int = int(ms)
        # Lever timestamps are typically epoch milliseconds
        if ms_int > 10_000_000_000:  # > ~2286-11-20 in seconds, so must be ms
            ts = ms_int / 1000.0
        else:
            ts = float(ms_int)
        return dt.datetime.fromtimestamp(ts, tz=dt.timezone.utc).isoformat()
    except Exception:
        return None


def fetch_jobs(lever_account: str) -> List[Dict[str, Any]]:
    url = f"https://api.lever.co/v0/postings/{lever_account}?mode=json"
    r = requests.get(url, timeout=60)
    r.raise_for_status()
    return r.json()

def normalize_job(job: Dict[str, Any], company_name: str, source: str = "lever") -> NormalizedSignal:
    title = job.get("text") or ""
    description = job.get("description") or ""
    payload = dict(job)
    payload["text_blob"] = f"{title}\n{description}".strip()
    return NormalizedSignal(
        account_name=company_name,
        signal_type="job_posting",
        source=source,
        title=title or None,
        evidence_url=job.get("hostedUrl"),
        published_at=_ms_to_iso(job.get("createdAt") or job.get("updatedAt")),
        payload=payload,
    )

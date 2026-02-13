from __future__ import annotations

import json
import re
import time
from dataclasses import dataclass
from typing import Any, Dict, List, Optional

import requests

from radar.models import NormalizedSignal

PATENTSVIEW_QUERY_URL = "https://api.patentsview.org/patents/query"

DEFAULT_KEYWORDS = [
    "CAR-T", "chimeric antigen receptor", "T cell engager", "CD3", "bispecific",
    "TCR-T", "T cell receptor", "cell therapy", "adoptive cell",
]

@dataclass
class PatentsConfig:
    keywords: List[str]
    recent_window_days: int = 365
    max_patents_per_company: int = 10
    request_delay_s: float = 0.2

def _norm(s: str) -> str:
    s = (s or "").lower()
    s = re.sub(r"[^a-z0-9]+", " ", s)
    return re.sub(r"\s+", " ", s).strip()

def _within_days(date_str: str, window_days: int) -> bool:
    try:
        import datetime as dt
        d = dt.datetime.fromisoformat(date_str)
        if d.tzinfo is None:
            d = d.replace(tzinfo=dt.timezone.utc)
        now = dt.datetime.now(dt.timezone.utc)
        return (now - d).days <= window_days
    except Exception:
        return False

def _keyword_hits(text: str, keywords: List[str]) -> List[str]:
    t = _norm(text)
    hits = []
    for k in keywords:
        kk = _norm(k)
        if kk and kk in t:
            hits.append(k)
    return hits

def _query(company_name: str, cfg: PatentsConfig) -> Dict[str, Any]:
    # Build PatentsView boolean query: assignee contains company name AND text_any on title/abstract
    # Note: PatentsView query syntax uses JSON in 'q' parameter.
    q = {
        "_and": [
            {"_contains": {"assignee_organization": company_name}},
            {"_or": [
                {"_text_any": {"patent_title": " ".join(cfg.keywords)}},
                {"_text_any": {"patent_abstract": " ".join(cfg.keywords)}},
            ]}
        ]
    }
    f = [
        "patent_number", "patent_title", "patent_date",
        "patent_abstract", "assignees.assignee_organization"
    ]
    params = {"q": json.dumps(q), "f": json.dumps(f), "o": json.dumps({"per_page": cfg.max_patents_per_company})}
    time.sleep(cfg.request_delay_s)
    r = requests.get(PATENTSVIEW_QUERY_URL, params=params, timeout=60)
    r.raise_for_status()
    return r.json()

def ingest_patents(company_name: str, cfg: PatentsConfig) -> List[NormalizedSignal]:
    try:
        data = _query(company_name, cfg)
    except Exception:
        return []
    pats = data.get("patents") or []
    sigs: List[NormalizedSignal] = []
    for p in pats:
        num = p.get("patent_number")
        title = (p.get("patent_title") or "").strip()
        date = p.get("patent_date")
        abstract = (p.get("patent_abstract") or "").strip()
        if not num or not date:
            continue
        if not _within_days(date, cfg.recent_window_days):
            continue

        blob = f"{title}\n{abstract}"
        hits = _keyword_hits(blob, cfg.keywords)
        if not hits:
            continue

        evidence_url = f"https://patents.google.com/patent/US{num}"
        payload = {
            "patent_number": num,
            "patent_date": date,
            "patent_title": title,
            "patent_abstract": abstract[:2000],
            "matched_keywords": hits,
            "text_blob": f"{title}\n{abstract[:800]}".strip(),
        }
        sigs.append(NormalizedSignal(
            account_name=company_name,
            signal_type="patent_publication",
            source="patentsview",
            title=title or f"Patent US{num}",
            evidence_url=evidence_url,
            published_at=date,
            payload=payload,
        ))
    return sigs

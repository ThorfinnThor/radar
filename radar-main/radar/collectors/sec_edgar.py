from __future__ import annotations

import json
import os
import re
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import requests

from radar.models import NormalizedSignal

SEC_TICKERS_URL = "https://www.sec.gov/files/company_tickers.json"
SEC_SUBMISSIONS_URL = "https://data.sec.gov/submissions/CIK{cik}.json"

DEFAULT_KEYWORDS = [
    "car-t", "car t", "chimeric antigen receptor",
    "t cell engager", "t-cell engager", "cd3", "bispecific",
    "tcr-t", "tcr t", "t cell receptor", "cell therapy",
    "adoptive cell", "allogeneic", "autologous",
]

@dataclass
class SecConfig:
    user_agent: str
    keywords: List[str]
    recent_window_days: int = 60
    max_filings_per_company: int = 5
    min_keyword_hits: int = 1
    request_delay_s: float = 0.2
    cache_path: Path = Path(__file__).resolve().parents[2] / "data" / "sec_company_tickers.json"

def _ua(cfg: SecConfig) -> Dict[str, str]:
    # SEC requires a descriptive User-Agent with contact info.
    return {"User-Agent": cfg.user_agent, "Accept-Encoding": "gzip, deflate", "Accept": "application/json"}

def _norm(s: str) -> str:
    s = (s or "").lower()
    s = re.sub(r"[^a-z0-9]+", " ", s)
    return re.sub(r"\s+", " ", s).strip()

def _download_tickers(cfg: SecConfig) -> Dict[str, Any]:
    cfg.cache_path.parent.mkdir(parents=True, exist_ok=True)
    if cfg.cache_path.exists():
        try:
            return json.loads(cfg.cache_path.read_text(encoding="utf-8"))
        except Exception:
            pass
    r = requests.get(SEC_TICKERS_URL, headers=_ua(cfg), timeout=60)
    r.raise_for_status()
    data = r.json()
    cfg.cache_path.write_text(json.dumps(data), encoding="utf-8")
    return data

def _best_cik_for_company(company_name: str, tickers: Dict[str, Any]) -> Optional[str]:
    # company_tickers.json is a dict keyed by integer strings with {"cik_str","ticker","title"}
    target = _norm(company_name)
    best = None
    best_score = 0
    for _, rec in tickers.items():
        title = _norm(rec.get("title",""))
        if not title:
            continue
        # very simple token overlap
        tset = set(title.split())
        sset = set(target.split())
        score = len(tset & sset)
        if score > best_score:
            best_score = score
            best = str(rec.get("cik_str"))
    if best and best_score >= 2:
        return best.zfill(10)
    # fallback: try substring match
    for _, rec in tickers.items():
        title = _norm(rec.get("title",""))
        if target and title and (target in title or title in target):
            return str(rec.get("cik_str")).zfill(10)
    return None

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

def _fetch_submissions(cfg: SecConfig, cik10: str) -> Dict[str, Any]:
    time.sleep(cfg.request_delay_s)
    r = requests.get(SEC_SUBMISSIONS_URL.format(cik=cik10), headers=_ua(cfg), timeout=60)
    r.raise_for_status()
    return r.json()

def _filing_doc_url(cik_int: int, accession: str, primary_doc: str) -> str:
    # accession in submissions includes dashes, remove them for path
    acc_nodash = accession.replace("-", "")
    return f"https://www.sec.gov/Archives/edgar/data/{cik_int}/{acc_nodash}/{primary_doc}"

def _strip_html(html: str) -> str:
    html = re.sub(r"<script[\s\S]*?</script>", " ", html, flags=re.I)
    html = re.sub(r"<style[\s\S]*?</style>", " ", html, flags=re.I)
    html = re.sub(r"<[^>]+>", " ", html)
    html = re.sub(r"&nbsp;|&#160;", " ", html)
    return re.sub(r"\s+", " ", html).strip()

def _keyword_hits(text: str, keywords: List[str]) -> List[str]:
    t = _norm(text)
    hits = []
    for k in keywords:
        kk = _norm(k)
        if kk and kk in t:
            hits.append(k)
    return hits

def ingest_sec_filings(company_name: str, cfg: SecConfig) -> List[NormalizedSignal]:
    tickers = _download_tickers(cfg)
    cik10 = _best_cik_for_company(company_name, tickers)
    if not cik10:
        return []

    subs = _fetch_submissions(cfg, cik10)
    cik_int = int(subs.get("cik", cik10).lstrip("0") or "0")
    recent = (subs.get("filings", {}) or {}).get("recent", {}) or {}
    forms = recent.get("form", []) or []
    dates = recent.get("filingDate", []) or []
    accessions = recent.get("accessionNumber", []) or []
    primary_docs = recent.get("primaryDocument", []) or []
    report_dates = recent.get("reportDate", []) or []
    descriptions = recent.get("primaryDocDescription", []) or []

    allowed_forms = {"8-K", "10-Q", "10-K", "20-F", "6-K", "F-1", "S-1"}

    sigs: List[NormalizedSignal] = []
    for form, fdate, acc, pdoc, rdate, desc in list(zip(forms, dates, accessions, primary_docs, report_dates, descriptions))[:200]:
        if form not in allowed_forms:
            continue
        if not fdate or not _within_days(fdate, cfg.recent_window_days):
            continue
        if not acc or not pdoc:
            continue

        evidence_url = _filing_doc_url(cik_int, acc, pdoc)
        title = f"{form} filed {fdate}" + (f" – {desc}" if desc else "")
        text_blob = title

        # Optional: fetch and keyword-scan document (bounded)
        hits: List[str] = []
        excerpt = None
        if len(sigs) < cfg.max_filings_per_company:
            try:
                time.sleep(cfg.request_delay_s)
                r = requests.get(evidence_url, headers={"User-Agent": cfg.user_agent, "Accept": "text/html"}, timeout=60)
                if r.ok and r.text:
                    txt = _strip_html(r.text)
                    hits = _keyword_hits(txt, cfg.keywords)
                    if hits:
                        excerpt = txt[:500]
                        text_blob = f"{title}\n{excerpt}"
            except Exception:
                pass

        # If keyword filtering enabled, require hits
        if cfg.min_keyword_hits > 0 and len(hits) < cfg.min_keyword_hits:
            continue

        payload = {
            "cik": cik10,
            "form": form,
            "filing_date": fdate,
            "report_date": rdate,
            "accession": acc,
            "primary_document": pdoc,
            "description": desc,
            "matched_keywords": hits,
            "excerpt": excerpt,
            "text_blob": text_blob,
        }

        sigs.append(NormalizedSignal(
            account_name=company_name,
            signal_type="sec_filing",
            source="sec_edgar",
            title=title,
            evidence_url=evidence_url,
            published_at=fdate,
            payload=payload,
        ))

        if len(sigs) >= cfg.max_filings_per_company:
            break

    return sigs

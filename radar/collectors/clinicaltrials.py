from __future__ import annotations
import requests
from urllib.parse import urlencode
from typing import Any, Dict, List, Optional
from radar.models import NormalizedSignal

DEFAULT_FIELDS = [
    "protocolSection.identificationModule.nctId",
    "protocolSection.identificationModule.briefTitle",
    "protocolSection.statusModule.overallStatus",
    "protocolSection.statusModule.lastUpdatePostDateStruct.date",
    "protocolSection.designModule.phases",
    "protocolSection.sponsorCollaboratorsModule.leadSponsor.name",
    "protocolSection.conditionsModule.conditions",
    "protocolSection.armsInterventionsModule.interventions",
]

def fetch_studies(base_url: str, query_term: str, page_size: int = 200) -> List[Dict[str, Any]]:
    params = {
        "query.term": query_term,
        "pageSize": page_size,
        "format": "json",
        "countTotal": "true",
        "fields": ",".join(DEFAULT_FIELDS),
    }
    url = f"{base_url}?{urlencode(params)}"
    r = requests.get(url, timeout=45)
    r.raise_for_status()
    return r.json().get("studies", [])

def normalize_study(study: Dict[str, Any], source: str = "clinicaltrials") -> tuple[Optional[str], Optional[int], NormalizedSignal, Dict[str, Any]]:
    ps = study.get("protocolSection", {}) or {}
    ident = ps.get("identificationModule", {}) or {}
    status = ps.get("statusModule", {}) or {}
    design = ps.get("designModule", {}) or {}
    sponsor = (ps.get("sponsorCollaboratorsModule", {}) or {}).get("leadSponsor", {}) or {}

    nct_id = ident.get("nctId")
    title = ident.get("briefTitle")
    overall_status = status.get("overallStatus")
    last_update = (status.get("lastUpdatePostDateStruct", {}) or {}).get("date")
    phases = design.get("phases", []) or []

    evidence_url = f"https://clinicaltrials.gov/study/{nct_id}" if nct_id else None

    account_name = sponsor.get("name") or "UNKNOWN"
    payload = {
        "nct_id": nct_id,
        "overall_status": overall_status,
        "phases": phases,
    }

    sig = NormalizedSignal(
        account_name=account_name,
        signal_type="trial_candidate",
        source=source,
        title=title,
        evidence_url=evidence_url,
        published_at=last_update,
        payload=payload,
    )
    return nct_id, sig, {
        "brief_title": title,
        "overall_status": overall_status,
        "phases": phases,
        "last_update_posted": last_update,
        "study_url": evidence_url,
        "raw": study,
    }

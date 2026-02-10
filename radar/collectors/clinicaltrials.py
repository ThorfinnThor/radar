from __future__ import annotations
import requests
from urllib.parse import urlencode
from typing import Any, Dict, List, Optional, Tuple
from radar.models import NormalizedSignal

DEFAULT_FIELDS = [
    "protocolSection.identificationModule.nctId",
    "protocolSection.identificationModule.briefTitle",
    "protocolSection.statusModule.overallStatus",
    "protocolSection.statusModule.lastUpdatePostDateStruct.date",
    "protocolSection.designModule.phases",
    "protocolSection.sponsorCollaboratorsModule.leadSponsor.name",
    "protocolSection.sponsorCollaboratorsModule.leadSponsor.class",
    "protocolSection.sponsorCollaboratorsModule.collaborators.name",
    "protocolSection.sponsorCollaboratorsModule.collaborators.class",
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
    r = requests.get(url, timeout=60)
    r.raise_for_status()
    return r.json().get("studies", [])

def normalize_study(study: Dict[str, Any], source: str = "clinicaltrials") -> Tuple[Optional[str], NormalizedSignal, Dict[str, Any]]:
    ps = study.get("protocolSection", {}) or {}
    ident = ps.get("identificationModule", {}) or {}
    status = ps.get("statusModule", {}) or {}
    design = ps.get("designModule", {}) or {}
    sc = ps.get("sponsorCollaboratorsModule", {}) or {}

    lead = sc.get("leadSponsor", {}) or {}
    lead_name = lead.get("name") or "UNKNOWN"
    lead_class = (lead.get("class") or "").upper()

    collaborators = sc.get("collaborators", []) or []
    collab_list: List[Dict[str, str]] = []
    for c in collaborators:
        if isinstance(c, dict) and c.get("name"):
            collab_list.append({"name": c.get("name"), "class": (c.get("class") or "").upper()})

    nct_id = ident.get("nctId")
    title = ident.get("briefTitle")
    overall_status = status.get("overallStatus")
    last_update = (status.get("lastUpdatePostDateStruct", {}) or {}).get("date")
    phases = design.get("phases", []) or []

    evidence_url = f"https://clinicaltrials.gov/study/{nct_id}" if nct_id else None

    payload = {
        "nct_id": nct_id,
        "overall_status": overall_status,
        "phases": phases,
        "lead_sponsor_class": lead_class,
        "collaborators": collab_list,
    }

    sig = NormalizedSignal(
        account_name=lead_name,
        signal_type="trial_candidate",
        source=source,
        title=title,
        evidence_url=evidence_url,
        published_at=last_update,
        payload=payload,
    )

    study_blob = {
        "brief_title": title,
        "overall_status": overall_status,
        "phases": phases,
        "last_update_posted": last_update,
        "sponsor_class": lead_class,
        "study_url": evidence_url,
        "raw": study,
    }
    return nct_id, sig, study_blob

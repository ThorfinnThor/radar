from __future__ import annotations
from typing import Any, Dict, List

# Start with a canonical role library for cell & immunotherapies
ROLE_LIBRARY = {
    "translational": "Director/VP, Translational Medicine (Oncology/Immunology)",
    "biomarkers": "Head/Director, Clinical Biomarkers / Translational Biomarkers",
    "immune_monitoring": "Head/Director, Immune Monitoring / Flow Cytometry / Cytometry Core",
    "bioassay_potency": "Director, Bioassay / Potency / Analytical Development",
    "clinical_science": "Clinical Scientist / Early Clinical Development Lead (Phase 1)",
    "cmc_analytical": "Director, CMC / Analytical Development (Cell Therapy/Biologics)",
    "process_dev": "Director, Process Development / MSAT (Cell Therapy)",
    "comp_bio": "Director, Computational Biology / Systems Immunology",
    "external_innovation": "Director, External Innovation / Search & Evaluation (Cell Therapy)",
}

# Keyword → role keys (keep these high-signal; you can tune over time)
KEYWORD_MAP = [
    (["biomarker", "biomarkers", "translational"], ["translational", "biomarkers"]),
    (["immune monitoring", "flow cytometry", "spectral cytometry", "cytometry"], ["immune_monitoring"]),
    (["potency", "bioassay", "cell-based assay", "assay development"], ["bioassay_potency"]),
    (["comparability"], ["bioassay_potency", "cmc_analytical"]),
    (["analytical development"], ["cmc_analytical"]),
    (["process development", "msat", "manufacturing science"], ["process_dev"]),
    (["single-cell", "single cell", "systems immunology", "computational"], ["comp_bio"]),
    (["car-t", "car t", "tcr-t", "tcr t", "t-cell engager", "t cell engager", "cd3", "bispecific"], ["clinical_science"]),
]

def recommend_roles(signals: List[Dict[str, Any]], max_roles: int = 5) -> List[str]:
    """Recommend 3–5 roles per account based on observed signals.
    Uses simple keyword matching over:
      - signal titles (trial titles, job titles)
      - signal payloads (best-effort)
    """
    txt = " ".join([(s.get("title") or "") for s in signals]).lower()

    # Some signals imply external partnering/search & eval relevance
    if any((s.get("signal_type") == "trial_collaborator") for s in signals):
        txt += " external innovation search evaluation"

    role_keys: List[str] = []

    for kws, keys in KEYWORD_MAP:
        if any(k in txt for k in kws):
            for k in keys:
                if k not in role_keys:
                    role_keys.append(k)

    # Ensure at least a sensible baseline set
    baseline = ["translational", "biomarkers", "immune_monitoring", "bioassay_potency", "clinical_science"]
    for k in baseline:
        if k not in role_keys:
            role_keys.append(k)

    roles = [ROLE_LIBRARY[k] for k in role_keys if k in ROLE_LIBRARY]

    # Trim to max_roles but keep at least 3
    roles = roles[:max_roles] if len(roles) > max_roles else roles
    if len(roles) < 3:
        roles = roles + [ROLE_LIBRARY["clinical_science"]]
        roles = roles[:3]
    return roles

from __future__ import annotations
from dataclasses import dataclass
from typing import Any, Dict, Optional

@dataclass
class NormalizedSignal:
    account_name: str
    signal_type: str
    source: str
    title: Optional[str]
    evidence_url: Optional[str]
    published_at: Optional[str]
    payload: Dict[str, Any]

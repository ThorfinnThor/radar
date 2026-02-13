from __future__ import annotations
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List
import yaml

ROOT = Path(__file__).resolve().parents[1]

def load_yaml(path: Path) -> Dict[str, Any]:
    with path.open("r", encoding="utf-8") as f:
        return yaml.safe_load(f) or {}

@dataclass(frozen=True)
class AppConfig:
    config: Dict[str, Any]
    companies: Dict[str, Any]

    @staticmethod
    def load() -> "AppConfig":
        cfg = load_yaml(ROOT / "config" / "config.yaml")
        cos = load_yaml(ROOT / "config" / "companies.yaml")
        return AppConfig(config=cfg, companies=cos)

    def ctg_base_url(self) -> str:
        return self.config.get("ctg", {}).get("base_url")

    def ctg_page_size(self) -> int:
        return int(self.config.get("ctg", {}).get("page_size", 200))

    def ctg_queries(self) -> List[str]:
        return self.config.get("ctg", {}).get("ctg_queries", [])

    def ctg_keep_statuses(self) -> List[str]:
        return self.config.get("ctg", {}).get("keep_statuses", [])

    def ctg_keep_sponsor_classes(self) -> List[str]:
        return self.config.get("ctg", {}).get("keep_sponsor_classes", [])

    def ctg_high_urgency_phases(self) -> List[str]:
        return self.config.get("ctg", {}).get("high_urgency_phases", [])

    def ctg_include_industry_collaborators(self) -> bool:
        return bool(self.config.get("ctg", {}).get("include_industry_collaborators", True))

    def aliases(self) -> Dict[str, str]:
        return self.config.get("normalization", {}).get("aliases", {})

    def export_top_n(self) -> int:
        return int(self.config.get("exports", {}).get("top_n", 40))

    def export_csv_path(self) -> str:
        return self.config.get("exports", {}).get("out_csv", "exports/latest_top40.csv")

    def export_json_path(self) -> str:
        return self.config.get("exports", {}).get("out_json", "exports/latest_top40.json")

    def export_watchlist_csv_path(self) -> str:
        return self.config.get("exports", {}).get("watchlist_csv", "exports/latest_watchlist.csv")

    def export_watchlist_json_path(self) -> str:
        return self.config.get("exports", {}).get("watchlist_json", "exports/latest_watchlist.json")

    def companies_list(self) -> List[Dict[str, Any]]:
        return (self.companies or {}).get("companies", [])

    def company_names_set(self) -> set[str]:
        return {c.get("name","").strip() for c in self.companies_list() if c.get("name")}

# Trigger Radar — T-cell Engagers + CAR-T (industry-first)

This repo builds and maintains an **account discovery list** from **public signals**:
- **ClinicalTrials.gov API v2**: trials matching CAR-T / T-cell engager terms
- **SEC filings** + **Patents**: keyword-triggered weekly signals for the watchlist

Key upgrades in this version:
- Filters ClinicalTrials sponsors to **INDUSTRY** (configurable)
- Optionally ingests **INDUSTRY collaborators** as accounts (big-pharma often appears as collaborator)
- Exports both:
  - `exports/latest_top40.csv` + `.json` (ranked)
  - `exports/latest_watchlist.csv` + `.json` (always includes companies in `config/companies.yaml`)

## Quick start (local)

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt

python -m radar.main --mode all

# Recompute exports from the existing database (no ingest)
python -m radar.main --mode export-only
```

Outputs:
- SQLite DB: `data/radar.sqlite` (ignored by git)
- Exports: `exports/*.csv`, `exports/*.json` (committed by GitHub Actions)

MIT License.

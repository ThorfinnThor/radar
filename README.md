# Trigger Radar (T-cell Engagers + CAR-T) — lightweight account discovery

This repo builds and maintains a **Top-40 target account list** using **public signals**:
- **ClinicalTrials.gov API v2** (trial start/status/phase signals)
- **Greenhouse** + **Lever** public job boards (biomarker/cytometry/assay/potency hiring signals)

It stores signals in a local **SQLite** DB and exports:
- `exports/latest_top40.csv`
- `exports/latest_top40.json`

A **GitHub Actions** workflow runs daily and (optionally) weekly to refresh signals and commit the latest export back to the repo.

## Quick start (local)

1) Create a virtualenv and install dependencies:
```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

2) Edit configuration:
- `config/config.yaml` controls queries and scoring weights.
- `config/companies.yaml` lists companies whose job boards you want to monitor (Greenhouse/Lever tokens).

3) Run:
```bash
# daily trial scan (recommended daily)
python -m radar.main --mode daily

# weekly job scan + spike detection + export (recommended weekly)
python -m radar.main --mode weekly
```

Outputs:
- Database: `data/radar.sqlite`
- Export: `exports/latest_top40.csv`

## Configuration

### `config/config.yaml`
- `ctg_queries`: search terms for ClinicalTrials.gov
- `job_keywords`: keywords matched against job titles
- `scoring`: weights and thresholds

### `config/companies.yaml`
Add companies you care about for job monitoring.

Examples:
```yaml
companies:
  - name: Example Bio
    ats: greenhouse
    greenhouse_board_token: examplebio
  - name: Another Bio
    ats: lever
    lever_account: anotherbio
```

> Tip: Find the token by visiting a company's careers page and looking for `greenhouse.io` or `lever.co` URLs.

## GitHub Actions

Workflow file: `.github/workflows/radar.yml`

- Runs daily at **06:10 UTC** (≈07:10 Berlin in winter).
- Always runs `--mode daily`.
- On Mondays, also runs `--mode weekly` (jobs scan + export).
- Commits updated exports back to the repo.

### Required secrets
None. Uses the default `GITHUB_TOKEN` to commit.

## Notes / limitations
- This does **not** scrape LinkedIn or restricted sources.
- Job collection is only for companies you list in `config/companies.yaml`.
- Sponsor/company normalization is rule-based; you can add aliases in `config/config.yaml`.

---
MIT License.

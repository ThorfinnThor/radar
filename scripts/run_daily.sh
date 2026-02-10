#!/usr/bin/env bash
set -euo pipefail
python -m radar.main --mode daily
python -m radar.main --mode export-only

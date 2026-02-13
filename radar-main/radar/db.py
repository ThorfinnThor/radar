from __future__ import annotations
import sqlite3, json, datetime as dt
from pathlib import Path
from typing import Any, Dict, Optional, List

DB_PATH = Path(__file__).resolve().parents[1] / "data" / "radar.sqlite"

SCHEMA = [
"""CREATE TABLE IF NOT EXISTS accounts (
  account_id INTEGER PRIMARY KEY AUTOINCREMENT,
  name TEXT NOT NULL UNIQUE,
  domain TEXT,
  modality_tags TEXT,
  fit_score REAL DEFAULT 0,
  urgency_score REAL DEFAULT 0,
  access_score REAL DEFAULT 0,
  total_score REAL DEFAULT 0,
  last_seen_at TEXT
);""",
"""CREATE TABLE IF NOT EXISTS signals (
  signal_id INTEGER PRIMARY KEY AUTOINCREMENT,
  account_id INTEGER NOT NULL,
  signal_type TEXT NOT NULL,
  source TEXT NOT NULL,
  title TEXT,
  evidence_url TEXT,
  published_at TEXT,
  payload_json TEXT,
  created_at TEXT NOT NULL,
  UNIQUE(account_id, signal_type, source, evidence_url),
  FOREIGN KEY(account_id) REFERENCES accounts(account_id)
);""",
"""CREATE TABLE IF NOT EXISTS studies (
  nct_id TEXT PRIMARY KEY,
  account_id INTEGER,
  brief_title TEXT,
  overall_status TEXT,
  phases_json TEXT,
  last_update_posted TEXT,
  sponsor_class TEXT,
  study_url TEXT,
  raw_json TEXT,
  FOREIGN KEY(account_id) REFERENCES accounts(account_id)
);""",
"""CREATE INDEX IF NOT EXISTS idx_signals_account ON signals(account_id);"""
]

def connect(db_path: Path = DB_PATH) -> sqlite3.Connection:
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    return conn

def migrate(conn: sqlite3.Connection) -> None:
    cur = conn.cursor()
    for stmt in SCHEMA:
        cur.executescript(stmt)
    conn.commit()

def utc_now_iso() -> str:
    return dt.datetime.now(dt.timezone.utc).isoformat()

def upsert_account(conn: sqlite3.Connection, name: str, domain: Optional[str] = None, modality_tags: Optional[list[str]] = None) -> int:
    name = (name or "").strip() or "UNKNOWN"
    tags = json.dumps(modality_tags or [])
    cur = conn.cursor()
    cur.execute(
        """INSERT INTO accounts(name, domain, modality_tags, last_seen_at)
           VALUES(?,?,?,?)
           ON CONFLICT(name) DO UPDATE SET
             domain=COALESCE(excluded.domain, accounts.domain),
             modality_tags=COALESCE(excluded.modality_tags, accounts.modality_tags),
             last_seen_at=excluded.last_seen_at"""
        , (name, domain, tags, utc_now_iso())
    )
    conn.commit()
    cur.execute("SELECT account_id FROM accounts WHERE name=?", (name,))
    return int(cur.fetchone()["account_id"])

def insert_signal(conn: sqlite3.Connection, account_id: int, signal_type: str, source: str,
                  title: Optional[str], evidence_url: Optional[str], published_at: Optional[str],
                  payload: Optional[Dict[str, Any]]) -> None:
    cur = conn.cursor()
    cur.execute(
        """INSERT OR IGNORE INTO signals(account_id, signal_type, source, title, evidence_url, published_at, payload_json, created_at)
           VALUES(?,?,?,?,?,?,?,?)"""
        , (account_id, signal_type, source, title, evidence_url, published_at, json.dumps(payload or {}), utc_now_iso())
    )
    conn.commit()

def upsert_study(conn: sqlite3.Connection, nct_id: str, account_id: int, brief_title: str,
                 overall_status: str, phases: list[str], last_update_posted: Optional[str],
                 sponsor_class: Optional[str], study_url: Optional[str], raw: Dict[str, Any]) -> None:
    cur = conn.cursor()
    cur.execute(
        """INSERT INTO studies(nct_id, account_id, brief_title, overall_status, phases_json, last_update_posted, sponsor_class, study_url, raw_json)
           VALUES(?,?,?,?,?,?,?,?,?)
           ON CONFLICT(nct_id) DO UPDATE SET
             account_id=excluded.account_id,
             brief_title=excluded.brief_title,
             overall_status=excluded.overall_status,
             phases_json=excluded.phases_json,
             last_update_posted=excluded.last_update_posted,
             sponsor_class=excluded.sponsor_class,
             study_url=excluded.study_url,
             raw_json=excluded.raw_json"""
        , (nct_id, account_id, brief_title, overall_status, json.dumps(phases or []), last_update_posted, sponsor_class, study_url, json.dumps(raw))
    )
    conn.commit()

def fetch_accounts(conn: sqlite3.Connection):
    cur = conn.cursor()
    cur.execute("SELECT * FROM accounts")
    return cur.fetchall()


def get_studies_for_account(conn: sqlite3.Connection, account_id: int) -> List[Dict[str, Any]]:
    """Return normalized studies for an account from the studies table."""
    cur = conn.cursor()
    cur.execute(
        "SELECT nct_id, brief_title, overall_status, phases_json, last_update_posted, sponsor_class, study_url, raw_json FROM studies WHERE account_id=?",
        (account_id,),
    )
    rows = cur.fetchall() or []
    out: List[Dict[str, Any]] = []
    for r in rows:
        phases = []
        try:
            phases = json.loads(r["phases_json"] or "[]") or []
        except Exception:
            phases = []
        out.append({
            "nct_id": r["nct_id"],
            "brief_title": r["brief_title"],
            "overall_status": r["overall_status"],
            "phases": phases,
            "last_update_posted": r["last_update_posted"],
            "sponsor_class": r["sponsor_class"],
            "study_url": r["study_url"],
            "raw_json": r["raw_json"],
        })
    return out
def set_scores(conn: sqlite3.Connection, account_id: int, fit: float, urgency: float, access: float, total: float) -> None:
    cur = conn.cursor()
    cur.execute("UPDATE accounts SET fit_score=?, urgency_score=?, access_score=?, total_score=?, last_seen_at=? WHERE account_id=?",
                (fit, urgency, access, total, utc_now_iso(), account_id))
    conn.commit()


def get_signals_for_account(conn: sqlite3.Connection, account_id: int, signal_type: Optional[str] = None):
    cur = conn.cursor()
    if signal_type:
        cur.execute(
            "SELECT signal_type, source, title, evidence_url, published_at, payload_json, created_at "
            "FROM signals WHERE account_id=? AND signal_type=? "
            "ORDER BY COALESCE(published_at, created_at) DESC",
            (account_id, signal_type),
        )
    else:
        cur.execute(
            "SELECT signal_type, source, title, evidence_url, published_at, payload_json, created_at "
            "FROM signals WHERE account_id=? "
            "ORDER BY COALESCE(published_at, created_at) DESC",
            (account_id,),
        )
    return [dict(r) for r in cur.fetchall()]

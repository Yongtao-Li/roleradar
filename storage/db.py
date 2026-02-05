from __future__ import annotations

import sqlite3
from pathlib import Path
from datetime import date, datetime
from typing import Any, Iterable, Optional, Protocol


class JobLike(Protocol):
    company: str
    job_id: str
    title: str
    url: str
    location: str | None


DB_PATH_DEFAULT = "jobs.sqlite3"


def get_conn(db_path: str = DB_PATH_DEFAULT) -> sqlite3.Connection:
    path = Path(db_path)
    if path.parent != Path("."):
        path.parent.mkdir(parents=True, exist_ok=True)
    return sqlite3.connect(str(path), check_same_thread=False)


def init_db(conn: sqlite3.Connection) -> None:
    conn.execute("""
    CREATE TABLE IF NOT EXISTS jobs (
        job_id TEXT PRIMARY KEY,
        company TEXT NOT NULL,
        title TEXT NOT NULL,
        url TEXT NOT NULL,
        location TEXT,
        first_seen TEXT NOT NULL,
        last_seen TEXT NOT NULL
    )
    """)

    conn.execute("""
    CREATE TABLE IF NOT EXISTS runs (
        run_date TEXT NOT NULL,
        company TEXT NOT NULL,
        ran_at TEXT NOT NULL,
        total_jobs INTEGER NOT NULL,
        new_jobs INTEGER NOT NULL,
        PRIMARY KEY (run_date, company)
    )
    """)
    conn.commit()


def upsert_jobs(conn: sqlite3.Connection, jobs: Iterable[JobLike]) -> None:
    today = date.today().isoformat()
    for j in jobs:
        conn.execute("""
        INSERT INTO jobs (job_id, company, title, url, location, first_seen, last_seen)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(job_id) DO UPDATE SET
            title=excluded.title,
            url=excluded.url,
            location=COALESCE(excluded.location, jobs.location),
            last_seen=excluded.last_seen
        """, (j.job_id, j.company, j.title, j.url, getattr(j, "location", None), today, today))
    conn.commit()


def record_run(conn: sqlite3.Connection, company: str, total_jobs: int, new_jobs: int) -> None:
    today = date.today().isoformat()
    now = datetime.now().isoformat(timespec="seconds")

    conn.execute("""
    INSERT INTO runs (run_date, company, ran_at, total_jobs, new_jobs)
    VALUES (?, ?, ?, ?, ?)
    ON CONFLICT(run_date, company) DO UPDATE SET
        ran_at=excluded.ran_at,
        total_jobs=excluded.total_jobs,
        new_jobs=excluded.new_jobs
    """, (today, company, now, total_jobs, new_jobs))
    conn.commit()


def get_last_run(conn: sqlite3.Connection, company: str) -> Optional[tuple]:
    cur = conn.execute("""
        SELECT run_date, ran_at, total_jobs, new_jobs
        FROM runs
        WHERE company = ?
        ORDER BY run_date DESC
        LIMIT 1
    """, (company,))
    return cur.fetchone()


def get_new_today(conn: sqlite3.Connection, company: str) -> list[tuple[str, str, str, str]]:
    today = date.today().isoformat()
    cur = conn.execute("""
        SELECT title, url, first_seen, last_seen
        FROM jobs
        WHERE company = ? AND first_seen = ?
        ORDER BY title
    """, (company, today))
    return cur.fetchall()


def search_jobs(conn, company: str | None, query: str, location: str | None = None, limit: int = 500):
    q = f"%{query.strip()}%"

    where = ["title LIKE ?"]
    params: list[Any] = [q]

    if company:
        where.insert(0, "company = ?")
        params.insert(0, company)

    if location and location != "(Any)":
        where.append("location = ?")
        params.append(location)

    sql = f"""
        SELECT title, url, location, first_seen, last_seen
        FROM jobs
        WHERE {' AND '.join(where)}
        ORDER BY last_seen DESC, title ASC
        LIMIT ?
    """
    params.append(limit)
    return conn.execute(sql, params).fetchall()

def list_recent(conn, company: str | None, location: str | None = None, limit: int = 2000):
    where = []
    params: list[Any] = []

    if company:
        where.append("company = ?")
        params.append(company)

    if location and location != "(Any)":
        where.append("location = ?")
        params.append(location)

    where_sql = f"WHERE {' AND '.join(where)}" if where else ""

    sql = f"""
        SELECT title, url, location, first_seen, last_seen
        FROM jobs
        {where_sql}
        ORDER BY last_seen DESC, title ASC
        LIMIT ?
    """
    params.append(limit)
    return conn.execute(sql, params).fetchall()

    
def list_locations(conn, company: str | None):
    cols = [r[1] for r in conn.execute("PRAGMA table_info(jobs)").fetchall()]
    if "location" not in cols:
        return []

    if company:
        cur = conn.execute("""
            SELECT DISTINCT location
            FROM jobs
            WHERE company = ?
              AND location IS NOT NULL AND TRIM(location) <> ''
            ORDER BY location
        """, (company,))
    else:
        cur = conn.execute("""
            SELECT DISTINCT location
            FROM jobs
            WHERE location IS NOT NULL AND TRIM(location) <> ''
            ORDER BY location
        """)
    return [r[0] for r in cur.fetchall()]


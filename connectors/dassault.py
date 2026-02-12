from __future__ import annotations

import hashlib
import re
import time
from dataclasses import dataclass
from typing import Dict, Iterable, List, Optional, Set
from urllib.parse import urlparse

import requests
from bs4 import BeautifulSoup
from xml.etree import ElementTree as ET

from utils.location import normalize_location


@dataclass(frozen=True)
class Job:
    company: str
    job_id: str
    title: str
    url: str
    location: Optional[str] = None
    description: Optional[str] = None


COMPANY = "Dassault Systemes"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (compatible; RoleRadar/1.0)",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
}

TIMEOUT = 30

SITEMAP_INDEX = "https://www.3ds.com/sitemap/sitemap.xml"

# Matches job detail URL paths like:
#   /careers/jobs/senior-director-enterprise-agreements-546412
#   /fr/careers/jobs/...-546412
JOB_PATH_RE = re.compile(r"^/(?:[a-z]{2}/)?careers/jobs/[^?#/]+-\d+/?$", re.IGNORECASE)

LOCATION_RE = re.compile(r"\bLocation:\s*([^\n\r]+)", re.IGNORECASE)
REFID_RE = re.compile(r"\bRef\s*ID:\s*([A-Za-z0-9_-]+)", re.IGNORECASE)
NUMERIC_ID_RE = re.compile(r"-(\d+)(?:/)?$")


def _get(url: str, session: requests.Session) -> str:
    r = session.get(url, headers=HEADERS, timeout=TIMEOUT)
    r.raise_for_status()
    return r.text


def _is_job_url(url: str) -> bool:
    try:
        p = urlparse(url)
        if not p.scheme.startswith("http"):
            return False
        host = (p.netloc or "").lower()
        if not host.endswith("3ds.com"):
            return False
        # strip query/fragments for path match
        path = p.path or ""
        return bool(JOB_PATH_RE.match(path))
    except Exception:
        return False


def _iter_sitemap_locs(xml_text: str) -> Iterable[str]:
    """Yield <loc> values from a sitemap (either urlset or sitemapindex)."""
    root = ET.fromstring(xml_text)

    # Namespace-agnostic tag matching
    def _tag_name(tag: str) -> str:
        return tag.split("}")[-1] if "}" in tag else tag

    for el in root.iter():
        if _tag_name(el.tag) == "loc" and el.text:
            yield el.text.strip()


def _collect_job_urls_from_sitemaps(
    session: requests.Session,
    max_sitemaps: Optional[int] = None,
    sleep_s: float = 0.05,
) -> List[str]:
    """Follow sitemap index -> nested sitemaps; return unique job URLs."""
    to_visit = [SITEMAP_INDEX]
    visited: Set[str] = set()
    job_urls: List[str] = []
    seen_jobs: Set[str] = set()

    while to_visit and (max_sitemaps is None or len(visited) < max_sitemaps):
        sm_url = to_visit.pop(0)
        if sm_url in visited:
            continue
        visited.add(sm_url)

        try:
            xml_text = _get(sm_url, session)
        except Exception:
            continue

        # Each <loc> can be either a nested sitemap or a concrete URL.
        for loc in _iter_sitemap_locs(xml_text):
            if loc.endswith(".xml") and "/sitemap/" in loc:
                if loc not in visited:
                    to_visit.append(loc)
            elif _is_job_url(loc):
                if loc not in seen_jobs:
                    seen_jobs.add(loc)
                    job_urls.append(loc)

        time.sleep(max(0.0, sleep_s))

    return job_urls


def _normalize_ds_location(loc_raw: str) -> Optional[str]:
    if not loc_raw:
        return None

    parts = [p.strip() for p in loc_raw.split(",") if p.strip()]
    if not parts:
        return None

    country_raw = parts[0]
    country_l = country_raw.lower()

    if country_l in {"united states", "us", "usa", "united states of america"}:
        country = "US"
        state = parts[1] if len(parts) >= 2 else ""
        city = parts[2] if len(parts) >= 3 else ""
        return normalize_location(country, state, city)

    country = country_raw

    if len(parts) >= 3:
        state = parts[1]
        city = parts[2]
        return normalize_location(country, state, city)

    city = parts[1] if len(parts) >= 2 else ""
    return normalize_location(country, "", city)


def _parse_job_detail(html: str, url: str) -> Optional[Job]:
    soup = BeautifulSoup(html, "html.parser")

    h1 = soup.find("h1")
    title = h1.get_text(strip=True) if h1 else ""
    if not title:
        t = soup.find("title")
        title = t.get_text(strip=True) if t else ""
    title = (title or "").strip()
    if not title:
        return None

    text = soup.get_text("\n", strip=True)

    ref_id = None
    m = REFID_RE.search(text)
    if m:
        ref_id = m.group(1).strip()

    loc_raw = None
    m = LOCATION_RE.search(text)
    if m:
        loc_raw = m.group(1).strip()

    location_key = _normalize_ds_location(loc_raw or "")

    url_id = None
    m = NUMERIC_ID_RE.search(urlparse(url).path or "")
    if m:
        url_id = m.group(1)

    stable_id = ref_id or url_id or hashlib.sha256(url.encode()).hexdigest()[:16]
    job_id = f"{COMPANY}:{stable_id}"

    return Job(company=COMPANY, job_id=job_id, title=title, url=url, location=location_key)


def scrape_dassault(
    *,
    max_jobs: Optional[int] = None,
    max_sitemaps: Optional[int] = None,
    sleep_s: float = 0.05,
) -> List[Job]:
    """Scrape Dassault Systèmes jobs using sitemap discovery (works when listings are JS-rendered)."""

    with requests.Session() as session:
        job_urls = _collect_job_urls_from_sitemaps(session, max_sitemaps=max_sitemaps, sleep_s=sleep_s)

        jobs: Dict[str, Job] = {}
        urls_to_fetch = job_urls if max_jobs is None else job_urls[:max_jobs]
        for url in urls_to_fetch:
            try:
                html = _get(url, session)
            except Exception:
                continue

            job = _parse_job_detail(html, url)
            if job:
                jobs[job.job_id] = job

            time.sleep(max(0.0, sleep_s))

    return list(jobs.values())


def scrape_dassault_dicts(**kwargs) -> List[dict]:
    return [
        {
            "company": j.company,
            "job_id": j.job_id,
            "title": j.title,
            "url": j.url,
            "location": j.location or "",
            "description": j.description or "",
        }
        for j in scrape_dassault(**kwargs)
    ]

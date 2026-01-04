from __future__ import annotations

import hashlib
from dataclasses import dataclass
from typing import List
from utils.location import normalize_location

import requests
import feedparser


@dataclass(frozen=True)
class Job:
    company: str
    job_id: str
    title: str
    url: str
    location: str | None = None


RSS_URL = "https://www.mathworks.com/company/jobs/opportunities/rss.xml"

def extract_location_from_entry(entry) -> str | None:
    """
    MathWorks RSS entries include structured location fields.
    Prefer locationname; otherwise build from city/state/country.
    """
    loc = (entry.get("locationname") or "").strip()
    if loc:
        return loc  # e.g., "US-MA-Natick"

    city = (entry.get("city") or "").strip()
    state = (entry.get("state") or "").strip()
    country = (entry.get("country") or "").strip()

    return normalize_location(country, state, city)

def scrape_mathworks() -> List[Job]:
    # Some servers return odd responses without browser-like headers
    headers = {
        "User-Agent": "Mozilla/5.0 (compatible; RoleRadar/1.0)",
        "Accept": "application/rss+xml, application/xml;q=0.9, text/xml;q=0.8, */*;q=0.5",
    }

    resp = requests.get(RSS_URL, headers=headers, timeout=30)
    resp.raise_for_status()

    feed = feedparser.parse(resp.content)

    jobs: list[Job] = []
    for entry in feed.entries:
        title = (entry.get("title") or "").strip()
        url = entry.get("link") or ""
        if not title or not url:
            continue

        stable_id = hashlib.sha256(url.encode("utf-8")).hexdigest()[:16]
        location = extract_location_from_entry(entry)
        jobs.append(Job(company="MathWorks", job_id=f"MathWorks:{stable_id}", title=title, url=url, location=location))

    return jobs

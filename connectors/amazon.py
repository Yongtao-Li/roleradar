from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from typing import List, Optional
from urllib.parse import urlencode
from utils.location import normalize_location

import requests

BASE = "https://www.amazon.jobs"
SEARCH_JSON = f"{BASE}/search.json"


@dataclass(frozen=True)
class Job:
    company: str
    job_id: str
    title: str
    url: str
    location: Optional[str] = None


def _request(params: dict) -> dict:
    headers = {
        "User-Agent": "Mozilla/5.0 (compatible; RoleRadar/1.0)",
        "Accept": "application/json",
    }
    r = requests.get(SEARCH_JSON, params=params, headers=headers, timeout=30)
    r.raise_for_status()
    return r.json()

def _extract_amazon_location(j: dict) -> Optional[str]:
    """
    Final, robust Amazon location extractor.

    Handles:
    - locations as list of dicts
    - locations as list of JSON strings
    - ignores building codes and internal IDs
    - outputs canonical COUNTRY-STATE-CITY
    """

    locs = j.get("locations")

    if isinstance(locs, list) and locs:
        first = locs[0]

        # Case 1: JSON string → dict
        if isinstance(first, str):
            try:
                first = json.loads(first)
            except Exception:
                first = None

        # Case 2: dict (after decoding or originally)
        if isinstance(first, dict):
            city = (
                first.get("normalizedCityName")
                or first.get("city")
            )

            state = (
                first.get("region")
                or first.get("normalizedStateName")
            )

            country = (
                first.get("countryIso2a")
                or first.get("normalizedCountryCode")
            )

            return normalize_location(country, state, city)

    # Fallback (very rare)
    return normalize_location(
        country=j.get("country_code"),
        state=j.get("state"),
        city=j.get("city"),
    )

def scrape_amazon(
    base_query: str = "",
    normalized_country_code: str = "USA",
    sort: str = "recent",
    result_limit: int = 50,
    max_pages: Optional[int] = None,
) -> List[Job]:
    """
    Pull jobs from Amazon's search.json and paginate using offset/result_limit.
    By default this runs a full sync (no fixed page cap) and stops when
    the API returns no jobs, when reported hits are exhausted, or when
    pagination appears stuck (multiple pages add no new unique jobs).
    The endpoint returns `jobs` items containing `job_path` that can be joined with https://www.amazon.jobs.
    :contentReference[oaicite:1]{index=1}
    """
    all_jobs: dict[str, Job] = {}
    offset = 0

    # facets are optional, but useful if you later want filters
    facets = [
        "normalized_country_code",
        "normalized_state_name",
        "normalized_city_name",
        "business_category",
        "category",
        "job_function_id",
    ]

    pages_fetched = 0
    consecutive_no_new = 0

    while True:
        if max_pages is not None and pages_fetched >= max_pages:
            break

        params = {
            "base_query": base_query,
            "offset": offset,
            "result_limit": result_limit,
            "sort": sort,
            "normalized_country_code[]": normalized_country_code,
        }
        # Amazon expects repeated `facets[]`
        for f in facets:
            params.setdefault("facets[]", [])
            params["facets[]"].append(f)

        data = _request(params)

        jobs = data.get("jobs", [])
        if not jobs:
            break

        before_count = len(all_jobs)

        for j in jobs:
            title = (j.get("title") or "").strip()
            job_path = j.get("job_path") or ""
            if not title or not job_path:
                continue

            url = BASE + job_path  # per Amazon response examples :contentReference[oaicite:2]{index=2}

            # Prefer job_id from payload when present; otherwise hash URL
            stable = str(j.get("id") or j.get("job_id") or hashlib.sha256(url.encode()).hexdigest()[:16])
            job_id = f"Amazon:{stable}"

            location = _extract_amazon_location(j)

            all_jobs[job_id] = Job(
                company="Amazon",
                job_id=job_id,
                title=title,
                url=url,
                location=location,
            )

        pages_fetched += 1

        if len(all_jobs) == before_count:
            consecutive_no_new += 1
        else:
            consecutive_no_new = 0

        if consecutive_no_new >= 3:
            break

        # Pagination: increment offset by jobs actually returned
        offset += len(jobs)

        # Optional: stop early if we’ve already collected everything
        hits = data.get("hits")
        if isinstance(hits, int) and offset >= hits:
            break

    return list(all_jobs.values())

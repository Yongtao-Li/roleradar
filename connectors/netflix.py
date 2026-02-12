"""
Netflix connector for RoleRadar (FIXED pagination).

Source site:
  https://explore.jobs.netflix.net/careers

Key findings on Netflix's deployment (as of your debug runs):
- `offset` is ignored (returns the same first page).
- `start` works for pagination.
- The API often caps the number of jobs returned per request (e.g., 10),
  even if you request a larger `limit`.
  => Therefore we must advance `start` by the ACTUAL number of jobs returned,
     not by the requested `page_size`.

This connector:
- Uses `start` pagination
- Includes required scoping params: domain, microsite, query="*"
- Uses API-reported `count` when present, but still robust if absent
- Defensive parsing across possible field-name variations
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Sequence
import time
import logging

import requests
from requests import Response


logger = logging.getLogger(__name__)


# ---- RoleRadar Job model (keep compatible with your app) ----
@dataclass
class Job:
    company: str
    job_id: str
    title: str
    location: str
    url: str
    description: Optional[str] = None


class NetflixConnector:
    COMPANY = "Netflix"
    BASE_URL = "https://explore.jobs.netflix.net"
    CAREERS_URL = f"{BASE_URL}/careers"
    API_JOBS_URL = f"{BASE_URL}/api/apply/v2/jobs"

    DEFAULT_DOMAIN = "netflix.com"
    DEFAULT_MICROSITE = "netflix.com"

    DEFAULT_HEADERS = {
        "Accept": "application/json",
        "User-Agent": "RoleRadar/1.0",
    }

    def __init__(
        self,
        session: Optional[requests.Session] = None,
        timeout_s: int = 25,
        max_retries: int = 3,
        backoff_s: float = 0.8,
        headers: Optional[Dict[str, str]] = None,
        log_level: int = logging.INFO,
    ) -> None:
        self.session = session or requests.Session()
        self.timeout_s = timeout_s
        self.max_retries = max_retries
        self.backoff_s = backoff_s
        self.headers = dict(self.DEFAULT_HEADERS)
        if headers:
            self.headers.update(headers)

        logger.setLevel(log_level)

    # ---------------- Public API ----------------
    def fetch_jobs(
        self,
        page_size: int = 100,
        max_pages: Optional[int] = None,
        keywords: Optional[Sequence[str]] = None,
        location_contains: Optional[Sequence[str]] = None,
    ) -> List[Job]:
        """
        Fetch jobs and return as a list of normalized Job objects.

        Args:
            page_size: value sent to API as `limit`. NOTE: API may cap actual returned jobs.
            max_pages: optional safety cap for pagination. `None` means full sync.
            keywords: optional list of keyword strings. If provided, jobs are filtered
                      client-side by title/description/location containing ANY keyword (case-insensitive).
            location_contains: optional list; keep jobs whose location contains ANY of these substrings.
        """
        postings: List[Job] = []

        start = 0
        total: Optional[int] = None
        page = 0

        seen_ids: set[str] = set()
        repeat_sig_count = 0
        last_sig: Optional[tuple[str, ...]] = None

        while True:
            if max_pages is not None and page >= max_pages:
                logger.info("Reached max_pages=%s; stopping.", max_pages)
                break

            data = self._get_jobs_page(limit=page_size, start=start)

            raw_jobs = self._extract_raw_jobs(data)
            if total is None:
                total = self._extract_total(data)

            if not raw_jobs:
                logger.info("Empty page returned at start=%s; stopping.", start)
                break

            # signature-based guard against stuck pagination
            sig = self._page_signature(raw_jobs, n=min(10, len(raw_jobs)))
            if last_sig is not None and sig == last_sig:
                repeat_sig_count += 1
            else:
                repeat_sig_count = 0
            last_sig = sig

            if repeat_sig_count >= 3:
                logger.warning("Page signature repeated 3x in a row; pagination likely stuck. Stopping.")
                break

            # parse & append, de-dup by job_id
            added_this_page = 0
            for raw in raw_jobs:
                job = self._parse_job(raw)
                if not job:
                    continue
                if job.job_id in seen_ids:
                    continue
                seen_ids.add(job.job_id)
                postings.append(job)
                added_this_page += 1

            # Advance start by ACTUAL number of jobs returned (critical fix)
            start += len(raw_jobs)
            page += 1

            logger.debug(
                "Netflix page=%s start(next)=%s returned=%s added_unique=%s total_unique=%s total_reported=%s",
                page, start, len(raw_jobs), added_this_page, len(postings), total
            )

            if isinstance(total, int) and len(postings) >= total:
                logger.info("Reached reported total count=%s; stopping.", total)
                break

        # Client-side filters (kept simple & robust)
        if keywords:
            kw = [k.strip().lower() for k in keywords if k and k.strip()]
            if kw:
                postings = [j for j in postings if self._matches_any_keyword(j, kw)]

        if location_contains:
            loc_terms = [t.strip().lower() for t in location_contains if t and t.strip()]
            if loc_terms:
                postings = [j for j in postings if any(term in (j.location or "").lower() for term in loc_terms)]

        return postings

    # ---------------- Internal helpers ----------------
    def _get_jobs_page(self, *, limit: int, start: int) -> Dict[str, Any]:
        params: Dict[str, Any] = {
            # Pagination (Netflix honors `start`)
            "limit": int(limit),
            "start": int(start),

            # Critical scoping
            "domain": self.DEFAULT_DOMAIN,
            "microsite": self.DEFAULT_MICROSITE,

            # Critical: force "search results" mode (otherwise you may get config JSON)
            "query": "*",

            # Sorting hints (safe if ignored)
            "sortBy": "relevance",
            "ascending": "false",
        }

        resp = self._request_with_retries("GET", self.API_JOBS_URL, params=params)
        return self._safe_json(resp)

    @staticmethod
    def _extract_raw_jobs(data: Dict[str, Any]) -> List[Dict[str, Any]]:
        raw = data.get("jobs") or data.get("positions") or data.get("results") or data.get("data") or []
        return [j for j in raw if isinstance(j, dict)] if isinstance(raw, list) else []

    @staticmethod
    def _extract_total(data: Dict[str, Any]) -> Optional[int]:
        for k in ("count", "total", "totalCount", "total_count"):
            v = data.get(k)
            if isinstance(v, int):
                return v
            if isinstance(v, str) and v.isdigit():
                return int(v)
        return None

    @staticmethod
    def _page_signature(raw_jobs: List[Dict[str, Any]], n: int = 10) -> tuple[str, ...]:
        ids: List[str] = []
        for j in raw_jobs[:n]:
            jid = j.get("id") or j.get("jobId") or j.get("positionId") or ""
            ids.append(str(jid))
        return tuple(ids)

    def _request_with_retries(self, method: str, url: str, **kwargs: Any) -> Response:
        last_err: Optional[Exception] = None
        for attempt in range(1, self.max_retries + 1):
            try:
                resp = self.session.request(
                    method,
                    url,
                    headers=self.headers,
                    timeout=self.timeout_s,
                    **kwargs,
                )
                # Retry on transient 5xx and some 429s
                if resp.status_code >= 500 or resp.status_code == 429:
                    raise requests.HTTPError(f"HTTP {resp.status_code}", response=resp)
                resp.raise_for_status()
                return resp
            except Exception as e:
                last_err = e
                if attempt < self.max_retries:
                    time.sleep(self.backoff_s * attempt)
                else:
                    raise
        raise RuntimeError("Request failed") from last_err

    @staticmethod
    def _safe_json(resp: Response) -> Dict[str, Any]:
        try:
            return resp.json()  # type: ignore[no-any-return]
        except Exception as e:
            snippet = (resp.text or "")[:500]
            raise ValueError(f"Expected JSON response. Got: {snippet!r}") from e

    def _parse_job(self, raw: Dict[str, Any]) -> Optional[Job]:
        job_id = raw.get("id") or raw.get("jobId") or raw.get("positionId")
        if job_id is None:
            return None
        job_id_str = str(job_id).strip()

        title = (raw.get("title") or raw.get("name") or "").strip()
        if not title:
            return None

        location = self._extract_location(raw)

        url = (
            raw.get("canonicalPositionUrl")
            or raw.get("url")
            or raw.get("applyUrl")
            or f"{self.BASE_URL}/jobs/{job_id_str}"
        )

        description = raw.get("description") or raw.get("jobDescription") or raw.get("descriptionText")

        return Job(
            company=self.COMPANY,
            job_id=f"{self.COMPANY}:{job_id_str}",
            title=title,
            location=location,
            url=url,
            description=description,
        )

    @staticmethod
    def _extract_location(raw: Dict[str, Any]) -> str:
        loc = raw.get("locations") or raw.get("location") or raw.get("jobLocation") or raw.get("locationName")
        if isinstance(loc, str):
            return loc.strip() or "Unspecified"
        if isinstance(loc, list):
            parts: List[str] = []
            for item in loc:
                if isinstance(item, str):
                    s = item.strip()
                    if s:
                        parts.append(s)
                elif isinstance(item, dict):
                    s = (
                        item.get("name")
                        or item.get("label")
                        or item.get("location")
                        or item.get("city")
                        or ""
                    )
                    s = str(s).strip()
                    if s:
                        parts.append(s)
            return ", ".join(dict.fromkeys(parts)) if parts else "Unspecified"
        if isinstance(loc, dict):
            s = loc.get("name") or loc.get("label") or loc.get("location") or ""
            s = str(s).strip()
            return s or "Unspecified"
        return "Unspecified"

    @staticmethod
    def _matches_any_keyword(job: Job, keywords_lc: Sequence[str]) -> bool:
        hay = " ".join([job.title or "", job.location or "", job.description or ""]).lower()
        return any(k in hay for k in keywords_lc)


def scrape_netflix(
    *,
    page_size: int = 100,
    max_pages: Optional[int] = None,
    keywords: Optional[Sequence[str]] = None,
    location_contains: Optional[Sequence[str]] = None,
) -> List[Job]:
    return NetflixConnector().fetch_jobs(
        page_size=page_size,
        max_pages=max_pages,
        keywords=keywords,
        location_contains=location_contains,
    )

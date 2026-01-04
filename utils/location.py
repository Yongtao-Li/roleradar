from __future__ import annotations

import re
from typing import Optional

def _clean_city(city: str) -> str:
    city = city.strip()
    if not city:
        return ""
    # Title case words, keep hyphens, replace spaces with underscores for key stability
    city = re.sub(r"\s+", " ", city)
    city = city.title()
    city = city.replace(" ", "_")
    return city

def normalize_location(country: Optional[str], state: Optional[str], city: Optional[str]) -> Optional[str]:
    """
    Canonical key: COUNTRY-STATE-CITY
    - country/state uppercased
    - city Title Case, spaces->underscores
    Returns None if everything missing.
    """
    c = (country or "").strip().upper()
    s = (state or "").strip().upper()
    ci = _clean_city(city or "")

    if not (c or s or ci):
        return None

    # allow blank state if not available
    return f"{c}-{s}-{ci}"

def display_location(key: Optional[str]) -> str:
    if not key:
        return "—"
    parts = key.split("-", 2)
    if len(parts) != 3:
        return key
    country, state, city = parts
    city_disp = city.replace("_", " ")
    if state:
        return f"{city_disp}, {state}, {country}"
    return f"{city_disp}, {country}"

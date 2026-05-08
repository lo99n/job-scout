"
SUPABASE PROFILE LOADER
=========================
Reads profiles from Supabase and returns the same dict structure
as profiles.json, so the rest of the scraper doesn't change.

Usage:
    from supabase_profiles import load_profiles
    profiles = load_profiles()
    # profiles["global_filters"] → same as before
    # profiles["friends"] → list of friend dicts from Supabase
"""

import os
import json
import logging
import requests

log = logging.getLogger("supabase_profiles")

SUPABASE_URL = os.getenv("SUPABASE_URL", "")
SUPABASE_KEY = os.getenv("SUPABASE_KEY", "")

# Global filters stay hardcoded — they apply to all users
# and don't need to be in the database
GLOBAL_FILTERS = {
    "languages_exclude": [
        "German C1", "German C2", "fließend Deutsch",
        "Deutsch Muttersprache", "verhandlungssicher Deutsch"
    ],
    "roles_exclude": [
        "software engineer", "developer", "data engineer",
        "devops", "sysadmin", "nurse", "doctor", "lawyer", "teacher"
    ],
    "location_include": [
        "Germany", "Berlin", "Munich", "Frankfurt", "Hamburg",
        "Stuttgart", "Düsseldorf", "Cologne", "Amsterdam", "London",
        "Paris", "Barcelona", "Madrid", "Milan", "Zurich", "Vienna",
        "Dublin", "Lisbon", "Europe", "Remote"
    ],
    "seniority_exclude": ["director", "VP", "C-level", "head of", "principal"],
    "min_english": True,
    "exclude_unpaid": True,
}


def load_profiles() -> dict:
    """
    Load profiles from Supabase. Returns same structure as profiles.json:
    {
        "global_filters": {...},
        "friends": [...]
    }
    Falls back to profiles.json if Supabase is unavailable.
    """
    if not SUPABASE_URL or not SUPABASE_KEY:
        log.warning("Supabase not configured. Falling back to profiles.json")
        return _load_from_file()

    try:
        response = requests.get(
            f"{SUPABASE_URL}/rest/v1/profiles",
            headers={
                "apikey": SUPABASE_KEY,
                "Authorization": f"Bearer {SUPABASE_KEY}",
                "Content-Type": "application/json",
            },
            params={
                "active": "eq.true",
                "select": "*",
            },
            timeout=10,
        )

        if response.status_code != 200:
            log.error(f"Supabase returned {response.status_code}: {response.text[:200]}")
            return _load_from_file()

        rows = response.json()

        if not rows:
            log.warning("No active profiles in Supabase. Falling back to profiles.json")
            return _load_from_file()

        friends = []
        for row in rows:
            friends.append({
                "id": row["profile_key"],
                "name": row["name"],
                "email": row["email"],
                "target_roles": row.get("target_roles") or [],
                "keywords": row.get("keywords") or [],
                "preferred_locations": row.get("preferred_locations") or [],
                "accepted_locations": row.get("accepted_locations") or [],
                "company_types": row.get("company_types") or [],
                "seniority": row.get("seniority") or [],
                "min_salary": row.get("min_salary"),
                "target_salary": row.get("target_salary"),
                "languages": row.get("languages") or [],
                "bonus_keywords": row.get("bonus_keywords") or [],
            })

        log.info(f"Loaded {len(friends)} active profiles from Supabase")

        return {
            "global_filters": GLOBAL_FILTERS,
            "friends": friends,
        }

    except Exception as e:
        log.error(f"Supabase connection failed: {e}")
        return _load_from_file()


def _load_from_file() -> dict:
    """Fallback: load from profiles.json."""
    try:
        profiles_path = os.path.join(os.path.dirname(__file__), "profiles.json")
        with open(profiles_path) as f:
            log.info("Loaded profiles from profiles.json (fallback)")
            return json.load(f)
    except FileNotFoundError:
        log.error("profiles.json not found either. No profiles available.")
        return {"global_filters": GLOBAL_FILTERS, "friends": []}

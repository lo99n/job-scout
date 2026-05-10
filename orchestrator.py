"""
AI ORCHESTRATOR
================
Runs BEFORE the scraper to generate smart search terms and ATS seed companies.
Runs AFTER matching to backfill profiles that got fewer than 4 matches.

Flow:
  1. Read all profiles from Supabase
  2. Call Claude to generate targeted search terms + ATS seed companies
  3. Write strategy to /app/scraper/ai_strategy.json
  4. Scraper reads strategy instead of using static term generation
  5. After matching, check counts per profile
  6. For profiles under 4: generate targeted company list, run ATS-only, re-match
  7. Max 2 backfill loops

Usage:
  Called from main.py as step 0 (before scraper) and step 2.5 (after matching)
"""

import os
import json
import time
import logging
import requests

log = logging.getLogger("orchestrator")

ANTHROPIC_API_KEY = os.getenv("ORCHESTRATOR_API_KEY", os.getenv("ANTHROPIC_API_KEY", ""))
MODEL = "claude-sonnet-4-5"
API_URL = "https://api.anthropic.com/v1/messages"
STRATEGY_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "scraper", "ai_strategy.json")
MIN_MATCHES = 4
MAX_BACKFILL_LOOPS = 2


def generate_search_strategy(profiles: list[dict]) -> dict:
    """
    Call Claude to analyze all profiles and generate:
    - Targeted board search terms per profile
    - Shared search terms (overlap across profiles)
    - ATS seed companies per profile
    
    Returns strategy dict written to ai_strategy.json
    """
    if not ANTHROPIC_API_KEY:
        log.warning("No ANTHROPIC_API_KEY. Skipping AI orchestration.")
        return None

    # Build profile summaries
    profile_summaries = []
    for p in profiles:
        cv = p.get("cv_parsed") or {}
        summary = {
            "name": p["name"],
            "profile_key": p.get("profile_key", p.get("id", "")),
            "target_roles": p.get("target_roles", []),
            "keywords": p.get("keywords", []),
            "preferred_locations": p.get("preferred_locations", []),
            "accepted_locations": p.get("accepted_locations", []),
            "company_types": p.get("company_types", []),
            "seniority": p.get("seniority", []),
            "languages": p.get("languages", []),
            "bonus_keywords": p.get("bonus_keywords", []),
        }
        if cv.get("skills"):
            summary["cv_skills"] = cv["skills"]
        if cv.get("industries"):
            summary["cv_industries"] = cv["industries"]
        if cv.get("current_title"):
            summary["cv_current_title"] = cv["current_title"]
        profile_summaries.append(summary)

    prompt = f"""You are a job search strategist. I have {len(profiles)} job seekers. I need to find 5 quality job matches for each of them every day.

I scrape from two sources:
1. JOB BOARDS (Arbeitnow, RemoteOK) — these use keyword search terms
2. ATS SYSTEMS (Greenhouse, Lever, Ashby) — these scrape specific company career pages

PROFILES:
{json.dumps(profile_summaries, indent=2)}

Generate a search strategy. Return a JSON object with this structure:
{{
  "board_search_terms": {{
    "shared": ["term1", "term2"],
    "per_profile": {{
      "profile_key": ["specific term1", "specific term2"]
    }}
  }},
  "ats_seed_companies": {{
    "per_profile": {{
      "profile_key": ["Company Name 1", "Company Name 2"]
    }}
  }},
  "reasoning": "Brief explanation of the strategy"
}}

Rules:
- Board search terms should be 2-4 words max (API query format)
- Generate 10-15 shared terms that cast a wide net
- Generate 5-10 specific terms per profile that target their niche
- ATS seed companies: suggest 5-10 real European companies per profile that are likely to have roles matching their profile and that use Greenhouse, Lever, or Ashby
- Focus on companies in their preferred locations
- Consider the candidate's actual experience level and background, not aspirational roles
- IMPORTANT: None of these candidates speak fluent German. Avoid German-only companies.

Return ONLY the JSON object. No markdown, no backticks."""

    try:
        response = requests.post(
            API_URL,
            headers={
                "Content-Type": "application/json",
                "x-api-key": ANTHROPIC_API_KEY,
                "anthropic-version": "2023-06-01",
            },
            json={
                "model": MODEL,
                "max_tokens": 3000,
                "messages": [{"role": "user", "content": prompt}],
            },
            timeout=60,
        )

        if response.status_code != 200:
            log.error(f"Strategy API error {response.status_code}: {response.text[:300]}")
            return None

        data = response.json()
        text = ""
        for block in data.get("content", []):
            if block.get("type") == "text":
                text += block.get("text", "")

        text = text.strip().strip("`").strip()
        if text.startswith("json"):
            text = text[4:].strip()

        strategy = json.loads(text)

        # Write strategy to file
        os.makedirs(os.path.dirname(STRATEGY_FILE), exist_ok=True)
        with open(STRATEGY_FILE, "w") as f:
            json.dump(strategy, f, indent=2)

        log.info(f"AI Strategy generated:")
        log.info(f"  Shared terms: {len(strategy.get('board_search_terms', {}).get('shared', []))}")
        per_profile = strategy.get("board_search_terms", {}).get("per_profile", {})
        for key, terms in per_profile.items():
            log.info(f"  {key}: {len(terms)} specific terms")
        ats_seeds = strategy.get("ats_seed_companies", {}).get("per_profile", {})
        for key, companies in ats_seeds.items():
            log.info(f"  {key}: {len(companies)} ATS seed companies")
        log.info(f"  Reasoning: {strategy.get('reasoning', 'N/A')}")

        return strategy

    except json.JSONDecodeError as e:
        log.error(f"Failed to parse strategy response: {e}")
        return None
    except Exception as e:
        log.error(f"Strategy generation failed: {e}")
        return None


def generate_backfill_companies(profile: dict, current_count: int, already_tried: list[str] = None) -> list[str]:
    """
    For a profile that got fewer than MIN_MATCHES, generate targeted company names
    to search via ATS discovery.
    
    Returns list of company names to try.
    """
    if not ANTHROPIC_API_KEY:
        return []

    already_tried = already_tried or []
    needed = 5 - current_count

    cv = profile.get("cv_parsed") or {}
    
    prompt = f"""A job seeker needs {needed} more job matches. The broad search only found {current_count} good matches for them today.

CANDIDATE:
Name: {profile['name']}
Target roles: {', '.join(profile.get('target_roles', []))}
Keywords: {', '.join(profile.get('keywords', []))}
Preferred locations: {', '.join(profile.get('preferred_locations', []))}
Also accepts: {', '.join(profile.get('accepted_locations', []))}
Company types: {', '.join(profile.get('company_types', []))}
Seniority: {', '.join(profile.get('seniority', []))}
CV skills: {', '.join(cv.get('skills', []))}
CV industries: {', '.join(cv.get('industries', []))}
Current/last title: {cv.get('current_title', 'N/A')}

Companies already searched (skip these): {', '.join(already_tried) if already_tried else 'None'}

Suggest 15 specific real companies in Europe that:
1. Are likely hiring for roles matching this profile RIGHT NOW
2. Use Greenhouse, Lever, or Ashby as their ATS
3. Are in or near their preferred locations
4. Post jobs in English (candidate doesn't speak fluent German)
5. Are at the right stage/size for the seniority level they're targeting

Return ONLY a JSON array of company names. No markdown, no explanation.
Example: ["Personio", "Celonis", "N26"]"""

    try:
        response = requests.post(
            API_URL,
            headers={
                "Content-Type": "application/json",
                "x-api-key": ANTHROPIC_API_KEY,
                "anthropic-version": "2023-06-01",
            },
            json={
                "model": MODEL,
                "max_tokens": 500,
                "messages": [{"role": "user", "content": prompt}],
            },
            timeout=30,
        )

        if response.status_code != 200:
            log.error(f"Backfill API error {response.status_code}: {response.text[:200]}")
            return []

        data = response.json()
        text = ""
        for block in data.get("content", []):
            if block.get("type") == "text":
                text += block.get("text", "")

        text = text.strip().strip("`").strip()
        if text.startswith("json"):
            text = text[4:].strip()

        companies = json.loads(text)
        log.info(f"  Backfill for {profile['name']}: {len(companies)} targeted companies")
        return companies

    except Exception as e:
        log.error(f"Backfill generation failed for {profile['name']}: {e}")
        return []


def load_strategy() -> dict:
    """Load the AI strategy from file (called by scraper)."""
    if os.path.exists(STRATEGY_FILE):
        try:
            with open(STRATEGY_FILE, "r") as f:
                return json.load(f)
        except (json.JSONDecodeError, IOError):
            pass
    return None


def get_search_terms_from_strategy(strategy: dict, profiles: list[dict]) -> list[str]:
    """
    Extract a flat list of search terms from the AI strategy.
    Combines shared terms + all per-profile terms, deduplicated.
    """
    if not strategy:
        return []

    terms = set()
    
    board = strategy.get("board_search_terms", {})
    for term in board.get("shared", []):
        terms.add(term.lower().strip())
    
    for key, profile_terms in board.get("per_profile", {}).items():
        for term in profile_terms:
            terms.add(term.lower().strip())

    return sorted(terms)


def get_ats_seeds_from_strategy(strategy: dict, profile_key: str = None) -> list[str]:
    """
    Get ATS seed companies from strategy.
    If profile_key given, return only that profile's seeds.
    Otherwise return all unique companies.
    """
    if not strategy:
        return []

    ats = strategy.get("ats_seed_companies", {}).get("per_profile", {})
    
    if profile_key:
        return ats.get(profile_key, [])
    
    # All unique companies
    all_companies = set()
    for companies in ats.values():
        all_companies.update(companies)
    return sorted(all_companies)


def check_backfill_needed(match_counts: dict[str, int]) -> dict[str, int]:
    """
    Check which profiles need backfill.
    Returns dict of {profile_key: matches_needed} for profiles under MIN_MATCHES.
    """
    needs_backfill = {}
    for key, count in match_counts.items():
        if count < MIN_MATCHES:
            needs_backfill[key] = 5 - count
            log.info(f"  {key}: {count} matches (needs {5 - count} more)")
    return needs_backfill

"""
ATS INTEGRATION
================
Hooks into the existing scraper pipeline.
After board scraping completes, extracts company names from results,
discovers their ATS, and pulls additional jobs.

Usage (from scraper.py):
    from ats_integration import enrich_with_ats
    board_jobs = [...]  # jobs from arbeitnow, remoteok, etc.
    ats_jobs = enrich_with_ats(board_jobs, seen_urls=set())
    all_jobs = board_jobs + ats_jobs
"""

import os
import re
import logging
from ats_scraper import ATSDiscovery, ATSJob

log = logging.getLogger("ats_integration")

# Cache lives next to the scraper
CACHE_PATH = os.path.join(os.path.dirname(__file__), "ats_cache.json")


def extract_company_names(board_jobs: list[dict]) -> list[str]:
    """
    Extract unique company names from board scraper results.
    Cleans up common noise in company name fields.
    """
    names = set()
    for job in board_jobs:
        company = job.get("company", "").strip()
        if not company:
            continue
        # Skip generic/placeholder names
        if company.lower() in {"confidential", "n/a", "various", "unknown", ""}:
            continue
        # Clean up common suffixes that break slug matching
        clean = re.sub(r"\s*(GmbH|Inc\.?|Ltd\.?|LLC|AG|S\.?A\.?|B\.?V\.?|SE|plc)\s*$", "", company, flags=re.IGNORECASE).strip()
        if clean:
            names.add(clean)
    return sorted(names)


def ats_jobs_to_scraper_format(ats_jobs: list[ATSJob]) -> list[dict]:
    """
    Convert ATSJob objects into the same dict format the scraper uses,
    so they can be scored by the existing matcher.
    """
    results = []
    for job in ats_jobs:
        results.append({
            "title": job.title,
            "company": job.company,
            "location": job.location,
            "url": job.url,
            "description": job.description,
            "source": f"ats:{job.source}",  # e.g. "ats:greenhouse"
            "remote": job.remote,
            "salary_min": None,
            "salary_max": None,
            "posted_date": job.posted_date,
            "tags": [job.department] if job.department else [],
        })
    return results


def enrich_with_ats(board_jobs: list[dict], seen_urls: set = None) -> list[dict]:
    """
    Main integration point.
    Takes jobs from board scrapers, extracts company names,
    discovers ATS boards, pulls all open roles, deduplicates
    against board results, and returns new jobs in scraper format.

    Args:
        board_jobs: list of job dicts from board scrapers
        seen_urls: set of URLs already seen (for dedup)

    Returns:
        list of new job dicts from ATS discovery (scraper format)
    """
    if seen_urls is None:
        seen_urls = set()

    # Collect all existing URLs for dedup
    existing_urls = seen_urls | {j.get("url", "") for j in board_jobs}

    # Extract company names
    companies = extract_company_names(board_jobs)
    log.info(f"\n[ATS Integration] Found {len(companies)} unique companies from board results")

    if not companies:
        return []

    # Discover and scrape ATS
    discovery = ATSDiscovery(cache_path=CACHE_PATH)
    ats_jobs = discovery.discover_and_scrape(companies)

    # Deduplicate against board results
    new_ats_jobs = [j for j in ats_jobs if j.url and j.url not in existing_urls]
    log.info(f"[ATS Integration] {len(ats_jobs)} total ATS jobs, {len(new_ats_jobs)} new after dedup")

    # Convert to scraper format
    return ats_jobs_to_scraper_format(new_ats_jobs)

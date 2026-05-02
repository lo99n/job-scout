"""
ATS INTEGRATION
================
Hooks into the existing scraper pipeline.
After board scraping completes, extracts company names from results,
discovers their ATS, and pulls additional jobs as Job objects.

Usage (from scraper.py):
    from ats_integration import enrich_with_ats
    ats_extra = enrich_with_ats(all_jobs, seen_urls={j.url for j in all_jobs})
    all_jobs.extend(ats_extra)
"""

import os
import re
import uuid
import logging
from datetime import datetime
from ats_scraper import ATSDiscovery, ATSJob

log = logging.getLogger("ats_integration")

CACHE_PATH = os.path.join(os.path.dirname(__file__), "ats_cache.json")


def extract_company_names(board_jobs) -> list[str]:
    """Extract unique company names from Job objects."""
    names = set()
    for job in board_jobs:
        company = job.company.strip() if hasattr(job, "company") else ""
        if not company or company.lower() in {"confidential", "n/a", "various", "unknown"}:
            continue
        clean = re.sub(r"\s*(GmbH|Inc\.?|Ltd\.?|LLC|AG|S\.?A\.?|B\.?V\.?|SE|plc)\s*$", "", company, flags=re.IGNORECASE).strip()
        if clean:
            names.add(clean)
    return sorted(names)


def enrich_with_ats(board_jobs, seen_urls: set = None):
    """
    Takes Job objects from board scrapers, discovers ATS boards,
    pulls all open roles, deduplicates, returns new Job objects.
    """
    from scraper import Job  # import here to avoid circular imports

    if seen_urls is None:
        seen_urls = set()

    existing_urls = set(seen_urls) | {j.url for j in board_jobs if hasattr(j, "url")}

    companies = extract_company_names(board_jobs)
    log.info(f"\n[ATS Integration] Found {len(companies)} unique companies from board results")

    if not companies:
        return []

    discovery = ATSDiscovery(cache_path=CACHE_PATH)
    ats_jobs = discovery.discover_and_scrape(companies)

    new_ats_jobs = [j for j in ats_jobs if j.url and j.url not in existing_urls]
    log.info(f"[ATS Integration] {len(ats_jobs)} total ATS jobs, {len(new_ats_jobs)} new after dedup")

    # Convert ATSJob → Job
    result = []
    for aj in new_ats_jobs:
        result.append(Job(
            id=str(uuid.uuid4())[:8],
            title=aj.title,
            company=aj.company,
            location=aj.location,
            url=aj.url,
            description=aj.description,
            source=f"ats:{aj.source}",
            salary_min=None,
            salary_max=None,
            posted_date=aj.posted_date or None,
            remote=aj.remote,
            tags=[aj.department] if aj.department else [],
            scraped_at=datetime.now().isoformat(),
        ))
    return result

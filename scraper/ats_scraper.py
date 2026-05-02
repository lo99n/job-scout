"""
ATS DISCOVERY SCRAPER
======================
Discovers Greenhouse, Lever, Ashby career pages and pulls open roles.
Includes filters for location, seniority, recency, and language.
"""

import re
import json
import time
import logging
import requests
from dataclasses import dataclass, field, asdict
from typing import Optional
from datetime import datetime, timedelta

logging.basicConfig(level=logging.INFO)
log = logging.getLogger("ats_discovery")

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36",
    "Accept": "application/json, text/html, */*",
}

REQUEST_TIMEOUT = 15
RATE_LIMIT_DELAY = 0.2


# ═══════════════════════════════════════════════════════════════
# JOB FILTERS
# ═══════════════════════════════════════════════════════════════

EUROPE_LOCATIONS = [
    "germany", "berlin", "munich", "münchen", "hamburg", "frankfurt", "düsseldorf",
    "dusseldorf", "cologne", "köln", "stuttgart",
    "netherlands", "amsterdam", "rotterdam", "the hague", "eindhoven", "utrecht",
    "france", "paris", "lyon", "marseille",
    "spain", "madrid", "barcelona", "valencia",
    "italy", "milan", "milano", "rome", "roma",
    "uk", "united kingdom", "london", "manchester", "edinburgh",
    "ireland", "dublin",
    "switzerland", "zurich", "zürich", "geneva", "basel",
    "austria", "vienna", "wien",
    "belgium", "brussels", "bruxelles",
    "portugal", "lisbon", "lisboa", "porto",
    "sweden", "stockholm", "gothenburg",
    "denmark", "copenhagen",
    "norway", "oslo",
    "finland", "helsinki",
    "poland", "warsaw", "krakow", "kraków", "wroclaw",
    "czech", "prague", "praha",
    "luxembourg",
    "europe", "emea", "eu", "dach", "remote",
]

SENIOR_KEYWORDS = [
    "senior", "sr.", "sr ", "lead", "principal", "staff", "head of",
    "director", "vp ", "vice president", "c-level", "chief",
    "manager,", "manager -", "manager –",  # "manager" alone could be mid-level
    "architect",
]

# Titles that indicate mid-level or below (override senior keywords)
MID_OR_BELOW_KEYWORDS = [
    "junior", "jr.", "jr ", "associate", "analyst", "coordinator",
    "specialist", "assistant", "intern", "trainee", "graduate",
    "entry", "werkstudent", "working student",
]

NON_ENGLISH_SIGNALS = [
    # German
    "stellenbeschreibung", "aufgaben", "anforderungen", "wir bieten",
    "deine aufgaben", "dein profil", "was wir bieten", "bewerbung",
    "arbeitsort", "festanstellung", "vollzeit", "teilzeit",
    # French
    "description du poste", "responsabilités", "nous offrons",
    "votre profil", "rejoignez", "candidature", "temps plein",
    # Dutch
    "functieomschrijving", "wat bied je", "wat zoeken wij",
]


def is_europe_or_remote(location: str) -> bool:
    """Check if location is in Europe or remote."""
    if not location:
        return False
    loc = location.lower()
    return any(term in loc for term in EUROPE_LOCATIONS)


def is_mid_level_or_below(title: str) -> bool:
    """Check if title is mid-level or below (not senior/lead/director)."""
    t = title.lower()
    # If it has explicit junior/associate/etc, it's mid or below
    if any(kw in t for kw in MID_OR_BELOW_KEYWORDS):
        return True
    # If it has senior/lead/director/etc, it's above mid
    if any(kw in t for kw in SENIOR_KEYWORDS):
        return False
    # No signal either way → assume mid-level, include it
    return True


def is_recent(posted_date: str, max_days: int = 15) -> bool:
    """Check if posted within the last N days."""
    if not posted_date:
        return True  # no date = include (benefit of the doubt)
    try:
        # Handle various date formats
        for fmt in ["%Y-%m-%d", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%dT%H:%M:%S.%f"]:
            try:
                dt = datetime.strptime(posted_date[:len("2026-05-02T00:00:00.000")], fmt)
                return (datetime.now() - dt).days <= max_days
            except ValueError:
                continue
        # Try millisecond timestamp (Lever uses this)
        if posted_date.isdigit() and len(posted_date) >= 10:
            dt = datetime.fromtimestamp(int(posted_date) / 1000)
            return (datetime.now() - dt).days <= max_days
        return True  # can't parse = include
    except Exception:
        return True


def is_english(description: str) -> bool:
    """Check if description is primarily in English."""
    if not description:
        return True
    desc_lower = description.lower()
    non_english_hits = sum(1 for signal in NON_ENGLISH_SIGNALS if signal in desc_lower)
    return non_english_hits < 3  # allow 1-2 stray words


def filter_job(title: str, location: str, description: str, posted_date: str) -> tuple[bool, str]:
    """
    Apply all filters. Returns (passes, reason).
    """
    if not is_europe_or_remote(location):
        return False, "not_europe"
    if not is_mid_level_or_below(title):
        return False, "too_senior"
    if not is_recent(posted_date):
        return False, "too_old"
    if not is_english(description):
        return False, "not_english"
    return True, ""


# ═══════════════════════════════════════════════════════════════
# ATS JOB DATACLASS
# ═══════════════════════════════════════════════════════════════

@dataclass
class ATSJob:
    title: str
    company: str
    location: str
    url: str
    description: str = ""
    source: str = ""
    remote: bool = False
    department: str = ""
    posted_date: str = ""
    ats_company_slug: str = ""

    def to_dict(self):
        return asdict(self)


# ═══════════════════════════════════════════════════════════════
# GREENHOUSE
# ═══════════════════════════════════════════════════════════════

class GreenhouseScraper:

    def try_slugs(self, company_name: str) -> Optional[str]:
        slugs = self._generate_slugs(company_name)
        for slug in slugs:
            for base in [
                f"https://boards-api.greenhouse.io/v1/boards/{slug}/jobs",
                f"https://api.greenhouse.io/v1/boards/{slug}/jobs",
            ]:
                try:
                    resp = requests.get(base, headers=HEADERS, timeout=REQUEST_TIMEOUT)
                    if resp.status_code == 200:
                        data = resp.json()
                        if data.get("jobs"):
                            return slug
                    time.sleep(RATE_LIMIT_DELAY)
                except Exception:
                    continue
        return None

    def scrape(self, slug: str, company_name: str) -> list[ATSJob]:
        urls = [
            f"https://boards-api.greenhouse.io/v1/boards/{slug}/jobs?content=true",
            f"https://api.greenhouse.io/v1/boards/{slug}/jobs?content=true",
        ]
        data = None
        for url in urls:
            try:
                resp = requests.get(url, headers=HEADERS, timeout=REQUEST_TIMEOUT)
                if resp.status_code == 200:
                    data = resp.json()
                    break
            except Exception:
                continue
        if data is None:
            return []

        jobs = []
        for j in data.get("jobs", []):
            location = j.get("location", {}).get("name", "")
            desc_html = j.get("content", "")
            desc_text = re.sub(r"<[^>]+>", " ", desc_html)
            desc_text = re.sub(r"\s+", " ", desc_text).strip()
            dept = j.get("departments", [])

            jobs.append(ATSJob(
                title=j.get("title", ""),
                company=company_name,
                location=location,
                url=j.get("absolute_url", ""),
                description=desc_text,
                source="greenhouse",
                remote="remote" in location.lower(),
                department=dept[0].get("name", "") if dept else "",
                posted_date=j.get("updated_at", "")[:10],
                ats_company_slug=slug,
            ))
        return jobs

    def _generate_slugs(self, name: str) -> list[str]:
        clean = re.sub(r"[^a-zA-Z0-9\s]", "", name).strip()
        lower = clean.lower()
        seen = set()
        slugs = []
        for s in [lower.replace(" ", ""), lower, lower.replace(" ", "-"), lower.replace(" ", "_")]:
            if s not in seen:
                seen.add(s)
                slugs.append(s)
        return slugs


# ═══════════════════════════════════════════════════════════════
# LEVER
# ═══════════════════════════════════════════════════════════════

class LeverScraper:

    def try_slugs(self, company_name: str) -> Optional[str]:
        slugs = self._generate_slugs(company_name)
        for slug in slugs:
            url = f"https://api.lever.co/v0/postings/{slug}?mode=json&limit=1"
            try:
                resp = requests.get(url, headers=HEADERS, timeout=REQUEST_TIMEOUT)
                if resp.status_code == 200:
                    data = resp.json()
                    if isinstance(data, list) and len(data) > 0:
                        return slug
                time.sleep(RATE_LIMIT_DELAY)
            except Exception:
                continue
        return None

    def scrape(self, slug: str, company_name: str) -> list[ATSJob]:
        url = f"https://api.lever.co/v0/postings/{slug}?mode=json"
        try:
            resp = requests.get(url, headers=HEADERS, timeout=REQUEST_TIMEOUT)
            if resp.status_code != 200:
                return []
            data = resp.json()
        except Exception:
            return []

        jobs = []
        for j in data:
            location = j.get("categories", {}).get("location", "")
            desc = j.get("descriptionPlain", "")
            # Lever gives createdAt as millisecond timestamp
            created = j.get("createdAt", "")
            posted = str(created) if created else ""

            jobs.append(ATSJob(
                title=j.get("text", ""),
                company=company_name,
                location=location,
                url=j.get("hostedUrl", ""),
                description=desc,
                source="lever",
                remote="remote" in location.lower() if location else False,
                department=j.get("categories", {}).get("team", ""),
                posted_date=posted,
                ats_company_slug=slug,
            ))
        return jobs

    def _generate_slugs(self, name: str) -> list[str]:
        clean = re.sub(r"[^a-zA-Z0-9\s]", "", name).strip()
        lower = clean.lower()
        seen = set()
        slugs = []
        for s in [lower.replace(" ", ""), lower.replace(" ", "-")]:
            if s not in seen:
                seen.add(s)
                slugs.append(s)
        return slugs


# ═══════════════════════════════════════════════════════════════
# ASHBY
# ═══════════════════════════════════════════════════════════════

class AshbyScraper:

    def try_slugs(self, company_name: str) -> Optional[str]:
        slugs = self._generate_slugs(company_name)
        for slug in slugs:
            url = f"https://api.ashbyhq.com/posting-api/job-board/{slug}?includeCompensation=true"
            try:
                resp = requests.get(url, headers=HEADERS, timeout=REQUEST_TIMEOUT)
                if resp.status_code == 200:
                    data = resp.json()
                    if data.get("jobs"):
                        return slug
                time.sleep(RATE_LIMIT_DELAY)
            except Exception:
                continue
        return None

    def scrape(self, slug: str, company_name: str) -> list[ATSJob]:
        url = f"https://api.ashbyhq.com/posting-api/job-board/{slug}?includeCompensation=true"
        try:
            resp = requests.get(url, headers=HEADERS, timeout=REQUEST_TIMEOUT)
            if resp.status_code != 200:
                return []
            data = resp.json()
        except Exception:
            return []

        jobs = []
        for j in data.get("jobs", []):
            location = j.get("location", "")
            if isinstance(location, dict):
                location = location.get("name", "")

            desc_html = j.get("descriptionHtml", "") or j.get("descriptionPlain", "")
            desc_text = re.sub(r"<[^>]+>", " ", desc_html)
            desc_text = re.sub(r"\s+", " ", desc_text).strip()

            job_url = j.get("jobUrl", f"https://jobs.ashbyhq.com/{slug}/{j.get('id', '')}")

            jobs.append(ATSJob(
                title=j.get("title", ""),
                company=company_name,
                location=location if isinstance(location, str) else "",
                url=job_url,
                description=desc_text,
                source="ashby",
                remote=j.get("isRemote", False) or "remote" in str(location).lower(),
                department=j.get("department", ""),
                posted_date=j.get("publishedAt", "")[:10] if j.get("publishedAt") else "",
                ats_company_slug=slug,
            ))
        return jobs

    def _generate_slugs(self, name: str) -> list[str]:
        clean = re.sub(r"[^a-zA-Z0-9\s]", "", name).strip()
        lower = clean.lower()
        seen = set()
        slugs = []
        for s in [lower.replace(" ", ""), lower.replace(" ", "-")]:
            if s not in seen:
                seen.add(s)
                slugs.append(s)
        return slugs


# ═══════════════════════════════════════════════════════════════
# ATS CACHE
# ═══════════════════════════════════════════════════════════════

class ATSCache:
    """Remembers which ATS each company uses. Stores misses too."""

    def __init__(self, cache_path: str = "ats_cache.json"):
        self.path = cache_path
        self.data = self._load()

    def _load(self) -> dict:
        try:
            with open(self.path, "r") as f:
                return json.load(f)
        except (FileNotFoundError, json.JSONDecodeError):
            return {}

    def save(self):
        with open(self.path, "w") as f:
            json.dump(self.data, f, indent=2)

    def get(self, company: str) -> Optional[dict]:
        return self.data.get(company.lower())

    def set(self, company: str, ats: Optional[str], slug: Optional[str]):
        self.data[company.lower()] = {"ats": ats, "slug": slug}


# ═══════════════════════════════════════════════════════════════
# DISCOVERY ENGINE
# ═══════════════════════════════════════════════════════════════

class ATSDiscovery:

    def __init__(self, cache_path: str = "ats_cache.json"):
        self.greenhouse = GreenhouseScraper()
        self.lever = LeverScraper()
        self.ashby = AshbyScraper()
        self.cache = ATSCache(cache_path)

    def discover_ats(self, company_name: str) -> Optional[tuple[str, str]]:
        cached = self.cache.get(company_name)
        if cached is not None:
            if cached["ats"] is None:
                return None
            return (cached["ats"], cached["slug"])

        log.info(f"  Discovering ATS for: {company_name}")

        slug = self.greenhouse.try_slugs(company_name)
        if slug:
            self.cache.set(company_name, "greenhouse", slug)
            log.info(f"    → Greenhouse: {slug}")
            return ("greenhouse", slug)

        slug = self.lever.try_slugs(company_name)
        if slug:
            self.cache.set(company_name, "lever", slug)
            log.info(f"    → Lever: {slug}")
            return ("lever", slug)

        slug = self.ashby.try_slugs(company_name)
        if slug:
            self.cache.set(company_name, "ashby", slug)
            log.info(f"    → Ashby: {slug}")
            return ("ashby", slug)

        self.cache.set(company_name, None, None)
        log.info(f"    → No ATS found")
        return None

    def scrape_company(self, company_name: str, ats: str, slug: str) -> list[ATSJob]:
        if ats == "greenhouse":
            return self.greenhouse.scrape(slug, company_name)
        elif ats == "lever":
            return self.lever.scrape(slug, company_name)
        elif ats == "ashby":
            return self.ashby.scrape(slug, company_name)
        return []

    def discover_and_scrape(self, company_names: list[str], apply_filters: bool = True) -> list[ATSJob]:
        """
        Discover ATS for companies, scrape jobs, optionally filter.
        Returns filtered ATSJob list.
        """
        all_jobs = []
        filter_stats = {"not_europe": 0, "too_senior": 0, "too_old": 0, "not_english": 0}

        unique = list(dict.fromkeys([c.strip() for c in company_names if c.strip()]))
        log.info(f"[ATS Discovery] Processing {len(unique)} unique companies")

        for company in unique:
            result = self.discover_ats(company)
            if result is None:
                continue

            ats, slug = result
            raw_jobs = self.scrape_company(company, ats, slug)

            if apply_filters:
                filtered = []
                for job in raw_jobs:
                    passes, reason = filter_job(job.title, job.location, job.description, job.posted_date)
                    if passes:
                        filtered.append(job)
                    elif reason in filter_stats:
                        filter_stats[reason] += 1
                log.info(f"    {company} ({ats}): {len(raw_jobs)} total, {len(filtered)} after filters")
                all_jobs.extend(filtered)
            else:
                all_jobs.extend(raw_jobs)
                log.info(f"    {company} ({ats}): {len(raw_jobs)} open roles")

            time.sleep(RATE_LIMIT_DELAY)

        self.cache.save()

        log.info(f"\n[ATS Discovery] Results:")
        log.info(f"  Companies processed: {len(unique)}")
        log.info(f"  Jobs after filters: {len(all_jobs)}")
        if apply_filters:
            log.info(f"  Filtered out — not Europe/remote: {filter_stats['not_europe']}")
            log.info(f"  Filtered out — too senior: {filter_stats['too_senior']}")
            log.info(f"  Filtered out — too old (>15 days): {filter_stats['too_old']}")
            log.info(f"  Filtered out — not English: {filter_stats['not_english']}")

        return all_jobs


# ═══════════════════════════════════════════════════════════════
# STANDALONE TEST
# ═══════════════════════════════════════════════════════════════

if __name__ == "__main__":
    test_companies = ["n8n", "Personio", "Notion", "Mistral AI", "Linear"]
    discovery = ATSDiscovery(cache_path="ats_cache_test.json")
    jobs = discovery.discover_and_scrape(test_companies, apply_filters=True)

    print(f"\n{'=' * 60}")
    print(f"Found {len(jobs)} jobs (filtered)")
    for job in jobs[:10]:
        print(f"  {job.title} | {job.company} | {job.location} | {job.source}")

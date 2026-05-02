"""
ATS DISCOVERY SCRAPER
======================
Given a list of company names (from job board results), discovers their ATS
(Greenhouse, Lever, Ashby) and pulls all open roles with full descriptions.

Usage:
    from ats_scraper import ATSDiscovery
    discovery = ATSDiscovery()
    jobs = discovery.discover_and_scrape(["n8n", "Personio", "Bending Spoons"])
"""

import re
import json
import time
import logging
import requests
from dataclasses import dataclass, field, asdict
from typing import Optional
from urllib.parse import quote_plus

logging.basicConfig(level=logging.INFO)
log = logging.getLogger("ats_discovery")

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept": "application/json, text/html, */*",
}

REQUEST_TIMEOUT = 15
RATE_LIMIT_DELAY = 0.5  # seconds between requests


@dataclass
class ATSJob:
    title: str
    company: str
    location: str
    url: str
    description: str = ""
    source: str = ""  # greenhouse, lever, ashby
    remote: bool = False
    department: str = ""
    posted_date: str = ""
    ats_company_slug: str = ""

    def to_dict(self):
        return asdict(self)

    @classmethod
    def from_dict(cls, d):
        return cls(**{k: v for k, v in d.items() if k in cls.__dataclass_fields__})


class GreenhouseScraper:
    """
    Greenhouse exposes a JSON API at:
    https://boards-api.greenhouse.io/v1/boards/{slug}/jobs
    Each job has full HTML description.
    """

    def try_slugs(self, company_name: str) -> Optional[str]:
        """Try common slug variations to find the company's Greenhouse board."""
        slugs = self._generate_slugs(company_name)
        for slug in slugs:
            # Try both known Greenhouse endpoints
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
        """Pull all jobs from a Greenhouse board."""
        # Try primary endpoint, fall back to alternative
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
            log.warning(f"Greenhouse scrape failed for {slug}: no valid response")
            return []

        jobs = []
        for j in data.get("jobs", []):
            location = j.get("location", {}).get("name", "")
            # Clean HTML from description
            desc_html = j.get("content", "")
            desc_text = re.sub(r"<[^>]+>", " ", desc_html)
            desc_text = re.sub(r"\s+", " ", desc_text).strip()

            job = ATSJob(
                title=j.get("title", ""),
                company=company_name,
                location=location,
                url=j.get("absolute_url", ""),
                description=desc_text,
                source="greenhouse",
                remote="remote" in location.lower(),
                department=self._extract_department(j),
                posted_date=j.get("updated_at", "")[:10],
                ats_company_slug=slug,
            )
            jobs.append(job)
        return jobs

    def _extract_department(self, job_data: dict) -> str:
        departments = job_data.get("departments", [])
        if departments:
            return departments[0].get("name", "")
        return ""

    def _generate_slugs(self, name: str) -> list[str]:
        clean = re.sub(r"[^a-zA-Z0-9\s]", "", name).strip()
        lower = clean.lower()
        no_spaces = lower.replace(" ", "")
        dashed = lower.replace(" ", "-")
        underscored = lower.replace(" ", "_")
        # Return unique slugs preserving order
        seen = set()
        slugs = []
        for s in [no_spaces, lower, dashed, underscored, clean]:
            if s not in seen:
                seen.add(s)
                slugs.append(s)
        return slugs


class LeverScraper:
    """
    Lever has public job pages at:
    https://api.lever.co/v0/postings/{slug}
    Returns JSON with full descriptions.
    """

    def try_slugs(self, company_name: str) -> Optional[str]:
        slugs = self._generate_slugs(company_name)
        for slug in slugs:
            url = f"https://api.lever.co/v0/postings/{slug}?mode=json"
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
        except Exception as e:
            log.warning(f"Lever scrape failed for {slug}: {e}")
            return []

        jobs = []
        for j in data:
            location = j.get("categories", {}).get("location", "")
            # Build description from lists
            desc_parts = []
            for section in j.get("lists", []):
                desc_parts.append(section.get("text", ""))
                for item in section.get("content", "").split("<li>"):
                    clean = re.sub(r"<[^>]+>", "", item).strip()
                    if clean:
                        desc_parts.append(clean)
            desc_text = " ".join(desc_parts)

            # Also get the opening description
            additional = j.get("descriptionPlain", "")
            if additional:
                desc_text = additional + " " + desc_text

            job = ATSJob(
                title=j.get("text", ""),
                company=company_name,
                location=location,
                url=j.get("hostedUrl", ""),
                description=desc_text.strip(),
                source="lever",
                remote="remote" in location.lower() if location else False,
                department=j.get("categories", {}).get("team", ""),
                posted_date=str(j.get("createdAt", ""))[:10],
                ats_company_slug=slug,
            )
            jobs.append(job)
        return jobs

    def _generate_slugs(self, name: str) -> list[str]:
        clean = re.sub(r"[^a-zA-Z0-9\s]", "", name).strip()
        lower = clean.lower()
        no_spaces = lower.replace(" ", "")
        dashed = lower.replace(" ", "-")
        seen = set()
        slugs = []
        for s in [no_spaces, dashed, lower]:
            if s not in seen:
                seen.add(s)
                slugs.append(s)
        return slugs


class AshbyScraper:
    """
    Ashby has a public API at:
    https://api.ashbyhq.com/posting-api/job-board/{slug}
    Returns JSON with job listings.
    """

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
        except Exception as e:
            log.warning(f"Ashby scrape failed for {slug}: {e}")
            return []

        jobs = []
        for j in data.get("jobs", []):
            location = j.get("location", "")
            if isinstance(location, dict):
                location = location.get("name", "")

            # Get full description
            desc_html = j.get("descriptionHtml", "") or j.get("description", "")
            desc_text = re.sub(r"<[^>]+>", " ", desc_html)
            desc_text = re.sub(r"\s+", " ", desc_text).strip()

            job_url = f"https://jobs.ashbyhq.com/{slug}/{j.get('id', '')}"

            job = ATSJob(
                title=j.get("title", ""),
                company=company_name,
                location=location if isinstance(location, str) else "",
                url=j.get("jobUrl", job_url),
                description=desc_text,
                source="ashby",
                remote="remote" in str(location).lower(),
                department=j.get("department", ""),
                posted_date=j.get("publishedAt", "")[:10] if j.get("publishedAt") else "",
                ats_company_slug=slug,
            )
            jobs.append(job)
        return jobs

    def _generate_slugs(self, name: str) -> list[str]:
        clean = re.sub(r"[^a-zA-Z0-9\s]", "", name).strip()
        lower = clean.lower()
        no_spaces = lower.replace(" ", "")
        dashed = lower.replace(" ", "-")
        seen = set()
        slugs = []
        for s in [no_spaces, dashed, lower]:
            if s not in seen:
                seen.add(s)
                slugs.append(s)
        return slugs


# ═══════════════════════════════════════════════════════════════════
# ATS DISCOVERY CACHE
# ═══════════════════════════════════════════════════════════════════

class ATSCache:
    """
    Remembers which ATS each company uses so we don't re-discover every run.
    Stored as JSON: { "company_name": {"ats": "greenhouse", "slug": "companyname"} }
    Also stores misses: { "company_name": {"ats": null} }
    """

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


# ═══════════════════════════════════════════════════════════════════
# MAIN DISCOVERY ENGINE
# ═══════════════════════════════════════════════════════════════════

class ATSDiscovery:
    """
    Takes company names, discovers their ATS, and scrapes all open roles.
    Caches results so repeated runs are fast.
    """

    def __init__(self, cache_path: str = "ats_cache.json"):
        self.greenhouse = GreenhouseScraper()
        self.lever = LeverScraper()
        self.ashby = AshbyScraper()
        self.cache = ATSCache(cache_path)

    def discover_ats(self, company_name: str) -> Optional[tuple[str, str]]:
        """
        Try to find which ATS a company uses.
        Returns (ats_name, slug) or None.
        """
        # Check cache first
        cached = self.cache.get(company_name)
        if cached is not None:
            if cached["ats"] is None:
                return None  # known miss
            return (cached["ats"], cached["slug"])

        log.info(f"  Discovering ATS for: {company_name}")

        # Try Greenhouse first (most common for startups)
        slug = self.greenhouse.try_slugs(company_name)
        if slug:
            self.cache.set(company_name, "greenhouse", slug)
            log.info(f"    → Greenhouse: {slug}")
            return ("greenhouse", slug)

        # Try Lever
        slug = self.lever.try_slugs(company_name)
        if slug:
            self.cache.set(company_name, "lever", slug)
            log.info(f"    → Lever: {slug}")
            return ("lever", slug)

        # Try Ashby
        slug = self.ashby.try_slugs(company_name)
        if slug:
            self.cache.set(company_name, "ashby", slug)
            log.info(f"    → Ashby: {slug}")
            return ("ashby", slug)

        # No ATS found
        self.cache.set(company_name, None, None)
        log.info(f"    → No ATS found")
        return None

    def scrape_company(self, company_name: str, ats: str, slug: str) -> list[ATSJob]:
        """Scrape all jobs from a known ATS."""
        if ats == "greenhouse":
            return self.greenhouse.scrape(slug, company_name)
        elif ats == "lever":
            return self.lever.scrape(slug, company_name)
        elif ats == "ashby":
            return self.ashby.scrape(slug, company_name)
        return []

    def discover_and_scrape(self, company_names: list[str]) -> list[ATSJob]:
        """
        Main entry point. Takes a list of company names, discovers their ATS,
        scrapes all open roles, and returns them.
        """
        all_jobs = []
        discovered = 0
        cached_hits = 0

        # Deduplicate company names
        unique_companies = list(dict.fromkeys(
            [c.strip() for c in company_names if c.strip()]
        ))

        log.info(f"[ATS Discovery] Processing {len(unique_companies)} unique companies")

        for company in unique_companies:
            result = self.discover_ats(company)
            if result is None:
                continue

            ats, slug = result
            was_cached = self.cache.get(company) is not None

            if was_cached:
                cached_hits += 1
            else:
                discovered += 1

            jobs = self.scrape_company(company, ats, slug)
            all_jobs.extend(jobs)
            log.info(f"    {company} ({ats}): {len(jobs)} open roles")

            time.sleep(RATE_LIMIT_DELAY)

        # Save cache
        self.cache.save()

        log.info(f"\n[ATS Discovery] Results:")
        log.info(f"  Companies processed: {len(unique_companies)}")
        log.info(f"  ATS discovered (new): {discovered}")
        log.info(f"  ATS from cache: {cached_hits}")
        log.info(f"  Total jobs found: {len(all_jobs)}")

        return all_jobs


# ═══════════════════════════════════════════════════════════════════
# STANDALONE TEST
# ═══════════════════════════════════════════════════════════════════

if __name__ == "__main__":
    # Test with some known companies
    test_companies = [
        "n8n",
        "Personio",
        "Pitch",
        "Bending Spoons",
        "Mistral AI",
        "Anyscale",
        "Notion",
        "Linear",
        "Figma",
        "Vercel",
    ]

    discovery = ATSDiscovery(cache_path="ats_cache_test.json")
    jobs = discovery.discover_and_scrape(test_companies)

    print(f"\n{'=' * 60}")
    print(f"Found {len(jobs)} total jobs across all companies")
    print(f"{'=' * 60}")

    # Show sample
    for job in jobs[:10]:
        print(f"\n  {job.title}")
        print(f"  {job.company} | {job.location} | {job.source}")
        print(f"  {job.url}")
        print(f"  Desc: {job.description[:100]}...")

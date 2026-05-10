"""
ATS DISCOVERY SCRAPER (updated filters)
=========================================
Stricter language filtering — catches German-language postings,
not just explicit "German required" phrases.
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
# JOB FILTERS (v2 — stricter language detection)
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
    "manager,", "manager -", "manager –",
    "architect",
]

MID_OR_BELOW_KEYWORDS = [
    "junior", "jr.", "jr ", "associate", "analyst", "coordinator",
    "specialist", "assistant", "intern", "trainee", "graduate",
    "entry", "werkstudent", "working student",
]

# ── German language detection (v2 — much more aggressive) ──

# Explicit German requirement phrases (EN + DE)
GERMAN_REQUIRED_PATTERNS = [
    re.compile(p, re.IGNORECASE) for p in [
        r"german\s*(c1|c2|native|fluent|mother\s*tongue|muttersprache)",
        r"flie[ßs]end\s*deutsch",
        r"verhandlungssicher\s*deutsch",
        r"deutsch\s*(c1|c2|muttersprachlich|erforderlich|zwingend|vorausgesetzt)",
        r"german\s*language\s*(required|essential|mandatory|necessary|needed)",
        r"must\s*speak\s*german",
        r"fluency\s*in\s*german\s*(is\s*)?(required|essential|mandatory)",
        r"german\s*(is\s*)?(required|essential|a\s*must)",
        r"native[\s-]*level\s*german",
        r"business[\s-]*level\s*german",
        r"proficient\s*in\s*german",
        r"excellent\s*(command\s*of|knowledge\s*of)\s*german",
        r"deutschkenntnisse\s*(erforderlich|zwingend|erwünscht|vorausgesetzt|von vorteil)",
        r"sehr\s*gute\s*deutschkenntnisse",
        r"gute\s*deutschkenntnisse",
        r"sprache.*deutsch.*erforderlich",
        r"skills\s*in\s*german.*(?:required|c1|c2|fluent)",
        r"german\s*and\s*english.*(?:c1|c2|required|mandatory)",
        r"in\s*german\s*and\s*english.*(?:required|c1|c2)",
        r"(?:c1|c2)\s*level\s*required\s*for\s*both",
        r"minimum\s*c1.*german",
        r"german.*minimum\s*c1",
        r"DACH\s*market.*german",
    ]
]

# Words/phrases that only appear in German-language text
# If we find enough of these, the posting itself is in German
GERMAN_LANGUAGE_SIGNALS = [
    # Headings and section titles
    "stellenbeschreibung", "aufgaben", "anforderungen", "wir bieten",
    "deine aufgaben", "dein profil", "was wir bieten", "bewerbung",
    "über uns", "ihr profil", "ihre aufgaben", "unser angebot",
    "was dich erwartet", "was du mitbringst", "darauf kannst du dich freuen",
    "das erwartet dich", "das bringst du mit",
    # Common job description words
    "arbeitsort", "festanstellung", "vollzeit", "teilzeit",
    "berufserfahrung", "abgeschlossenes studium", "idealerweise",
    "eigenverantwortlich", "teamfähigkeit", "kommunikationsstärke",
    "selbstständig", "verantwortungsbewusst", "belastbar",
    "bewerben sie sich", "bewirb dich", "freuen uns auf",
    "ab sofort", "zum nächstmöglichen zeitpunkt", "unbefristet",
    "wir suchen", "zur verstärkung", "mitarbeiter", "mitarbeiterin",
    # Verbs and grammar that are distinctly German
    "und", "oder", "für", "mit", "bei", "nach",
    "werden", "haben", "sind", "können",
]

# Non-English signals (French, Dutch, etc.)
NON_ENGLISH_SIGNALS = [
    # French
    "description du poste", "responsabilités", "nous offrons",
    "votre profil", "rejoignez", "candidature", "temps plein",
    "nous recherchons", "vous êtes", "expérience souhaitée",
    # Dutch
    "functieomschrijving", "wat bied je", "wat zoeken wij",
    "jouw profiel", "wij bieden", "solliciteren",
]


def _count_german_signals(text: str) -> int:
    """Count how many German-language signals appear in text."""
    text_lower = text.lower()
    count = 0
    for signal in GERMAN_LANGUAGE_SIGNALS:
        if signal in text_lower:
            count += 1
    return count


def requires_german(title: str, description: str) -> bool:
    """
    Check if a job requires German — either explicitly stated or
    because the posting is written in German.
    """
    full_text = f"{title} {description}"

    # Check explicit requirement patterns
    for pattern in GERMAN_REQUIRED_PATTERNS:
        if pattern.search(full_text):
            return True

    # Check if the posting itself is in German
    # We look at the description only (title might have German city names)
    if description:
        german_signal_count = _count_german_signals(description)
        # If we find 5+ German signals, it's a German-language posting
        if german_signal_count >= 5:
            return True

        # Also check: if common German structural words appear frequently,
        # it's likely a German posting. Count "und", "oder", "für", "mit"
        # but only if they appear as whole words
        desc_lower = description.lower()
        german_word_count = 0
        for word in ["und", "oder", "für", "mit", "bei", "nach", "werden", "haben", "sind", "können"]:
            # Match as whole words only
            if re.search(rf'\b{word}\b', desc_lower):
                german_word_count += 1
        if german_word_count >= 6:
            return True

    return False


def is_europe_or_remote(location: str) -> bool:
    if not location:
        return False
    loc = location.lower()
    return any(term in loc for term in EUROPE_LOCATIONS)


def is_mid_level_or_below(title: str) -> bool:
    t = title.lower()
    if any(kw in t for kw in MID_OR_BELOW_KEYWORDS):
        return True
    if any(kw in t for kw in SENIOR_KEYWORDS):
        return False
    return True


def is_recent(posted_date: str, max_days: int = 30) -> bool:
    if not posted_date:
        return True
    try:
        for fmt in ["%Y-%m-%d", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%dT%H:%M:%S.%f"]:
            try:
                dt = datetime.strptime(posted_date[:len("2026-05-02T00:00:00.000")], fmt)
                return (datetime.now() - dt).days <= max_days
            except ValueError:
                continue
        if posted_date.isdigit() and len(posted_date) >= 10:
            dt = datetime.fromtimestamp(int(posted_date) / 1000)
            return (datetime.now() - dt).days <= max_days
        return True
    except Exception:
        return True


def is_english(description: str) -> bool:
    """Check if description is primarily in English (not German, French, Dutch, etc.)."""
    if not description:
        return True
    desc_lower = description.lower()

    # Check non-English signals (French, Dutch)
    non_english_hits = sum(1 for signal in NON_ENGLISH_SIGNALS if signal in desc_lower)
    if non_english_hits >= 2:
        return False

    return True


def filter_job(title: str, location: str, description: str, posted_date: str) -> tuple[bool, str]:
    """Apply all filters. Returns (passes, reason)."""
    if not is_europe_or_remote(location):
        return False, "not_europe"
    if not is_mid_level_or_below(title):
        return False, "too_senior"
    if not is_recent(posted_date):
        return False, "too_old"
    if requires_german(title, description):
        return False, "requires_german"
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
        all_jobs = []
        filter_stats = {"not_europe": 0, "too_senior": 0, "too_old": 0, "requires_german": 0, "not_english": 0}

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
            log.info(f"  Filtered out — requires German: {filter_stats['requires_german']}")
            log.info(f"  Filtered out — not English: {filter_stats['not_english']}")

        return all_jobs


if __name__ == "__main__":
    test_companies = ["n8n", "Personio", "Notion", "Mistral AI", "Linear"]
    discovery = ATSDiscovery(cache_path="ats_cache_test.json")
    jobs = discovery.discover_and_scrape(test_companies, apply_filters=True)

    print(f"\n{'=' * 60}")
    print(f"Found {len(jobs)} jobs (filtered)")
    for job in jobs[:10]:
        print(f"  {job.title} | {job.company} | {job.location} | {job.source}")

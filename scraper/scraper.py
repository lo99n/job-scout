"""
Job Scraper & Matcher for WHU MBA Friends
==========================================
Scrapes 8 job boards, qualifies descriptions, and matches to friend profiles.
Outputs 5 unique jobs per friend per day.

Usage:
    python scraper.py              # Run full scrape + match
    python scraper.py --match-only # Re-match from cached jobs
    python scraper.py --dry-run    # Test without saving
"""

import json
import re
import csv
import time
import hashlib
import os
import sys
from datetime import datetime, timedelta
from pathlib import Path
from dataclasses import dataclass, field, asdict
from typing import Optional
from urllib.parse import urlencode, quote_plus
from ats_integration import enrich_with_ats
from ai_matcher import AIJobMatcher

import requests
from bs4 import BeautifulSoup

# ── Config ──────────────────────────────────────────────────────────────────

BASE_DIR = Path(__file__).parent
PROFILES_FILE = BASE_DIR / "profiles.json"
JOBS_CACHE = BASE_DIR / "jobs_cache.json"
SEEN_FILE = BASE_DIR / "seen_jobs.json"
OUTPUT_DIR = BASE_DIR / "output"
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
}
REQUEST_DELAY = 2  # seconds between requests per board


# ── Data Model ──────────────────────────────────────────────────────────────

@dataclass
class Job:
    id: str
    title: str
    company: str
    location: str
    url: str
    description: str
    source: str
    salary_min: Optional[int] = None
    salary_max: Optional[int] = None
    posted_date: Optional[str] = None
    remote: bool = False
    tags: list = field(default_factory=list)
    scraped_at: str = field(default_factory=lambda: datetime.now().isoformat())

    def to_dict(self):
        return asdict(self)

    @classmethod
    def from_dict(cls, d):
        return cls(**{k: v for k, v in d.items() if k in cls.__dataclass_fields__})


# ── Scraper Base ────────────────────────────────────────────────────────────

class BaseScraper:
    name = "base"
    base_url = ""

    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update(HEADERS)

    def scrape(self, search_terms: list[str], location: str = "Germany") -> list[Job]:
        raise NotImplementedError

    def _get(self, url, params=None, retries=3):
        for attempt in range(retries):
            try:
                resp = self.session.get(url, params=params, timeout=15)
                if resp.status_code == 200:
                    return resp
                elif resp.status_code == 429:
                    wait = min(30, 5 * (attempt + 1))
                    print(f"  [!] Rate limited on {self.name}, waiting {wait}s...")
                    time.sleep(wait)
                else:
                    print(f"  [!] {self.name} returned {resp.status_code} for {url}")
                    return None
            except requests.RequestException as e:
                print(f"  [!] {self.name} error: {e}")
                if attempt < retries - 1:
                    time.sleep(3)
        return None

    def _make_id(self, title, company, url):
        raw = f"{title}|{company}|{url}"
        return hashlib.md5(raw.encode()).hexdigest()[:12]


# ── Board Scrapers ──────────────────────────────────────────────────────────

class ArbeitnowScraper(BaseScraper):
    """Arbeitnow — public JSON API, very clean"""
    name = "arbeitnow"
    base_url = "https://www.arbeitnow.com/api/job-board-api"

    def scrape(self, search_terms, location="Germany"):
        jobs = []
        seen_urls = set()
        for page in range(1, 4):  # 3 pages max
            resp = self._get(self.base_url, params={"page": page})
            if not resp:
                break
            data = resp.json()
            for item in data.get("data", []):
                url = item.get("url", "")
                if url in seen_urls:
                    continue
                seen_urls.add(url)
                title = item.get("title", "")
                desc = item.get("description", "")
                loc = item.get("location", "")
                # Basic relevance filter
                combined = f"{title} {desc}".lower()
                if any(term.lower() in combined for term in search_terms):
                    jobs.append(Job(
                        id=self._make_id(title, item.get("company_name", ""), url),
                        title=title,
                        company=item.get("company_name", ""),
                        location=loc,
                        url=url,
                        description=BeautifulSoup(desc, "html.parser").get_text(separator=" ", strip=True),
                        source=self.name,
                        remote=item.get("remote", False),
                        tags=item.get("tags", []),
                        posted_date=item.get("created_at", ""),
                    ))
            time.sleep(REQUEST_DELAY)
            if not data.get("links", {}).get("next"):
                break
        print(f"  [{self.name}] Found {len(jobs)} relevant jobs")
        return jobs


class RemoteOKScraper(BaseScraper):
    """RemoteOK — public JSON API"""
    name = "remoteok"
    base_url = "https://remoteok.com/api"

    def scrape(self, search_terms, location="Germany"):
        jobs = []
        resp = self._get(self.base_url)
        if not resp:
            return jobs
        data = resp.json()
        # First item is metadata, skip it
        for item in data[1:] if len(data) > 1 else []:
            title = item.get("position", "")
            company = item.get("company", "")
            desc = item.get("description", "")
            combined = f"{title} {desc} {' '.join(item.get('tags', []))}".lower()
            if any(term.lower() in combined for term in search_terms):
                url = f"https://remoteok.com/remote-jobs/{item.get('slug', item.get('id', ''))}"
                salary_min = None
                salary_max = None
                if item.get("salary_min"):
                    try:
                        salary_min = int(item["salary_min"])
                    except (ValueError, TypeError):
                        pass
                if item.get("salary_max"):
                    try:
                        salary_max = int(item["salary_max"])
                    except (ValueError, TypeError):
                        pass
                jobs.append(Job(
                    id=self._make_id(title, company, url),
                    title=title,
                    company=company,
                    location=item.get("location", "Remote"),
                    url=url,
                    description=BeautifulSoup(desc, "html.parser").get_text(separator=" ", strip=True) if desc else "",
                    source=self.name,
                    remote=True,
                    tags=item.get("tags", []),
                    salary_min=salary_min,
                    salary_max=salary_max,
                    posted_date=item.get("date", ""),
                ))
        print(f"  [{self.name}] Found {len(jobs)} relevant jobs")
        return jobs


class StartupJobsScraper(BaseScraper):
    """Startup.jobs — HTML scraping"""
    name = "startupjobs"
    base_url = "https://startup.jobs"

    def scrape(self, search_terms, location="Germany"):
        jobs = []
        seen_urls = set()
        for term in search_terms[:8]:  # Limit queries
            url = f"{self.base_url}/?q={quote_plus(term)}&location=Europe"
            resp = self._get(url)
            if not resp:
                continue
            soup = BeautifulSoup(resp.text, "html.parser")
            for card in soup.select("a.job-card, div.job-listing, article.job"):
                link = card.get("href", "") or ""
                if link and not link.startswith("http"):
                    link = f"{self.base_url}{link}"
                if link in seen_urls:
                    continue
                seen_urls.add(link)
                title_el = card.select_one("h2, h3, .job-title, .title")
                company_el = card.select_one(".company, .company-name, span.text-gray")
                location_el = card.select_one(".location, .job-location")
                title = title_el.get_text(strip=True) if title_el else ""
                company = company_el.get_text(strip=True) if company_el else ""
                loc = location_el.get_text(strip=True) if location_el else ""
                if title and link:
                    # Try to get full description
                    desc = self._get_description(link)
                    jobs.append(Job(
                        id=self._make_id(title, company, link),
                        title=title,
                        company=company,
                        location=loc,
                        url=link,
                        description=desc,
                        source=self.name,
                    ))
            time.sleep(REQUEST_DELAY)
        print(f"  [{self.name}] Found {len(jobs)} relevant jobs")
        return jobs

    def _get_description(self, url):
        resp = self._get(url)
        if not resp:
            return ""
        soup = BeautifulSoup(resp.text, "html.parser")
        desc_el = soup.select_one(".job-description, .description, article, .content")
        if desc_el:
            return desc_el.get_text(separator=" ", strip=True)[:5000]
        return ""


class EuroTechJobsScraper(BaseScraper):
    """EuroTechJobs — HTML scraping"""
    name = "eurotechjobs"
    base_url = "https://www.eurotechjobs.com"

    def scrape(self, search_terms, location="Germany"):
        jobs = []
        seen_urls = set()
        for term in search_terms[:6]:
            url = f"{self.base_url}/search/?q={quote_plus(term)}"
            resp = self._get(url)
            if not resp:
                continue
            soup = BeautifulSoup(resp.text, "html.parser")
            for row in soup.select("div.job-item, tr.job, div.listing, article"):
                link_el = row.select_one("a[href*='/job/'], a[href*='/jobs/']")
                if not link_el:
                    continue
                link = link_el.get("href", "")
                if link and not link.startswith("http"):
                    link = f"{self.base_url}{link}"
                if link in seen_urls:
                    continue
                seen_urls.add(link)
                title = link_el.get_text(strip=True)
                company_el = row.select_one(".company, .employer")
                location_el = row.select_one(".location, .job-location")
                company = company_el.get_text(strip=True) if company_el else ""
                loc = location_el.get_text(strip=True) if location_el else ""
                desc = self._get_description(link)
                if title:
                    jobs.append(Job(
                        id=self._make_id(title, company, link),
                        title=title,
                        company=company,
                        location=loc,
                        url=link,
                        description=desc,
                        source=self.name,
                    ))
            time.sleep(REQUEST_DELAY)
        print(f"  [{self.name}] Found {len(jobs)} relevant jobs")
        return jobs

    def _get_description(self, url):
        resp = self._get(url)
        if not resp:
            return ""
        soup = BeautifulSoup(resp.text, "html.parser")
        desc_el = soup.select_one(".job-description, .description, .job-details, article")
        return desc_el.get_text(separator=" ", strip=True)[:5000] if desc_el else ""


class IndeedScraper(BaseScraper):
    """Indeed — HTML scraping with rate limit care"""
    name = "indeed"
    base_url = "https://de.indeed.com"

    def scrape(self, search_terms, location="Germany"):
        jobs = []
        seen_urls = set()
        for term in search_terms[:6]:
            url = f"{self.base_url}/jobs"
            params = {"q": term, "l": "Germany", "lang": "en", "fromage": "7"}
            resp = self._get(url, params=params)
            if not resp:
                continue
            soup = BeautifulSoup(resp.text, "html.parser")
            for card in soup.select("div.job_seen_beacon, div.jobsearch-ResultsList div.result"):
                title_el = card.select_one("h2 a, a.jcs-JobTitle")
                if not title_el:
                    continue
                title = title_el.get_text(strip=True)
                link = title_el.get("href", "")
                if link and not link.startswith("http"):
                    link = f"{self.base_url}{link}"
                if link in seen_urls:
                    continue
                seen_urls.add(link)
                company_el = card.select_one("span.companyName, span[data-testid='company-name']")
                location_el = card.select_one("div.companyLocation, div[data-testid='text-location']")
                salary_el = card.select_one("div.salary-snippet, span.estimated-salary")
                company = company_el.get_text(strip=True) if company_el else ""
                loc = location_el.get_text(strip=True) if location_el else ""
                salary_min, salary_max = self._parse_salary(salary_el.get_text(strip=True)) if salary_el else (None, None)
                snippet_el = card.select_one("div.job-snippet, table.jobCardShelfContainer")
                snippet = snippet_el.get_text(separator=" ", strip=True)[:2000] if snippet_el else ""
                jobs.append(Job(
                    id=self._make_id(title, company, link),
                    title=title,
                    company=company,
                    location=loc,
                    url=link,
                    description=snippet,
                    source=self.name,
                    salary_min=salary_min,
                    salary_max=salary_max,
                ))
            time.sleep(REQUEST_DELAY + 1)  # Extra cautious
        print(f"  [{self.name}] Found {len(jobs)} relevant jobs")
        return jobs

    def _parse_salary(self, text):
        if not text:
            return None, None
        numbers = re.findall(r"[\d.,]+", text.replace(",", ""))
        nums = []
        for n in numbers:
            try:
                val = int(float(n.replace(".", "")))
                if val > 1000:
                    nums.append(val)
            except ValueError:
                pass
        if len(nums) >= 2:
            return min(nums), max(nums)
        elif len(nums) == 1:
            return nums[0], nums[0]
        return None, None


class WelcomeToTheJungleScraper(BaseScraper):
    """Welcome to the Jungle — API-backed"""
    name = "wttj"
    base_url = "https://www.welcometothejungle.com/en/jobs"

    def scrape(self, search_terms, location="Germany"):
        jobs = []
        seen_urls = set()
        for term in search_terms[:6]:
            url = f"{self.base_url}?query={quote_plus(term)}&page=1&aroundQuery=Germany"
            resp = self._get(url)
            if not resp:
                continue
            soup = BeautifulSoup(resp.text, "html.parser")
            for card in soup.select("article, div[data-testid='search-results-list-item-wrapper'], li.ais-Hits-item"):
                link_el = card.select_one("a[href*='/jobs/']")
                if not link_el:
                    continue
                link = link_el.get("href", "")
                if link and not link.startswith("http"):
                    link = f"https://www.welcometothejungle.com{link}"
                if link in seen_urls:
                    continue
                seen_urls.add(link)
                title_el = card.select_one("h3, h4, [role='heading']")
                company_el = card.select_one("span, p")
                title = title_el.get_text(strip=True) if title_el else link_el.get_text(strip=True)
                company = company_el.get_text(strip=True) if company_el else ""
                if title:
                    desc = self._get_description(link)
                    jobs.append(Job(
                        id=self._make_id(title, company, link),
                        title=title,
                        company=company,
                        location="Germany",
                        url=link,
                        description=desc,
                        source=self.name,
                    ))
            time.sleep(REQUEST_DELAY)
        print(f"  [{self.name}] Found {len(jobs)} relevant jobs")
        return jobs

    def _get_description(self, url):
        time.sleep(1)
        resp = self._get(url)
        if not resp:
            return ""
        soup = BeautifulSoup(resp.text, "html.parser")
        desc_el = soup.select_one("div[data-testid='job-section-description'], .job-description, section.sc-")
        return desc_el.get_text(separator=" ", strip=True)[:5000] if desc_el else ""


class OttaScraper(BaseScraper):
    """Otta — HTML/JS hybrid"""
    name = "otta"
    base_url = "https://app.otta.com"

    def scrape(self, search_terms, location="Germany"):
        jobs = []
        seen_urls = set()
        # Otta uses JS rendering heavily, so we try the search page
        for term in search_terms[:5]:
            url = f"{self.base_url}/jobs?query={quote_plus(term)}&location=Germany"
            resp = self._get(url)
            if not resp:
                continue
            soup = BeautifulSoup(resp.text, "html.parser")
            # Try to find job cards in whatever structure Otta uses
            for card in soup.select("a[href*='/jobs/'], div[class*='JobCard'], li[class*='job']"):
                link = card.get("href", "")
                if not link:
                    link_el = card.select_one("a")
                    link = link_el.get("href", "") if link_el else ""
                if link and not link.startswith("http"):
                    link = f"{self.base_url}{link}"
                if not link or link in seen_urls:
                    continue
                seen_urls.add(link)
                title = card.get_text(strip=True)[:100]
                jobs.append(Job(
                    id=self._make_id(title, "", link),
                    title=title,
                    company="",
                    location="Germany",
                    url=link,
                    description="",
                    source=self.name,
                ))
            time.sleep(REQUEST_DELAY)
        print(f"  [{self.name}] Found {len(jobs)} relevant jobs")
        return jobs


class GlassdoorScraper(BaseScraper):
    """Glassdoor — HTML scraping, moderate difficulty"""
    name = "glassdoor"
    base_url = "https://www.glassdoor.com"

    def scrape(self, search_terms, location="Germany"):
        jobs = []
        seen_urls = set()
        for term in search_terms[:5]:
            url = f"{self.base_url}/Job/germany-{quote_plus(term)}-jobs-SRCH_IL.0,7_IN96_KO8,{8+len(term)}.htm"
            resp = self._get(url)
            if not resp:
                continue
            soup = BeautifulSoup(resp.text, "html.parser")
            for card in soup.select("li.react-job-listing, div.jobCard, article[data-id]"):
                title_el = card.select_one("a.jobTitle, a[data-test='job-title']")
                if not title_el:
                    continue
                title = title_el.get_text(strip=True)
                link = title_el.get("href", "")
                if link and not link.startswith("http"):
                    link = f"{self.base_url}{link}"
                if link in seen_urls:
                    continue
                seen_urls.add(link)
                company_el = card.select_one("span.EmployerProfile_compactEmployerName__LE242, div.employer-name")
                location_el = card.select_one("span.job-location, div[data-test='emp-location']")
                company = company_el.get_text(strip=True) if company_el else ""
                loc = location_el.get_text(strip=True) if location_el else ""
                jobs.append(Job(
                    id=self._make_id(title, company, link),
                    title=title,
                    company=company,
                    location=loc,
                    url=link,
                    description="",  # Glassdoor descriptions need clicking through
                    source=self.name,
                ))
            time.sleep(REQUEST_DELAY + 1)
        print(f"  [{self.name}] Found {len(jobs)} relevant jobs")
        return jobs


# ── Search Terms Generator ──────────────────────────────────────────────────

def generate_search_terms(profiles_data: dict) -> list[str]:
    """Build deduplicated search terms from all friend profiles."""
    terms = set()
    for friend in profiles_data["friends"]:
        for role in friend["target_roles"]:
            terms.add(role)
        # Add top keywords combined with location hints
        for kw in friend["keywords"][:5]:
            terms.add(kw)
    # Add some cross-cutting terms
    terms.update([
        "MBA", "business analyst Germany", "consultant Germany",
        "product manager Europe", "marketing manager Germany",
        "startup associate", "founders associate Berlin",
        "risk analyst Frankfurt", "sourcing manager Munich",
    ])
    return list(terms)


# ── Job Qualifier ───────────────────────────────────────────────────────────

class JobQualifier:
    """Analyzes job descriptions and filters based on global criteria."""

    def __init__(self, global_filters: dict):
        self.filters = global_filters
        self._german_patterns = [
            re.compile(p, re.IGNORECASE)
            for p in [
                r"german\s*(c1|c2|native|fluent|mother\s*tongue|muttersprache)",
                r"flie[ßs]end\s*deutsch",
                r"verhandlungssicher\s*deutsch",
                r"deutsch\s*(c1|c2|muttersprachlich)",
                r"german\s*language\s*required",
                r"must\s*speak\s*german",
                r"fluency\s*in\s*german\s*(is\s*)?(required|essential|mandatory)",
            ]
        ]

    def qualifies(self, job: Job) -> tuple[bool, str]:
        """Returns (passes, reason_if_rejected)"""
        text = f"{job.title} {job.description}".lower()

        # Check German language requirement
        full_text = f"{job.title} {job.description}"
        for pattern in self._german_patterns:
            if pattern.search(full_text):
                return False, "German C1+ required"

        # Check excluded roles
        for role in self.filters.get("roles_exclude", []):
            if role.lower() in job.title.lower():
                return False, f"Excluded role: {role}"

        # Check excluded seniority
        for level in self.filters.get("seniority_exclude", []):
            if level.lower() in text:
                return False, f"Excluded seniority: {level}"

        # Check unpaid
        if self.filters.get("exclude_unpaid"):
            if any(w in text for w in ["unpaid", "volunteer position", "no compensation", "unbezahlt"]):
                return False, "Unpaid position"

        return True, ""


# ── Friend Matcher ──────────────────────────────────────────────────────────

class FriendMatcher:
    """Scores jobs against individual friend profiles."""

    def score(self, job: Job, friend: dict) -> dict:
        """Score 0-100 with breakdown."""
        scores = {}
        text = f"{job.title} {job.description} {job.company} {' '.join(job.tags)}".lower()
        title_lower = job.title.lower()
        location_lower = job.location.lower()

        # Role match (0-35)
        role_score = 0
        matched_role = None
        for role in friend["target_roles"]:
            role_words = role.lower().split()
            if all(w in title_lower for w in role_words):
                role_score = 35
                matched_role = role
                break
            elif any(w in title_lower for w in role_words if len(w) > 3):
                role_score = max(role_score, 20)
                matched_role = role
            elif all(w in text for w in role_words):
                role_score = max(role_score, 10)
                matched_role = role
        scores["role"] = role_score

        # Keyword match (0-25)
        kw_hits = sum(1 for kw in friend["keywords"] if kw.lower() in text)
        scores["keywords"] = min(25, int(kw_hits / max(len(friend["keywords"]), 1) * 50))

        # Location match (0-20)
        loc_score = 0
        if any(loc.lower() in location_lower for loc in friend.get("preferred_locations", [])):
            loc_score = 20
        elif any(loc.lower() in location_lower for loc in friend.get("accepted_locations", [])):
            loc_score = 12
        elif "remote" in location_lower:
            loc_score = 15
        elif any(loc.lower() in location_lower for loc in ["germany", "deutschland", "europe"]):
            loc_score = 8
        scores["location"] = loc_score

        # Company type match (0-10)
        company_score = 0
        for ct in friend.get("company_types", []):
            if ct.lower() in text:
                company_score = 10
                break
        scores["company"] = company_score

        # Bonus keywords (0-10)
        bonus_hits = sum(1 for bk in friend.get("bonus_keywords", []) if bk.lower() in text)
        scores["bonus"] = min(10, bonus_hits * 3)

        # Salary check (penalty)
        salary_penalty = 0
        min_salary = friend.get("min_salary")
        if min_salary and job.salary_max:
            if job.salary_max < min_salary:
                salary_penalty = -50  # Hard reject

        total = sum(scores.values()) + salary_penalty
        return {
            "total": max(0, min(100, total)),
            "breakdown": scores,
            "matched_role": matched_role,
            "salary_ok": salary_penalty == 0,
        }


# ── Distribution Engine ────────────────────────────────────────────────────

class Distributor:
    """Picks top 5 unseen jobs per friend, avoids duplicates across days."""

    def __init__(self, seen_file: Path):
        self.seen_file = seen_file
        self.seen = self._load_seen()

    def _load_seen(self) -> dict:
        if self.seen_file.exists():
            with open(self.seen_file) as f:
                return json.load(f)
        return {}

    def _save_seen(self):
        with open(self.seen_file, "w") as f:
            json.dump(self.seen, f, indent=2)

    def pick_jobs(self, friend_id: str, scored_jobs: list[tuple[Job, dict]], n: int = 5) -> list[tuple[Job, dict]]:
        """Pick top N unseen jobs for a friend."""
        if friend_id not in self.seen:
            self.seen[friend_id] = []

        seen_ids = set(self.seen[friend_id])

        # Filter unseen, sort by score descending
        candidates = [
            (job, score_info)
            for job, score_info in scored_jobs
            if job.id not in seen_ids and score_info["total"] >= 25  # Minimum threshold
        ]
        candidates.sort(key=lambda x: x[1]["total"], reverse=True)

        # Pick top N
        picked = candidates[:n]

        # Mark as seen
        for job, _ in picked:
            self.seen[friend_id].append(job.id)

        self._save_seen()
        return picked


# ── Output Generator ────────────────────────────────────────────────────────

def generate_output(friend: dict, picks: list[tuple[Job, dict]], output_dir: Path):
    """Generate CSV and markdown for a friend's daily picks."""
    output_dir.mkdir(parents=True, exist_ok=True)
    date_str = datetime.now().strftime("%Y-%m-%d")
    name_slug = friend["id"]

    # CSV
    csv_path = output_dir / f"{name_slug}_{date_str}.csv"
    with open(csv_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["Rank", "Score", "Title", "Company", "Location", "URL", "Matched Role", "Source", "Why"])
        for i, (job, score_info) in enumerate(picks, 1):
            breakdown = score_info["breakdown"]
            why = f"Role:{breakdown['role']} Kw:{breakdown['keywords']} Loc:{breakdown['location']} Co:{breakdown['company']} Bonus:{breakdown['bonus']}"
            writer.writerow([
                i, score_info["total"], job.title, job.company,
                job.location, job.url, score_info.get("matched_role", ""),
                job.source, why
            ])

    # Markdown summary
    md_path = output_dir / f"{name_slug}_{date_str}.md"
    with open(md_path, "w", encoding="utf-8") as f:
        f.write(f"# Job picks for {friend['name']} — {date_str}\n\n")
        if not picks:
            f.write("No new matching jobs found today. Try broadening search terms or check back tomorrow.\n")
            return csv_path, md_path
        for i, (job, score_info) in enumerate(picks, 1):
            f.write(f"## {i}. {job.title}\n")
            f.write(f"**{job.company}** · {job.location} · Score: {score_info['total']}/100\n\n")
            f.write(f"[Apply here]({job.url})\n\n")
            if job.description:
                summary = job.description[:300]
                if len(job.description) > 300:
                    summary += "..."
                f.write(f"{summary}\n\n")
            f.write(f"*Source: {job.source} · Matched: {score_info.get('matched_role', 'general')}*\n\n---\n\n")

    return csv_path, md_path


# ── Agent Output Hook ───────────────────────────────────────────────────────

AGENT_OUTPUT_DIR = BASE_DIR / "agent_queue"

def prepare_agent_payload(friend: dict, picks: list[tuple[Job, dict]]) -> dict:
    """
    Builds a structured JSON payload for the downstream agent.
    Drop your agent's expected format here — this is the handoff contract.
    """
    payload = {
        "recipient": {
            "id": friend["id"],
            "name": friend["name"],
        },
        "generated_at": datetime.now().isoformat(),
        "job_count": len(picks),
        "jobs": [],
    }
    for rank, (job, score_info) in enumerate(picks, 1):
        payload["jobs"].append({
            "rank": rank,
            "score": score_info["total"],
            "score_breakdown": score_info["breakdown"],
            "matched_role": score_info.get("matched_role", ""),
            "title": job.title,
            "company": job.company,
            "location": job.location,
            "url": job.url,
            "source": job.source,
            "remote": job.remote,
            "salary_min": job.salary_min,
            "salary_max": job.salary_max,
            "description_preview": job.description[:500] if job.description else "",
            "tags": job.tags,
            "posted_date": job.posted_date,
            "why": score_info.get("why", ""),
        })
    return payload


def write_agent_queue(friend: dict, picks: list[tuple[Job, dict]]):
    """
    Writes a JSON file per friend into agent_queue/.
    Your downstream agent should watch this directory (or you push these files
    to its API/repo). Each file is a self-contained delivery payload.
    """
    AGENT_OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    date_str = datetime.now().strftime("%Y-%m-%d")
    payload = prepare_agent_payload(friend, picks)
    out_path = AGENT_OUTPUT_DIR / f"{friend['id']}_{date_str}.json"
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2, ensure_ascii=False)
    return out_path


# ── Main Orchestrator ───────────────────────────────────────────────────────

def main():
    match_only = "--match-only" in sys.argv
    dry_run = "--dry-run" in sys.argv
    use_playwright = "--playwright" in sys.argv or "--pw" in sys.argv
    no_agent = "--no-agent" in sys.argv

    print("=" * 60)
    print("  JOB SCRAPER & MATCHER")
    print(f"  {datetime.now().strftime('%Y-%m-%d %H:%M')}")
    if use_playwright:
        print("  Mode: Playwright (full JS rendering)")
    print("=" * 60)

    # Load profiles
    with open(PROFILES_FILE) as f:
        profiles = json.load(f)

    qualifier = JobQualifier(profiles["global_filters"])
    matcher = FriendMatcher()
    distributor = Distributor(SEEN_FILE)

    all_jobs = []

    if match_only and JOBS_CACHE.exists():
        print("\n[*] Loading cached jobs...")
        with open(JOBS_CACHE) as f:
            all_jobs = [Job.from_dict(j) for j in json.load(f)]
        print(f"    Loaded {len(all_jobs)} cached jobs")
    else:
        # Generate search terms
        search_terms = generate_search_terms(profiles)
        print(f"\n[*] Generated {len(search_terms)} search terms")

        # Initialize scrapers — static HTML boards always run
        scrapers = [
            ArbeitnowScraper(),
            RemoteOKScraper(),
            StartupJobsScraper(),
            EuroTechJobsScraper(),
            IndeedScraper(),
        ]

        # JS-heavy boards: use Playwright if requested, otherwise fall back to static
        if use_playwright:
            try:
                from scraper_playwright import get_playwright_scrapers
                pw_scrapers = get_playwright_scrapers()
                if pw_scrapers:
                    scrapers.extend(pw_scrapers)
                    print(f"  [+] Playwright scrapers loaded: {[s.name for s in pw_scrapers]}")
                else:
                    print("  [!] Playwright not available, using static scrapers for Otta/Glassdoor/WTTJ")
                    scrapers.extend([WelcomeToTheJungleScraper(), OttaScraper(), GlassdoorScraper()])
            except ImportError:
                print("  [!] scraper_playwright.py not found, using static scrapers")
                scrapers.extend([WelcomeToTheJungleScraper(), OttaScraper(), GlassdoorScraper()])
        else:
            scrapers.extend([WelcomeToTheJungleScraper(), OttaScraper(), GlassdoorScraper()])

        # Scrape each board
        for scraper in scrapers:
            print(f"\n[*] Scraping {scraper.name}...")
            try:
                jobs = scraper.scrape(search_terms)
                all_jobs.extend(jobs)
            except Exception as e:
                print(f"  [!] {scraper.name} failed: {e}")

        # Deduplicate by title+company
        seen_key = set()
        deduped = []
        for job in all_jobs:
            key = f"{job.title.lower().strip()}|{job.company.lower().strip()}"
            if key not in seen_key:
                seen_key.add(key)
                deduped.append(job)
        all_jobs = deduped
        print(f"\n[*] Total unique jobs scraped: {len(all_jobs)}")
        print(f"\n[*] Running ATS discovery on {len(all_jobs)} board results...")
        seen_urls = {j.url for j in all_jobs}
        ats_extra = enrich_with_ats(all_jobs, seen_urls=seen_urls)
        all_jobs.extend(ats_extra)
        print(f"  [+] ATS discovery added {len(ats_extra)} new jobs")
        print(f"  [=] Total jobs for matching: {len(all_jobs)}")

        # Qualify
        qualified = []
        rejected_reasons = {}
        for job in all_jobs:
            passes, reason = qualifier.qualifies(job)
            if passes:
                qualified.append(job)
            else:
                rejected_reasons[reason] = rejected_reasons.get(reason, 0) + 1
        all_jobs = qualified

        print(f"[*] After qualification: {len(all_jobs)} jobs")
        if rejected_reasons:
            print("    Rejection breakdown:")
            for reason, count in sorted(rejected_reasons.items(), key=lambda x: -x[1]):
                print(f"      {reason}: {count}")

        # Cache
        if not dry_run:
            with open(JOBS_CACHE, "w") as f:
                json.dump([j.to_dict() for j in all_jobs], f, indent=2)

    # Match and distribute per friend
    print(f"\n[*] Matching {len(all_jobs)} jobs to {len(profiles['friends'])} friends...\n")

    results_summary = []

    ai_matcher = AIJobMatcher()
    results_summary = []
    for friend in profiles["friends"]:
        scored = [(job, matcher.score(job, friend)) for job in all_jobs]
        # Filter by salary
        if friend.get("min_salary"):
            scored = [(j, s) for j, s in scored if s["salary_ok"]]
        # Pre-filter: only send 50+ to AI
        candidates = [(j, s) for j, s in scored if s["total"] >= 50]
        print(f"  [{friend['name']}] {len(candidates)} candidates above 50 → AI scoring...")
        # AI scoring
        if candidates:
            jobs_with_kw = [(j, s["total"]) for j, s in candidates]
            ai_results = ai_matcher.score_batch(jobs_with_kw, friend)
            # Rebuild scored list with blended scores + why
            scored_ai = []
            for ai_r in ai_results:
                job = ai_r["job"]
                # Find original score_info to preserve breakdown
                orig = next((s for j, s in candidates if j is job), {})
                orig["total"] = ai_r["final_score"]
                orig["ai_score"] = ai_r["ai_score"]
                orig["why"] = ai_r["why"]
                scored_ai.append((job, orig))
            scored_ai.sort(key=lambda x: x[1]["total"], reverse=True)
        else:
            scored_ai = []
        picks = distributor.pick_jobs(friend["id"], scored_ai, n=5)

        if dry_run:
            print(f"  [{friend['name']}] Would deliver {len(picks)} jobs (top score: {picks[0][1]['total'] if picks else 0})")
        else:
            csv_path, md_path = generate_output(friend, picks, OUTPUT_DIR)
            print(f"  [{friend['name']}] {len(picks)} jobs → {csv_path.name}")
            for i, (job, score_info) in enumerate(picks, 1):
                why = score_info.get('why', '')
                print(f"    {i}. [{score_info['total']:3d}] {job.title} @ {job.company} ({job.source})")
                if why:
                    print(f"        → {why}")

            # Write agent queue payload
            if not no_agent:
                agent_path = write_agent_queue(friend, picks)
                print(f"    → Agent payload: {agent_path.name}")

            results_summary.append({
                "friend": friend["name"],
                "jobs_delivered": len(picks),
                "top_score": picks[0][1]["total"] if picks else 0,
            })

    # Write run log
    if not dry_run:
        log_file = BASE_DIR / "run_log.jsonl"
        with open(log_file, "a") as f:
            f.write(json.dumps({
                "timestamp": datetime.now().isoformat(),
                "total_scraped": len(all_jobs),
                "results": results_summary,
                "playwright": use_playwright,
            }) + "\n")

    # Print seen stats
    seen_stats = distributor.seen
    print(f"\n[*] Seen jobs memory:")
    for fid, seen_list in seen_stats.items():
        print(f"    {fid}: {len(seen_list)} jobs already sent (will never repeat)")

    print(f"\n{'=' * 60}")
    print(f"  Done! Output in: {OUTPUT_DIR}/")
    if not no_agent:
        print(f"  Agent payloads in: {AGENT_OUTPUT_DIR}/")
    print(f"{'=' * 60}")


if __name__ == "__main__":
    main()

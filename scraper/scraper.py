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
from ats_scraper import requires_german
from supabase_profiles import load_profiles
try:
    sys.path.insert(0, str(Path(__file__).parent.parent))
    from orchestrator import load_strategy, get_search_terms_from_strategy
    ORCHESTRATOR_AVAILABLE = True
except ImportError:
    ORCHESTRATOR_AVAILABLE = False

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

    def qualifies(self, job: Job) -> tuple[bool, str]:
        """Returns (passes, reason_if_rejected)"""
        text = f"{job.title} {job.description}".lower()

        # Check German language requirement (stricter v2 filter)
        if requires_german(job.title, job.description):
            return False, "German required"

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
            why = f"Role:{breakdown.get('role',0)} Kw:{breakdown.get('keywords', breakdown.get('keyword',0))} Loc:{breakdown.get('location',0)} Co:{breakdown.get('company',0)} Bonus:{breakdown.get('bonus',0)}"
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
    profiles = load_profiles()

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
        # Generate search terms — prefer AI strategy if available
        strategy = load_strategy() if ORCHESTRATOR_AVAILABLE else None
        if strategy:
            search_terms = get_search_terms_from_strategy(strategy, profiles)
            print(f"\n[*] Using AI strategy: {len(search_terms)} search terms")
        else:
            search_terms = generate_search_terms(profiles)
            print(f"\n[*] Using static terms: {len(search_terms)} search terms")

        # Initialize scrapers — static HTML boards always run
        scrapers = [
            ArbeitnowScraper(),
            RemoteOKScraper(),
        ]
        
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
        if friend.get("min_salary"):
            scored = [(j, s) for j, s in scored if s["salary_ok"]]
        candidates = [(j, s) for j, s in scored if s["total"] >= 50]
        print(f"  [{friend['name']}] {len(candidates)} candidates above 50 → AI scoring...")
        if candidates:
            jobs_with_kw = [(j, s["total"]) for j, s in candidates]
            ai_results = ai_matcher.score_batch(jobs_with_kw, friend)
            scored_ai = []
            for ai_result in ai_results:
                if ai_result["rejected"] or ai_result["final_score"] == 0:
                    continue
                job = ai_result["job"]
                orig = next((s for j, s in candidates if j is job), {})
                orig["total"] = ai_result["final_score"]
                orig["ai_score"] = ai_result["ai_score"]
                orig["why"] = ai_result["why"]
                orig["fit"] = ai_result["fit"]
                orig["want"] = ai_result["want"]
                scored_ai.append((job, orig))
            scored_ai.sort(key=lambda x: x[1]["total"], reverse=True)
            print(f"  [{friend['name']}] {len(ai_results) - len(scored_ai)} rejected by AI, {len(scored_ai)} final matches")
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
                print(f"    {i}. [{score_info['total']:3d}] (fit:{score_info.get('fit','?')} want:{score_info.get('want','?')}) {job.title} @ {job.company}")
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

def run_backfill(backfill_file: str):
    """ATS-only scrape for specific companies targeting one profile."""
    with open(backfill_file) as f:
        config = json.load(f)
    
    profile_key = config["profile_key"]
    companies = config["companies"]
    loop = config.get("loop", 1)
    
    print(f"\n[*] BACKFILL (loop {loop}) for {profile_key}: {len(companies)} companies")
    
    profiles = load_profiles()
    friend = next((f for f in profiles["friends"] if f["id"] == profile_key), None)
    if not friend:
        print(f"  [!] Profile {profile_key} not found")
        return
    
    from ats_scraper import ATSDiscovery
    
    discovery = ATSDiscovery()
    ats_jobs = discovery.discover_and_scrape(companies, apply_filters=True)
    
    if not ats_jobs:
        print("  [!] No backfill jobs found")
        return
    
    # Convert ATS jobs to Job objects
    all_jobs = []
    for aj in ats_jobs:
        all_jobs.append(Job(
            title=aj.title,
            company=aj.company,
            location=aj.location,
            url=aj.url,
            description=aj.description or "",
            source=f"ats-backfill-{aj.ats_platform}",
        ))
    
    qualifier = JobQualifier(profiles["global_filters"])
    matcher = FriendMatcher()
    distributor = Distributor(SEEN_FILE)
    ai_matcher = AIJobMatcher()
    
    # Qualify
    qualified = [j for j in all_jobs if qualifier.qualifies(j)[0]]
    print(f"  [*] {len(qualified)} qualified after filters")
    
    # Score and match for this one profile
    scored = [(j, matcher.score(j, friend)) for j in qualified]
    candidates = [(j, s) for j, s in scored if s["total"] >= 50]
    
    if candidates:
        jobs_with_kw = [(j, s["total"]) for j, s in candidates]
        ai_results = ai_matcher.score_batch(jobs_with_kw, friend)
        scored_ai = []
        for ai_result in ai_results:
            if ai_result["rejected"] or ai_result["final_score"] == 0:
                continue
            job = ai_result["job"]
            orig = next((s for j, s in candidates if j is job), {})
            orig["total"] = ai_result["final_score"]
            orig["why"] = ai_result["why"]
            scored_ai.append((job, orig))
        
        picks = distributor.pick_jobs(friend["id"], scored_ai, n=5)
        if picks:
            write_agent_queue(friend, picks)
            print(f"  [+] Backfill found {len(picks)} jobs for {friend['name']}")
        else:
            print(f"  [-] No new backfill matches for {friend['name']}")
    else:
        print(f"  [-] No candidates above threshold for {friend['name']}")

if __name__ == "__main__":
    if "--backfill" in sys.argv:
        idx = sys.argv.index("--backfill")
        run_backfill(sys.argv[idx + 1])
    else:
        main()

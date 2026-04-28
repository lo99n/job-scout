"""
JOB SCOUT v4 — Two-Profile Single-Pass Agent
==============================================
- One Sonnet call per profile, searches 10+ sources
- Jobs must be posted within the last 7 days
- German as hard requirement = auto-disqualify
- Never sends the same job twice (seen_jobs.json)
- Processes at 7:30 AM Berlin, sends at 8:30 AM Berlin
- Mon-Fri only. Railway deployment.
"""

import anthropic
import json
import os
import sys
import time
import logging
import schedule
import resend
from datetime import datetime

ANTHROPIC_API_KEY = os.getenv("ANTHROPIC_API_KEY")
RESEND_API_KEY = os.getenv("RESEND_API_KEY")
RESEND_FROM = os.getenv("RESEND_FROM", "Jason de Jobscoot <scout@platypus.farm>")
SEEN_FILE = "seen_jobs.json"
MODEL = "claude-sonnet-4-6"

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("job_scout")


# ═══════════════════════════════════════════════════════════════════
# SHARED RULES
# ═══════════════════════════════════════════════════════════════════
SHARED_RULES = """CRITICAL RULES:
1. Every job MUST have been posted within the last 7 days. No older listings.
2. If a job lists German as a HARD requirement (e.g. "fluent German required", "Deutsch erforderlich", "German C1 required"), DISQUALIFY it immediately. Do not include it. German as a "nice to have" or "plus" is fine.
3. Every job must have a real, working URL. Do not invent URLs.
4. Every job must have a clear location listed.
5. "why_good_fit" must reference the candidate's SPECIFIC experience, not generic statements.

SOURCES — search at least 10 of these (run 10+ searches):
1. berlinstartupjobs.com
2. Wellfound (AngelList)
3. LinkedIn Jobs
4. Greenhouse job boards
5. Lever job boards
6. Indeed Germany
7. Indeed Netherlands
8. Glassdoor
9. Otta.com
10. EuroTechJobs
11. arbeitnow.com
12. startup.jobs
13. Join.com
14. Workable job boards
15. Company career pages directly

Return ONLY a JSON object after all searches. No prose before or after. No markdown.
{
  "date": "Month Day, Year",
  "jobs": [
    {
      "title": "Job title",
      "company": "Company name",
      "location": "City, Country",
      "url": "Direct link to the job posting",
      "why_good_fit": "One sentence referencing the candidate's specific experience"
    }
  ]
}

Return at least 15 jobs, ideally 20. Quality over quantity — only include jobs where the candidate is genuinely a good fit."""


# ═══════════════════════════════════════════════════════════════════
# PROFILES
# ═══════════════════════════════════════════════════════════════════
PROFILES = [
    {
        "key": "lorenzo",
        "email": os.getenv("EMAIL_LORENZO", "lorenzo.nicastro@whu.edu"),
        "name": "Lorenzo Nicastro",
        "system": f"""You are a job search agent. Find the best 20 recently posted jobs for this candidate.

CANDIDATE: Lorenzo Nicastro
- MBA at WHU Otto Beisheim School of Management (09/2025 - present)
- 2.5yr Strategy Consultant at Capgemini: client-facing B2B, content strategy, multi-channel campaigns, KPI reporting
- 8mo Delegate Manager at Richmond Italia: B2B sales, outbound prospecting, CRM (MS Dynamics, 20K profiles), pipeline management, 60+ email campaigns, 200+ C-level stakeholders
- Head of Offline Events at Entrepreneurship Roundtable: event coordination, partnerships, cross-functional ops
- Builds AI agents with Python and Anthropic Claude API (deployed on Railway)
- Digital Marketing Master from TAG Innovation School
- Studied at University of Virginia (USA, 2017-2020)
- Languages: English C2 (native level), Italian C2 (native), Spanish B1, French A2, German A2
- Tools: MS Dynamics CRM, LinkedIn Sales Navigator, Claude API, Python, MS Office, WordPress
- Freelance writer 2020-2023
- ~3.5 years professional experience
- Eligible to work in EU. Lived in US, Italy, Scotland, Germany.

ROLE TYPES: SDR, BDR, Sales, Growth, GTM, Marketing, Partner Management, Channel Management, Ops, Chief of Staff, Founder's Office, Graduate programs

LOCATIONS:
- Berlin (majority, at least 10 of 20 jobs)
- Also allowed: Amsterdam, Copenhagen, Stockholm, Dublin, Hamburg, Munich, Madrid
- London ONLY if visa sponsorship is explicitly mentioned
- NEVER Paris
- Skip jobs with no location listed

SENIORITY: Junior to mid-level (0-5 years)
COMPANIES: Startups, AI companies, scaleups, or companies with strong graduate programs

{SHARED_RULES}""",
    },
    {
        "key": "fernando",
        "email": "fernando.hermoza@whu.edu",
        "name": "Fernando Hermoza Pelizzoli",
        "system": f"""You are a job search agent. Find the best 20 recently posted jobs for this candidate.

CANDIDATE: Fernando Hermoza Pelizzoli
- MBA at WHU Otto Beisheim School of Management (08/2025 - 07/2026)
- 1yr+ Senior Business Consultant at EY Lima (Financial Services): led digital transformation, core banking implementations, PMO, change management, AI chatbot initiatives, generated $1M+ in consulting sales
- 1yr+ PMO Consultant at EY Lima: governance frameworks, KPI tracking, vendor management, trained 10-person client PMO team
- 2.5yr Senior Business Specialist / Product Owner at Pacifico Seguros (Car Insurance): redesigned renewal process (+8pp retention), launched customer service chatbot (+30% satisfaction), data-driven pricing model (+15% growth)
- 3yr Product Analyst at Pacifico Seguros: managed 6K client portfolio, retention campaigns, cross-functional coordination with Commercial, Data, Product, IT teams
- Co-Founded Amage Coffee Roasters (fair trade coffee roastery, $8K invested)
- Bachelor in Business Management from Universidad de Lima
- Languages: Spanish C2 (native), English C1, German A2, Italian A1
- Tools: Jira, MS Project, Power BI, MS Office, Canva
- Skills: PMO, IT implementation governance, product ownership, business analysis, process redesign, stakeholder management, vendor management, KPI/budget tracking
- ~7 years professional experience
- EU Passport

ROLE TYPES: Product Manager, Account Manager, Business Consultant, Consultant, Business Analyst, Project Manager, Program Manager, Transformation Consultant, Business Developer

LOCATIONS:
- Germany (any city: Berlin, Munich, Hamburg, Frankfurt, Dusseldorf, etc.)
- Netherlands (Amsterdam, The Hague, Rotterdam, etc.)
- Denmark (Copenhagen)
- Ireland (Dublin)
- Luxembourg
- Switzerland (Zurich, Geneva, Basel)
- Skip jobs with no location listed

SENIORITY: Mid-level (3-8 years)
COMPANIES: Consulting firms, fintechs, insurance/financial services, tech companies, startups, scaleups, or companies with strong graduate/rotation programs

{SHARED_RULES}""",
    },
]


# ═══════════════════════════════════════════════════════════════════
# DEDUPLICATION
# ═══════════════════════════════════════════════════════════════════
def load_seen():
    if os.path.exists(SEEN_FILE):
        with open(SEEN_FILE, "r") as f:
            return set(json.load(f))
    return set()

def save_seen(seen):
    with open(SEEN_FILE, "w") as f:
        json.dump(list(seen), f)


# ═══════════════════════════════════════════════════════════════════
# CALL CLAUDE
# ═══════════════════════════════════════════════════════════════════
def call_claude(system, user_msg, max_turns=25):
    time.sleep(15)

    client = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)
    messages = [{"role": "user", "content": user_msg}]

    for turn in range(max_turns):
        kwargs = {
            "model": MODEL,
            "max_tokens": 8096,
            "system": system,
            "messages": messages,
            "tools": [{"type": "web_search_20250305", "name": "web_search"}],
        }

        for attempt in range(3):
            try:
                response = client.messages.create(**kwargs)
                break
            except anthropic.RateLimitError:
                wait = 60 * (attempt + 1)
                log.warning(f"    Rate limited, waiting {wait}s ({attempt+1}/3)...")
                time.sleep(wait)
        else:
            log.error("    Rate limit persisted after 3 retries.")
            return ""

        messages.append({"role": "assistant", "content": response.content})

        if response.stop_reason == "end_turn":
            return "\n".join(b.text for b in response.content if b.type == "text").strip()
        elif response.stop_reason == "tool_use":
            tool_results = []
            for block in response.content:
                if block.type == "tool_use":
                    log.info(f"    search: {block.input.get('query', '?')}")
                    tool_results.append({
                        "type": "tool_result",
                        "tool_use_id": block.id,
                        "content": "Search executed. Continue.",
                    })
            messages.append({"role": "user", "content": tool_results})
            time.sleep(10)
        else:
            break

    return ""


def parse_json(text):
    if not text:
        return None
    cleaned = text.replace("```json", "").replace("```", "").strip()
    start = cleaned.find("{")
    if start == -1:
        return None

    depth = 0
    in_str = False
    esc = False

    for i in range(start, len(cleaned)):
        c = cleaned[i]
        if esc:
            esc = False
            continue
        if c == "\\":
            esc = True
            continue
        if c == '"':
            in_str = not in_str
            continue
        if in_str:
            continue
        if c == "{":
            depth += 1
        if c == "}":
            depth -= 1
            if depth == 0:
                try:
                    return json.loads(cleaned[start:i+1])
                except json.JSONDecodeError:
                    return None
    return None


# ═══════════════════════════════════════════════════════════════════
# SEARCH + BUILD RESULTS
# ═══════════════════════════════════════════════════════════════════
def search_for_profile(profile, seen_urls):
    log.info(f"Searching for {profile['name']}...")

    seen_list = "\n".join(list(seen_urls)[:200]) if seen_urls else "(none)"
    today = datetime.now().strftime("%A, %B %d, %Y")

    user_msg = (
        f"Today is {today}. "
        f"Run at least 10 searches across different job boards, role types, and locations. "
        f"Only include jobs posted within the last 7 days. "
        f"Disqualify any job that requires fluent German. "
        f"\n\nDo NOT include any of these URLs (already sent):\n{seen_list}"
        f"\n\nAfter all searches, return the JSON object with at least 15 jobs (ideally 20)."
    )

    text = call_claude(profile["system"], user_msg)
    data = parse_json(text)

    if not data or "jobs" not in data:
        log.warning(f"  First attempt failed for {profile['name']}. Raw (first 500): {text[:500] if text else 'empty'}")
        log.warning("  Retrying in 60s...")
        time.sleep(60)
        text2 = call_claude(profile["system"], user_msg)
        data = parse_json(text2)

    if not data or "jobs" not in data:
        log.error(f"  Failed to get jobs for {profile['name']} after retry.")
        return None

    # Dedup
    original_count = len(data["jobs"])
    data["jobs"] = [j for j in data["jobs"] if j.get("url") and j["url"] not in seen_urls]
    log.info(f"  {profile['name']}: {original_count} found, {len(data['jobs'])} after dedup")

    return data


# ═══════════════════════════════════════════════════════════════════
# EMAIL
# ═══════════════════════════════════════════════════════════════════
def build_email(data, profile_name):
    jobs = data.get("jobs", [])
    date_str = data.get("date", datetime.now().strftime("%B %d, %Y"))

    jobs_html = ""
    for i, j in enumerate(jobs, 1):
        jobs_html += f"""
        <tr><td style="padding:16px;background:#111820;border-radius:8px;border-left:3px solid #a78bfa;">
            <div style="color:#a78bfa;font-size:10px;font-weight:700;">#{i} \u00b7 {j.get('location', '')}</div>
            <div style="margin:4px 0;">
                <a href="{j.get('url', '#')}" style="color:#fff;font-size:14px;font-weight:700;text-decoration:none;">
                    {j.get('title', 'Unknown Role')}
                </a>
            </div>
            <div style="color:#8a9ab0;font-size:12px;margin-bottom:6px;">{j.get('company', '')}</div>
            <div style="color:#00e5a0;font-size:11px;font-style:italic;">{j.get('why_good_fit', '')}</div>
        </td></tr>
        <tr><td style="height:8px;"></td></tr>"""

    MASCOT_URL = "https://raw.githubusercontent.com/lo99n/job-scout/main/jason.png"

    return f"""
    <html><body style="margin:0;padding:0;background:#070a0f;">
    <table width="100%" cellpadding="0" cellspacing="0" style="background:#070a0f;">
    <tr><td align="center" style="padding:20px;">
    <table width="640" cellpadding="0" cellspacing="0" style="font-family:'Courier New',monospace;background:#0a0e14;color:#d4dee8;border-radius:12px;overflow:hidden;">
        <tr><td style="padding:32px 28px 0;" align="center">
            <img src="{MASCOT_URL}" alt="Jason de Jobscoot" width="100" height="100" style="border-radius:50%;margin-bottom:12px;">
            <div style="color:#a78bfa;font-size:10px;letter-spacing:3px;font-weight:700;">DAILY BRIEFING FROM</div>
            <h1 style="color:#fff;font-size:32px;margin:4px 0 0;letter-spacing:-1px;">Jason de Jobscoot</h1>
            <p style="color:#5a6a7a;font-size:11px;letter-spacing:1px;margin:6px 0 0;">{date_str} \u00b7 {len(jobs)} matches for {profile_name} \u00b7 posted in last 7 days</p>
        </td></tr>
        <tr><td style="padding:16px 28px;"><hr style="border:none;border-top:1px solid #1c2530;margin:0;"></td></tr>
        <tr><td style="padding:0 28px;">
            <table width="100%" cellpadding="0" cellspacing="0">{jobs_html}</table>
        </td></tr>
        <tr><td style="padding:28px;text-align:center;">
            <p style="color:#3a4a5a;font-size:10px;letter-spacing:1px;margin:0;">Scouted by Jason de Jobscoot \u00b7 German-required jobs excluded \u00b7 Verify before applying</p>
        </td></tr>
    </table></td></tr></table></body></html>"""


def send_email(html, count, to_email, profile_name):
    resend.api_key = RESEND_API_KEY
    date_str = datetime.now().strftime("%B %d, %Y")
    params = {
        "from": RESEND_FROM,
        "to": [to_email],
        "subject": f"\U0001f9a6 Jason found {count} jobs for {profile_name} ({date_str})",
        "html": html,
    }
    result = resend.Emails.send(params)
    log.info(f"Email sent to {to_email} (id: {result.get('id', 'unknown')})")


# ═══════════════════════════════════════════════════════════════════
# MAIN PIPELINE
# ═══════════════════════════════════════════════════════════════════
pending_results = {}


def process_jobs():
    """Runs at 7:30 AM. Searches and stores results."""
    log.info("=" * 60)
    log.info("JOB SCOUT v4 — PROCESSING (7:30)")
    log.info("=" * 60)

    global pending_results
    pending_results = {}

    try:
        seen = load_seen()
        log.info(f"Previously seen: {len(seen)} jobs")

        for profile in PROFILES:
            data = search_for_profile(profile, seen)
            if data and data.get("jobs"):
                pending_results[profile["key"]] = {
                    "data": data,
                    "profile": profile,
                }
                # Add to seen immediately so second profile doesn't get same jobs
                for j in data["jobs"]:
                    if j.get("url"):
                        seen.add(j["url"])

        # Save seen list
        save_seen(seen)
        log.info(f"Seen list updated: {len(seen)} total")
        log.info("Processing complete. Emails queued for 8:30.")

    except Exception as e:
        log.error(f"Processing failed: {e}", exc_info=True)


def send_emails():
    """Runs at 8:30 AM. Sends the stored results."""
    log.info("=" * 60)
    log.info("JOB SCOUT v4 — SENDING EMAILS (8:30)")
    log.info("=" * 60)

    global pending_results

    if not pending_results:
        log.info("No results to send.")
        return

    for key, entry in pending_results.items():
        data = entry["data"]
        profile = entry["profile"]
        jobs = data.get("jobs", [])
        if not jobs:
            log.info(f"  No jobs for {profile['name']}, skipping.")
            continue
        html = build_email(data, profile["name"])
        send_email(html, len(jobs), profile["email"], profile["name"])

    pending_results = {}
    log.info("All emails sent.")


# ═══════════════════════════════════════════════════════════════════
# ENTRYPOINT
# ═══════════════════════════════════════════════════════════════════
if __name__ == "__main__":
    if not ANTHROPIC_API_KEY:
        print("ERROR: Set ANTHROPIC_API_KEY")
        sys.exit(1)
    if not RESEND_API_KEY:
        print("ERROR: Set RESEND_API_KEY")
        sys.exit(1)

    if "--now" in sys.argv:
        log.info("Running full pipeline NOW")
        process_jobs()
        send_emails()

    elif "--reset" in sys.argv:
        if os.path.exists(SEEN_FILE):
            os.remove(SEEN_FILE)
            print("Seen jobs cleared.")
        else:
            print("No seen jobs to clear.")

    else:
        log.info("Job Scout v4 is running!")
        log.info("Processing: Mon-Fri 7:30 AM Berlin")
        log.info("Sending: Mon-Fri 8:30 AM Berlin")
        for p in PROFILES:
            log.info(f"  {p['name']} -> {p['email']}")
        log.info("Waiting...\n")

        for day in ["monday", "tuesday", "wednesday", "thursday", "friday"]:
            getattr(schedule.every(), day).at("07:30").do(process_jobs)
            getattr(schedule.every(), day).at("08:30").do(send_emails)

        while True:
            schedule.run_pending()
            time.sleep(30)

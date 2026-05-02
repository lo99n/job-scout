"""
JOB SCOUT v5 — Email Dispatcher
=================================
- Receives job data from an external agent (JSON files in inbox/)
- Deduplicates against seen_jobs per profile
- Builds and sends styled HTML emails
- Embeds mascot image as base64 (no broken images)
- Processes at 7:30 AM Berlin, sends at 8:30 AM Berlin
- Mon-Fri only. Railway deployment.

INPUT FORMAT:
  Place JSON files in inbox/<profile_key>.json with structure:
  {
    "jobs": [
      {
        "title": "Job title",
        "company": "Company name",
        "location": "City, Country",
        "url": "https://...",
        "why_good_fit": "One sentence"
      }
    ]
  }
  Valid profile keys: lorenzo, fernando, maria, regina
"""

import json
import os
import sys
import base64
import logging
import schedule
import resend
from datetime import datetime

RESEND_API_KEY = os.getenv("RESEND_API_KEY")
RESEND_FROM = os.getenv("RESEND_FROM", "Jason de Jobscoot <scout@platypus.farm>")
SEEN_DIR = "seen_jobs"
INBOX_DIR = "inbox"
MASCOT_PATH = "jason.png"

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("job_scout")


# ═══════════════════════════════════════════════════════════════════
# PROFILES
# ═══════════════════════════════════════════════════════════════════
PROFILES = {
    "lorenzo": {
        "email": os.getenv("EMAIL_LORENZO", "lorenzo.nicastro@whu.edu"),
        "name": "Lorenzo Nicastro",
    },
    "fernando": {
        "email": os.getenv("EMAIL_FERNANDO", "fernando.hermoza@whu.edu"),
        "name": "Fernando Hermoza Pelizzoli",
    },
    "maria": {
        "email": os.getenv("EMAIL_MARIA", "maria.herbozo@whu.edu"),
        "name": "Maria Teresa Herbozo Debernardi",
    },
    "regina": {
        "email": os.getenv("EMAIL_REGINA", "regina.rubio-aguilar@whu.edu"),
        "name": "Regina Rubio Aguilar",
    },
}


# ═══════════════════════════════════════════════════════════════════
# MASCOT IMAGE
# ═══════════════════════════════════════════════════════════════════
def load_mascot_base64():
    """Load jason.png and return a base64 data URI for embedding in email."""
    if not os.path.exists(MASCOT_PATH):
        log.warning(f"Mascot image not found at {MASCOT_PATH}. Email will skip the image.")
        return None
    with open(MASCOT_PATH, "rb") as f:
        encoded = base64.b64encode(f.read()).decode("utf-8")
    return f"data:image/png;base64,{encoded}"


MASCOT_DATA_URI = None  # loaded once at startup


# ═══════════════════════════════════════════════════════════════════
# DEDUPLICATION
# ═══════════════════════════════════════════════════════════════════
def load_seen(profile_key):
    filepath = f"{SEEN_DIR}/{profile_key}.json"
    if os.path.exists(filepath):
        with open(filepath, "r") as f:
            return set(json.load(f))
    return set()


def save_seen(profile_key, seen):
    os.makedirs(SEEN_DIR, exist_ok=True)
    filepath = f"{SEEN_DIR}/{profile_key}.json"
    with open(filepath, "w") as f:
        json.dump(list(seen), f)


# ═══════════════════════════════════════════════════════════════════
# INBOX READER
# ═══════════════════════════════════════════════════════════════════
def read_inbox(profile_key):
    """Read and consume a profile's inbox JSON file. Returns job list or None."""
    filepath = f"{INBOX_DIR}/{profile_key}.json"
    if not os.path.exists(filepath):
        return None

    try:
        with open(filepath, "r") as f:
            data = json.load(f)
        os.remove(filepath)  # consume the file
        jobs = data.get("jobs", [])
        return jobs if jobs else None
    except (json.JSONDecodeError, IOError) as e:
        log.error(f"Failed to read inbox for {profile_key}: {e}")
        return None


# ═══════════════════════════════════════════════════════════════════
# EMAIL
# ═══════════════════════════════════════════════════════════════════
def build_email(jobs, profile_name, date_str):
    mascot_img = ""
    if MASCOT_DATA_URI:
        mascot_img = f'<img src="{MASCOT_DATA_URI}" alt="Jason de Jobscoot" width="100" height="100" style="border-radius:50%;margin-bottom:12px;">'

    jobs_html = ""
    for i, j in enumerate(jobs, 1):
        jobs_html += f"""
        <tr><td style="padding:16px;background:#111820;border-radius:8px;border-left:3px solid #a78bfa;">
            <div style="color:#a78bfa;font-size:10px;font-weight:700;">#{i} · {j.get('location', '')}</div>
            <div style="margin:4px 0;">
                <a href="{j.get('url', '#')}" style="color:#fff;font-size:14px;font-weight:700;text-decoration:none;">
                    {j.get('title', 'Unknown Role')}
                </a>
            </div>
            <div style="color:#8a9ab0;font-size:12px;margin-bottom:6px;">{j.get('company', '')}</div>
            <div style="color:#00e5a0;font-size:11px;font-style:italic;">{j.get('why_good_fit', '')}</div>
        </td></tr>
        <tr><td style="height:8px;"></td></tr>"""

    return f"""
    <html><body style="margin:0;padding:0;background:#070a0f;">
    <table width="100%" cellpadding="0" cellspacing="0" style="background:#070a0f;">
    <tr><td align="center" style="padding:20px;">
    <table width="640" cellpadding="0" cellspacing="0" style="font-family:'Courier New',monospace;background:#0a0e14;color:#d4dee8;border-radius:12px;overflow:hidden;">
        <tr><td style="padding:32px 28px 0;" align="center">
            {mascot_img}
            <div style="color:#a78bfa;font-size:10px;letter-spacing:3px;font-weight:700;">DAILY BRIEFING FROM</div>
            <h1 style="color:#fff;font-size:32px;margin:4px 0 0;letter-spacing:-1px;">Jason de Jobscoot</h1>
            <p style="color:#5a6a7a;font-size:11px;letter-spacing:1px;margin:6px 0 0;">{date_str} · {len(jobs)} matches for {profile_name} · posted in last 7 days</p>
        </td></tr>
        <tr><td style="padding:16px 28px;"><hr style="border:none;border-top:1px solid #1c2530;margin:0;"></td></tr>
        <tr><td style="padding:0 28px;">
            <table width="100%" cellpadding="0" cellspacing="0">{jobs_html}</table>
        </td></tr>
        <tr><td style="padding:28px;text-align:center;">
            <p style="color:#3a4a5a;font-size:10px;letter-spacing:1px;margin:0;">Scouted by Jason de Jobscoot · German-required jobs excluded · Verify before applying</p>
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
    """Runs at 7:30 AM. Reads inbox files, deduplicates, stores results."""
    log.info("=" * 60)
    log.info("JOB SCOUT v5 — PROCESSING (7:30)")
    log.info("=" * 60)

    global pending_results
    pending_results = {}

    try:
        for key, profile in PROFILES.items():
            seen = load_seen(key)
            log.info(f"Reading inbox for {profile['name']} (seen: {len(seen)})")

            jobs = read_inbox(key)
            if not jobs:
                log.info(f"  No inbox file for {profile['name']}, skipping.")
                continue

            original_count = len(jobs)
            jobs = [j for j in jobs if j.get("url") and j["url"] not in seen]
            log.info(f"  {profile['name']}: {original_count} received, {len(jobs)} after dedup")

            if jobs:
                pending_results[key] = {"jobs": jobs, "profile": profile}
                for j in jobs:
                    if j.get("url"):
                        seen.add(j["url"])
                save_seen(key, seen)

        log.info("Processing complete. Emails queued for 8:30.")

    except Exception as e:
        log.error(f"Processing failed: {e}", exc_info=True)


def send_emails():
    """Runs at 8:30 AM. Sends the stored results."""
    log.info("=" * 60)
    log.info("JOB SCOUT v5 — SENDING EMAILS (8:30)")
    log.info("=" * 60)

    global pending_results

    if not pending_results:
        log.info("No results to send.")
        return

    date_str = datetime.now().strftime("%B %d, %Y")

    for key, entry in pending_results.items():
        jobs = entry["jobs"]
        profile = entry["profile"]
        if not jobs:
            continue
        html = build_email(jobs, profile["name"], date_str)
        send_email(html, len(jobs), profile["email"], profile["name"])

    pending_results = {}
    log.info("All emails sent.")


# ═══════════════════════════════════════════════════════════════════
# ENTRYPOINT
# ═══════════════════════════════════════════════════════════════════
if __name__ == "__main__":
    if not RESEND_API_KEY:
        print("ERROR: Set RESEND_API_KEY")
        sys.exit(1)

    # Load mascot once
    MASCOT_DATA_URI = load_mascot_base64()

    # Ensure inbox dir exists
    os.makedirs(INBOX_DIR, exist_ok=True)

    if "--now" in sys.argv:
        log.info("Running full pipeline NOW")
        process_jobs()
        send_emails()

    elif "--reset" in sys.argv:
        import shutil
        if os.path.exists(SEEN_DIR):
            shutil.rmtree(SEEN_DIR)
            print("All seen jobs cleared.")
        else:
            print("No seen jobs to clear.")

    else:
        log.info("Job Scout v5 — Email Dispatcher running!")
        log.info("Processing: Mon-Fri 7:30 AM Berlin")
        log.info("Sending:    Mon-Fri 8:30 AM Berlin")
        for key, p in PROFILES.items():
            log.info(f"  {p['name']} -> {p['email']}")
        log.info(f"Inbox dir: {INBOX_DIR}/")
        log.info("Waiting...\n")

        for day in ["monday", "tuesday", "wednesday", "thursday", "friday"]:
            getattr(schedule.every(), day).at("07:30").do(process_jobs)
            getattr(schedule.every(), day).at("08:30").do(send_emails)

        while True:
            schedule.run_pending()
            import time
            time.sleep(30)

"""
JOB PIPELINE — Combined Orchestrator
======================================
Runs the full pipeline in one Railway container:

  7:00 AM Berlin  → Scraper runs, scores jobs, writes agent_queue/
                   → Bridge transforms agent_queue/ → inbox/
  7:30 AM Berlin  → Scout reads inbox/, deduplicates, queues emails
  8:30 AM Berlin  → Scout sends emails

Mon-Fri only.

Usage:
  python main.py              # Start scheduled pipeline
  python main.py --now        # Run full pipeline immediately
  python main.py --reset      # Clear all seen jobs
  python main.py --dry-run    # Full run, no emails sent
"""

import os
import sys
import logging
import schedule
import time
import subprocess
from datetime import datetime

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
log = logging.getLogger("pipeline")

# Resolve paths relative to this file
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
SCRAPER_SCRIPT = os.path.join(BASE_DIR, "scraper", "scraper.py")
BRIDGE_SCRIPT = os.path.join(BASE_DIR, "bridge.py")
SCOUT_SCRIPT = os.path.join(BASE_DIR, "job_scout.py")

# Shared directories
AGENT_QUEUE_DIR = os.path.join(BASE_DIR, "scraper", "agent_queue")
INBOX_DIR = os.path.join(BASE_DIR, "inbox")

# Ensure dirs exist
os.makedirs(INBOX_DIR, exist_ok=True)


def run_script(script_path, args=None, label=""):
    """Run a Python script as a subprocess."""
    cmd = [sys.executable, script_path] + (args or [])
    log.info(f"{'─' * 50}")
    log.info(f"RUNNING: {label or script_path}")
    log.info(f"CMD: {' '.join(cmd)}")
    log.info(f"{'─' * 50}")

    try:
        result = subprocess.run(
            cmd,
            cwd=os.path.dirname(script_path),
            capture_output=False,  # let output flow to Railway logs
            timeout=600,  # 10 min max per step
        )
        if result.returncode != 0:
            log.error(f"{label} exited with code {result.returncode}")
            return False
        log.info(f"{label} completed successfully")
        return True
    except subprocess.TimeoutExpired:
        log.error(f"{label} timed out after 600s")
        return False
    except Exception as e:
        log.error(f"{label} failed: {e}")
        return False


def step_1_scrape(use_playwright=True, dry_run=False):
    """Run the scraper to find and score jobs."""
    args = []
    if use_playwright:
        args.append("--playwright")
    if dry_run:
        args.append("--dry-run")
    return run_script(SCRAPER_SCRIPT, args, "STEP 1: Scraper")


def step_2_bridge(dry_run=False):
    """Transform scraper output into scout inbox format."""
    args = [
        "--scraper-dir", AGENT_QUEUE_DIR,
        "--scout-dir", INBOX_DIR,
    ]
    if dry_run:
        args.append("--dry-run")
    return run_script(BRIDGE_SCRIPT, args, "STEP 2: Bridge")


def step_3_scout_process():
    """Tell the scout to process inbox (import and call directly)."""
    log.info(f"{'─' * 50}")
    log.info("STEP 3: Scout — Processing inbox")
    log.info(f"{'─' * 50}")

    # Import scout's process function
    sys.path.insert(0, BASE_DIR)
    try:
        import job_scout
        # Load mascot if not done
        if job_scout.MASCOT_DATA_URI is None:
            job_scout.MASCOT_DATA_URI = job_scout.load_mascot_base64()
        job_scout.process_jobs()
        log.info("Scout processing complete")
        return True
    except Exception as e:
        log.error(f"Scout processing failed: {e}", exc_info=True)
        return False


def step_4_scout_send():
    """Tell the scout to send queued emails."""
    log.info(f"{'─' * 50}")
    log.info("STEP 4: Scout — Sending emails")
    log.info(f"{'─' * 50}")

    sys.path.insert(0, BASE_DIR)
    try:
        import job_scout
        job_scout.send_emails()
        log.info("Scout sending complete")
        return True
    except Exception as e:
        log.error(f"Scout sending failed: {e}", exc_info=True)
        return False


def full_pipeline(dry_run=False):
    """Run the complete pipeline: scrape → bridge → process → send."""
    log.info("=" * 60)
    log.info("JOB PIPELINE — FULL RUN")
    log.info(f"Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S %Z')}")
    log.info("=" * 60)

    # Step 1: Scrape
    if not step_1_scrape(dry_run=dry_run):
        log.error("Scraper failed. Continuing with bridge in case there are leftover payloads.")

    # Step 2: Bridge
    if not step_2_bridge(dry_run=dry_run):
        log.error("Bridge failed. Checking if inbox has data from a previous run.")

    # Step 3: Process inbox
    if not dry_run:
        step_3_scout_process()

    log.info("")
    log.info("Pipeline steps 1-3 complete. Emails queued.")
    log.info("Emails will send in the next send window.")
    log.info("=" * 60)


def morning_pipeline():
    """Scheduled morning run: scrape + bridge + process."""
    full_pipeline(dry_run=False)


def morning_send():
    """Scheduled morning send."""
    step_4_scout_send()


if __name__ == "__main__":
    dry_run = "--dry-run" in sys.argv

    if "--now" in sys.argv:
        log.info("Running full pipeline NOW (immediate mode)")
        full_pipeline(dry_run=dry_run)
        if not dry_run:
            step_4_scout_send()
        sys.exit(0)

    if "--reset" in sys.argv:
        import shutil
        # Reset scraper seen
        scraper_seen = os.path.join(BASE_DIR, "scraper", "seen_jobs.json")
        if os.path.exists(scraper_seen):
            os.remove(scraper_seen)
            print("Scraper seen_jobs cleared.")
        # Reset scout seen
        scout_seen = os.path.join(BASE_DIR, "seen_jobs")
        if os.path.exists(scout_seen):
            shutil.rmtree(scout_seen)
            print("Scout seen_jobs cleared.")
        print("All memory reset.")
        sys.exit(0)

    # ── Scheduled mode (Railway) ──────────────────────────────
    log.info("=" * 60)
    log.info("JOB PIPELINE — Scheduled Mode")
    log.info("  Scrape + Bridge + Process: Mon-Fri 7:00 AM Berlin")
    log.info("  Send emails:              Mon-Fri 8:30 AM Berlin")
    log.info("=" * 60)

    # Set timezone for schedule library
    os.environ["TZ"] = "Europe/Berlin"
    try:
        time.tzset()
    except AttributeError:
        pass  # Windows doesn't have tzset

    for day in ["monday", "tuesday", "wednesday", "thursday", "friday"]:
        getattr(schedule.every(), day).at("07:00").do(morning_pipeline)
        getattr(schedule.every(), day).at("08:30").do(morning_send)

    log.info("Scheduler running. Waiting for next trigger...\n")

    while True:
        schedule.run_pending()
        time.sleep(30)

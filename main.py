"""
JOB PIPELINE — Combined Orchestrator
======================================
Runs the full pipeline in one Railway container:

  Step 0:   AI Orchestrator generates search strategy
  Step 1:   Scraper runs with AI-generated terms + ATS seeds
  Step 1.5: Check match counts, backfill if needed (max 2 loops)
  Step 2:   Bridge transforms agent_queue/ → inbox/
  Step 3:   Scout reads inbox/, deduplicates, queues emails
  Step 4:   Scout sends emails

Mon-Fri only.

Usage:
  python main.py              # Start scheduled pipeline
  python main.py --now        # Run full pipeline immediately
  python main.py --reset      # Clear all seen jobs
  python main.py --dry-run    # Full run, no emails sent
"""

import os
import sys
import json
import logging
import schedule
import time
import subprocess
import glob
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

# Import orchestrator
sys.path.insert(0, BASE_DIR)
try:
    from orchestrator import (
        generate_search_strategy,
        generate_backfill_companies,
        check_backfill_needed,
        load_strategy,
        get_ats_seeds_from_strategy,
        MIN_MATCHES,
        MAX_BACKFILL_LOOPS,
    )
    ORCHESTRATOR_AVAILABLE = True
except ImportError:
    log.warning("Orchestrator module not found. Running without AI strategy.")
    ORCHESTRATOR_AVAILABLE = False


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
            capture_output=False,
            timeout=1800,
        )
        if result.returncode != 0:
            log.error(f"{label} exited with code {result.returncode}")
            return False
        log.info(f"{label} completed successfully")
        return True
    except subprocess.TimeoutExpired:
        log.error(f"{label} timed out after 1800s")
        return False
    except Exception as e:
        log.error(f"{label} failed: {e}")
        return False


def step_0_orchestrate(profiles):
    """AI generates search strategy based on all profiles."""
    if not ORCHESTRATOR_AVAILABLE:
        log.info("Orchestrator not available, using static search terms.")
        return None

    log.info(f"{'─' * 50}")
    log.info("STEP 0: AI Orchestrator — Generating search strategy")
    log.info(f"{'─' * 50}")

    strategy = generate_search_strategy(profiles)
    if strategy:
        log.info("Strategy generated successfully.")
    else:
        log.warning("Strategy generation failed. Scraper will use static terms.")
    return strategy


def step_1_scrape(use_playwright=False, dry_run=False):
    """Run the scraper to find and score jobs."""
    args = []
    if use_playwright:
        args.append("--playwright")
    if dry_run:
        args.append("--dry-run")
    return run_script(SCRAPER_SCRIPT, args, "STEP 1: Scraper")


def step_1_5_backfill(profiles, strategy):
    """
    Check match counts from agent_queue. If any profile got < 4 matches,
    generate targeted companies and run ATS-only scrape for them.
    Max 2 loops.
    """
    if not ORCHESTRATOR_AVAILABLE:
        return

    log.info(f"{'─' * 50}")
    log.info("STEP 1.5: Checking match counts for backfill")
    log.info(f"{'─' * 50}")

    # Read match counts from agent_queue files
    match_counts = {}
    profile_map = {}  # key -> full profile
    for p in profiles:
        key = p.get("profile_key", p.get("id", ""))
        profile_map[key] = p
        # Check agent_queue for this profile's results
        pattern = os.path.join(AGENT_QUEUE_DIR, f"{key}_*.json")
        files = glob.glob(pattern)
        count = 0
        for f in files:
            try:
                with open(f, "r") as fh:
                    data = json.load(fh)
                count += len(data.get("jobs", []))
            except (json.JSONDecodeError, IOError):
                pass
        match_counts[key] = count
        log.info(f"  {p['name']}: {count} matches")

    # Check who needs backfill
    needs_backfill = check_backfill_needed(match_counts)
    if not needs_backfill:
        log.info("All profiles have enough matches. No backfill needed.")
        return

    # Backfill loops
    already_tried = {}  # track companies per profile across loops
    for loop in range(1, MAX_BACKFILL_LOOPS + 1):
        if not needs_backfill:
            break

        log.info(f"\n  Backfill loop {loop}/{MAX_BACKFILL_LOOPS}")

        for key, needed in needs_backfill.items():
            profile = profile_map.get(key)
            if not profile:
                continue

            if key not in already_tried:
                already_tried[key] = []

            # Get existing ATS seeds from strategy
            existing_seeds = get_ats_seeds_from_strategy(strategy, key) if strategy else []
            already_tried[key].extend(existing_seeds)

            # Ask AI for targeted companies
            companies = generate_backfill_companies(
                profile, 
                match_counts[key],
                already_tried[key]
            )
            if not companies:
                log.info(f"    No backfill companies generated for {profile['name']}")
                continue

            already_tried[key].extend(companies)

            # Write backfill companies to a file the scraper can read
            backfill_file = os.path.join(BASE_DIR, "scraper", "backfill_companies.json")
            with open(backfill_file, "w") as f:
                json.dump({
                    "profile_key": key,
                    "companies": companies,
                    "loop": loop,
                }, f, indent=2)

            # Run scraper in backfill mode (ATS-only for specific companies)
            log.info(f"    Running ATS-only scrape for {profile['name']} ({len(companies)} companies)")
            run_script(
                SCRAPER_SCRIPT,
                ["--backfill", backfill_file],
                f"BACKFILL {loop}: {profile['name']}"
            )

            # Clean up
            if os.path.exists(backfill_file):
                os.remove(backfill_file)

        # Re-check counts
        for key in list(needs_backfill.keys()):
            pattern = os.path.join(AGENT_QUEUE_DIR, f"{key}_*.json")
            files = glob.glob(pattern)
            count = 0
            for f in files:
                try:
                    with open(f, "r") as fh:
                        data = json.load(fh)
                    count += len(data.get("jobs", []))
                except (json.JSONDecodeError, IOError):
                    pass
            match_counts[key] = count

        needs_backfill = check_backfill_needed(match_counts)
        if not needs_backfill:
            log.info("  All profiles now have enough matches.")


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
    """Tell the scout to process inbox."""
    log.info(f"{'─' * 50}")
    log.info("STEP 3: Scout — Processing inbox")
    log.info(f"{'─' * 50}")

    sys.path.insert(0, BASE_DIR)
    try:
        import job_scout
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


def load_profiles():
    """Load profiles from Supabase (via scraper module) or fallback."""
    try:
        sys.path.insert(0, os.path.join(BASE_DIR, "scraper"))
        from supabase_profiles import load_profiles as sp_load
        profiles = sp_load()
        if profiles:
            log.info(f"Loaded {len(profiles)} profiles from Supabase")
            return profiles
    except Exception as e:
        log.warning(f"Supabase profile load failed: {e}")

    # Fallback to profiles.json
    profiles_file = os.path.join(BASE_DIR, "scraper", "profiles.json")
    if os.path.exists(profiles_file):
        with open(profiles_file) as f:
            data = json.load(f)
        return data.get("friends", [])
    return []


def full_pipeline(dry_run=False):
    """Run the complete pipeline: orchestrate → scrape → backfill → bridge → process → send."""
    log.info("=" * 60)
    log.info("JOB PIPELINE — FULL RUN")
    log.info(f"Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S %Z')}")
    log.info("=" * 60)

    # Load profiles for orchestrator
    profiles = load_profiles()

    # Step 0: AI Strategy
    strategy = None
    if ORCHESTRATOR_AVAILABLE and profiles:
        strategy = step_0_orchestrate(profiles.get("friends", profiles) if isinstance(profiles, dict) else profiles)

    # Step 1: Scrape (uses ai_strategy.json if present)
    if not step_1_scrape(use_playwright=False, dry_run=dry_run):
        log.error("Scraper failed. Continuing with bridge in case there are leftover payloads.")

    # Step 1.5: Backfill (check counts, targeted ATS for underserved profiles)
    if ORCHESTRATOR_AVAILABLE and profiles and not dry_run:
        profile_list = profiles.get("friends", profiles) if isinstance(profiles, dict) else profiles
        step_1_5_backfill(profile_list, strategy)

    # Step 2: Bridge
    if not step_2_bridge(dry_run=dry_run):
        log.error("Bridge failed. Checking if inbox has data from a previous run.")

    # Step 3: Process inbox
    if not dry_run:
        step_3_scout_process()

    log.info("")
    log.info("Pipeline steps 0-3 complete. Emails queued.")
    log.info("Emails will send in the next send window.")
    log.info("=" * 60)


def morning_pipeline():
    """Scheduled morning run."""
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
        scraper_seen = os.path.join(BASE_DIR, "scraper", "seen_jobs.json")
        if os.path.exists(scraper_seen):
            os.remove(scraper_seen)
            print("Scraper seen_jobs cleared.")
        scout_seen = os.path.join(BASE_DIR, "seen_jobs")
        if os.path.exists(scout_seen):
            shutil.rmtree(scout_seen)
            print("Scout seen_jobs cleared.")
        strategy_file = os.path.join(BASE_DIR, "scraper", "ai_strategy.json")
        if os.path.exists(strategy_file):
            os.remove(strategy_file)
            print("AI strategy cleared.")
        print("All memory reset.")
        sys.exit(0)

    # — Scheduled mode (Railway) ——————————————————————————————
    log.info("=" * 60)
    log.info("JOB PIPELINE — Scheduled Mode")
    log.info("  Orchestrate + Scrape + Backfill + Bridge + Process: Mon-Fri 7:00 AM Berlin")
    log.info("  Send emails:                                       Mon-Fri 8:30 AM Berlin")
    log.info("=" * 60)

    os.environ["TZ"] = "Europe/Berlin"
    try:
        time.tzset()
    except AttributeError:
        pass

    for day in ["monday", "tuesday", "wednesday", "thursday", "friday"]:
        getattr(schedule.every(), day).at("07:00").do(morning_pipeline)
        getattr(schedule.every(), day).at("08:30").do(morning_send)

    log.info("Scheduler running. Waiting for next trigger...\n")

    while True:
        schedule.run_pending()
        time.sleep(30)

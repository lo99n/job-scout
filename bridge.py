"""
BRIDGE — Connects the Job Scraper to the Job Scout Email Dispatcher
====================================================================
Reads JSON payloads from the scraper's agent_queue/ directory,
transforms them into the format expected by the email dispatcher's inbox/,
and writes them there.

Run this AFTER the scraper (7:30 AM) and BEFORE the dispatcher processes (7:30 AM).
Or: run both on the same machine and schedule this in between.

Usage:
  python bridge.py                          # Standard run
  python bridge.py --scraper-dir /path/to   # Custom scraper output dir
  python bridge.py --scout-dir /path/to     # Custom scout inbox dir
  python bridge.py --dry-run                # Preview without writing
  python bridge.py --keep                   # Don't delete consumed files

Directory defaults (same machine):
  Scraper output:  ./job-scraper/agent_queue/
  Scout inbox:     ./inbox/
"""

import json
import os
import sys
import glob
import logging
from datetime import datetime

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
log = logging.getLogger("bridge")

# ── Defaults ────────────────────────────────────────────────────
DEFAULT_SCRAPER_DIR = os.path.join(os.path.dirname(__file__), "job-scraper", "agent_queue")
DEFAULT_SCOUT_DIR = os.path.join(os.path.dirname(__file__), "inbox")


def chain_from_scraper(agent_queue_dir: str, scout_inbox_dir: str, dry_run: bool = False):
    """
    Convenience entry point for calling the bridge from scraper.py's main().
    Consumes agent_queue files and writes to scout inbox in one step.
    """
    run_bridge(
        scraper_dir=agent_queue_dir,
        scout_dir=scout_inbox_dir,
        dry_run=dry_run,
        keep=False,
    )


def transform_job(job: dict) -> dict:
    """
    Transform a scraper job object into the scout's expected format.

    Scraper outputs:
      rank, score, score_breakdown, matched_role, title, company,
      location, url, source, remote, salary_min, salary_max,
      description_preview, tags, posted_date

    Scout expects:
      title, company, location, url, why_good_fit
    """
    # Build a human-readable "why_good_fit" from the scraper's scoring data
    parts = []

    matched_role = job.get("matched_role", "")
    if matched_role:
        parts.append(f"Matched role: {matched_role}")

    score = job.get("score", 0)
    if score:
        parts.append(f"Score: {score}/100")

    # Add source and salary if available
    source = job.get("source", "")
    if source:
        parts.append(f"via {source}")

    salary_min = job.get("salary_min")
    salary_max = job.get("salary_max")
    if salary_min and salary_max:
        parts.append(f"€{salary_min:,}-{salary_max:,}")
    elif salary_max:
        parts.append(f"up to €{salary_max:,}")

    if job.get("remote"):
        parts.append("Remote")

    # Use AI-generated "why" if available, otherwise build from score data
    why = job.get("why", "")
    if not why:
        why = " · ".join(parts) if parts else "Matched by scraper"

    return {
        "title": job.get("title", "Unknown Role"),
        "company": job.get("company", "Unknown Company"),
        "location": job.get("location", ""),
        "url": job.get("url", ""),
        "why_good_fit": why,
    }


def run_bridge(scraper_dir: str, scout_dir: str, dry_run: bool = False, keep: bool = False):
    """Main bridge logic."""
    log.info("=" * 60)
    log.info("BRIDGE — Scraper → Scout")
    log.info(f"  From: {scraper_dir}")
    log.info(f"  To:   {scout_dir}")
    log.info("=" * 60)

    if not os.path.exists(scraper_dir):
        log.warning(f"Scraper directory not found: {scraper_dir}")
        log.info("Nothing to bridge. The scraper may not have run yet.")
        return

    # Find all JSON files in agent_queue
    pattern = os.path.join(scraper_dir, "*.json")
    files = sorted(glob.glob(pattern))

    if not files:
        log.info("No payload files found in agent_queue/. Nothing to bridge.")
        return

    log.info(f"Found {len(files)} payload file(s)\n")

    os.makedirs(scout_dir, exist_ok=True)
    stats = {"files_read": 0, "jobs_bridged": 0, "profiles": []}

    for filepath in files:
        filename = os.path.basename(filepath)
        try:
            with open(filepath, "r") as f:
                payload = json.load(f)
        except (json.JSONDecodeError, IOError) as e:
            log.error(f"  Failed to read {filename}: {e}")
            continue

        stats["files_read"] += 1

        # Extract profile key from payload
        recipient = payload.get("recipient", {})
        profile_key = recipient.get("id", "")
        profile_name = recipient.get("name", "unknown")

        if not profile_key:
            # Try to infer from filename (e.g., lorenzo_2026-05-02.json)
            profile_key = filename.split("_")[0] if "_" in filename else filename.replace(".json", "")
            log.warning(f"  No recipient.id in {filename}, inferred: {profile_key}")

        jobs_raw = payload.get("jobs", [])
        if not jobs_raw:
            log.info(f"  {profile_name}: 0 jobs, skipping")
            continue

        # Transform jobs
        jobs_transformed = [transform_job(j) for j in jobs_raw]
        stats["jobs_bridged"] += len(jobs_transformed)
        stats["profiles"].append(profile_key)

        # Write to scout inbox
        inbox_path = os.path.join(scout_dir, f"{profile_key}.json")
        inbox_payload = {"jobs": jobs_transformed}

        if dry_run:
            log.info(f"  {profile_name}: {len(jobs_transformed)} jobs → would write to {inbox_path}")
            for j in jobs_transformed:
                log.info(f"    • {j['title']} @ {j['company']} — {j['why_good_fit']}")
        else:
            # If inbox file already exists, merge (don't overwrite)
            if os.path.exists(inbox_path):
                try:
                    with open(inbox_path, "r") as f:
                        existing = json.load(f)
                    existing_jobs = existing.get("jobs", [])
                    # Deduplicate by URL
                    existing_urls = {j.get("url") for j in existing_jobs}
                    new_jobs = [j for j in jobs_transformed if j.get("url") not in existing_urls]
                    inbox_payload["jobs"] = existing_jobs + new_jobs
                    log.info(f"  {profile_name}: merged {len(new_jobs)} new jobs with {len(existing_jobs)} existing")
                except (json.JSONDecodeError, IOError):
                    pass  # overwrite corrupted file

            with open(inbox_path, "w") as f:
                json.dump(inbox_payload, f, indent=2)
            log.info(f"  {profile_name}: {len(jobs_transformed)} jobs → {inbox_path}")

        # Consume the scraper file (unless --keep)
        if not dry_run and not keep:
            os.remove(filepath)
            log.info(f"  Consumed: {filename}")

    log.info(f"\n{'=' * 60}")
    log.info(f"  Bridge complete: {stats['files_read']} files, {stats['jobs_bridged']} jobs")
    log.info(f"  Profiles: {', '.join(set(stats['profiles']))}")
    log.info(f"{'=' * 60}")


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Bridge: Scraper agent_queue → Scout inbox")
    parser.add_argument("--scraper-dir", default=DEFAULT_SCRAPER_DIR,
                        help="Path to scraper's agent_queue/ directory")
    parser.add_argument("--scout-dir", default=DEFAULT_SCOUT_DIR,
                        help="Path to scout's inbox/ directory")
    parser.add_argument("--dry-run", action="store_true",
                        help="Preview without writing files")
    parser.add_argument("--keep", action="store_true",
                        help="Don't delete consumed scraper files")
    args = parser.parse_args()

    run_bridge(args.scraper_dir, args.scout_dir, args.dry_run, args.keep)

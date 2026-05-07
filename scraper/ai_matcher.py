"""
AI MATCHER v2
==============
Uses Claude Sonnet to:
1. Hard-filter jobs that require German (or any language the profile doesn't have)
2. Score job-profile matches
3. Generate a punchy, specific one-sentence "why" for each match

Called after the keyword matcher filters to 50+ scores.
Final score = 70% AI + 30% keyword.

Usage:
    from ai_matcher import AIJobMatcher
    matcher = AIJobMatcher()
    results = matcher.score_batch(jobs_with_scores, friend_profile)
"""

import os
import json
import time
import logging
import requests

log = logging.getLogger("ai_matcher")

ANTHROPIC_API_KEY = os.getenv("ANTHROPIC_API_KEY", "")
MODEL = "claude-opus-4-6"
API_URL = "https://api.anthropic.com/v1/messages"
MAX_RETRIES = 2
RETRY_DELAY = 2
BATCH_SIZE = 10


class AIJobMatcher:

    def __init__(self, api_key: str = None):
        self.api_key = api_key or ANTHROPIC_API_KEY
        if not self.api_key:
            log.warning("No ANTHROPIC_API_KEY found. AI matching disabled.")

    def _build_profile_summary(self, friend: dict) -> str:
        """Compact profile summary for the prompt."""
        parts = [
            f"Name: {friend['name']}",
            f"Target roles: {', '.join(friend['target_roles'])}",
            f"Keywords: {', '.join(friend['keywords'])}",
            f"Preferred locations: {', '.join(friend.get('preferred_locations', []))}",
            f"Also accepts: {', '.join(friend.get('accepted_locations', []))}",
            f"Company types: {', '.join(friend.get('company_types', []))}",
            f"Seniority: {', '.join(friend.get('seniority', []))}",
            f"Languages: {', '.join(friend.get('languages', []))}",
        ]
        if friend.get("min_salary"):
            parts.append(f"Min salary: {friend['min_salary']}€")
        if friend.get("bonus_keywords"):
            parts.append(f"Bonus if mentioned: {', '.join(friend['bonus_keywords'])}")
        return "\n".join(parts)

    def _build_job_summary(self, job) -> str:
        """Compact job summary. Works with Job objects or dicts."""
        title = getattr(job, "title", "") if hasattr(job, "title") else job.get("title", "")
        company = getattr(job, "company", "") if hasattr(job, "company") else job.get("company", "")
        location = getattr(job, "location", "") if hasattr(job, "location") else job.get("location", "")
        desc = getattr(job, "description", "") if hasattr(job, "description") else job.get("description", "")
        if len(desc) > 1000:
            desc = desc[:1000] + "..."
        return f"Title: {title}\nCompany: {company}\nLocation: {location}\nDescription: {desc}"

    def score_batch(self, jobs_with_scores: list[tuple], friend: dict) -> list[dict]:
        """
        Score a batch of jobs for one friend profile.

        Args:
            jobs_with_scores: list of (job, keyword_score) tuples
            friend: friend profile dict

        Returns:
            list of {job, ai_score, final_score, why, keyword_score, rejected, reject_reason} dicts
        """
        if not self.api_key:
            log.warning("No API key. Returning keyword scores only.")
            return [
                {
                    "job": job,
                    "ai_score": None,
                    "final_score": kw_score,
                    "why": "AI scoring unavailable",
                    "keyword_score": kw_score,
                    "rejected": False,
                    "reject_reason": None,
                }
                for job, kw_score in jobs_with_scores
            ]

        results = []
        profile_summary = self._build_profile_summary(friend)

        for i in range(0, len(jobs_with_scores), BATCH_SIZE):
            batch = jobs_with_scores[i:i + BATCH_SIZE]
            batch_results = self._score_batch_api(batch, friend, profile_summary)
            results.extend(batch_results)
            if i + BATCH_SIZE < len(jobs_with_scores):
                time.sleep(1)

        return results

    def _score_batch_api(self, batch: list[tuple], friend: dict, profile_summary: str) -> list[dict]:
        """Send one API call to score up to BATCH_SIZE jobs."""

        job_entries = []
        for idx, (job, kw_score) in enumerate(batch):
            job_summary = self._build_job_summary(job)
            job_entries.append(f"--- JOB {idx + 1} ---\n{job_summary}")

        jobs_text = "\n\n".join(job_entries)

        # Extract the candidate's language capabilities for the prompt
        candidate_langs = friend.get("languages", [])
        lang_names = []
        for lang in candidate_langs:
            # Extract just the language name (e.g., "English" from "English C2")
            name = lang.split()[0] if lang else ""
            if name:
                lang_names.append(name)

        prompt = f"""You are a job matching expert helping real people find jobs. You must be ruthlessly honest.

CANDIDATE PROFILE:
{profile_summary}

CANDIDATE SPEAKS: {', '.join(candidate_langs)}
CANDIDATE DOES NOT SPEAK GERMAN (unless listed above).

JOBS TO EVALUATE:
{jobs_text}

For each job, return a JSON array with one object per job, in order:
[
  {{
    "job_index": 1,
    "rejected": true/false,
    "reject_reason": "reason" or null,
    "ai_score": <0-100 integer>,
    "why": "<one sentence>"
  }},
  ...
]

HARD REJECTION RULES — set rejected=true if ANY of these apply:
1. The job requires German (C1, C2, fluent, native, "Deutschkenntnisse erforderlich", or the posting is written primarily in German) AND the candidate does not speak German at that level. This is the #1 filter. Be aggressive here.
2. The job description is written in German, French, Dutch, or another language the candidate doesn't speak at C1+ level. If >30% of the description is in a non-English language the candidate doesn't speak, reject it.
3. The role requires a specific credential or license the candidate clearly doesn't have (e.g., CFA for a quant role, medical license).
4. The seniority is clearly wrong (e.g., Director/VP role for an entry-level candidate, or intern role for a senior candidate).

For rejected jobs: set ai_score to 0 and why to a short reason like "Requires fluent German" or "Job posting is in German".

SCORING (for non-rejected jobs only):
- 85-100: Near-perfect match. Role title, location, seniority, and domain all align.
- 70-84: Strong match. Most criteria met, maybe one minor gap.
- 55-69: Decent. Some overlap but notable gaps in role fit or location.
- 40-54: Weak. Only partial overlap.
- 0-39: Poor match.

THE "WHY" SENTENCE — this goes directly into an email to the candidate. Make it:
- Specific: mention the actual company name, role, or detail that makes it a match
- Human: write like a friend texting you about a job they saw, not a recruiter
- Short: one sentence, max 15 words
- Examples of GOOD why sentences:
  "Growth role at a Series A AI startup in Berlin — right up your alley."
  "SumUp's looking for exactly your profile in Berlin, and they pay well."
  "Consulting gig at EY Frankfurt — ticks the fintech + transformation boxes."
- Examples of BAD why sentences (never write these):
  "This role aligns well with the candidate's experience and preferences."
  "Strong match based on keywords and location criteria."
  "The position offers good alignment with the profile's target roles."

Return ONLY the JSON array. No markdown, no backticks, no explanation."""

        for attempt in range(MAX_RETRIES + 1):
            try:
                response = requests.post(
                    API_URL,
                    headers={
                        "Content-Type": "application/json",
                        "x-api-key": self.api_key,
                        "anthropic-version": "2023-06-01",
                    },
                    json={
                        "model": MODEL,
                        "max_tokens": 2000,
                        "messages": [{"role": "user", "content": prompt}],
                    },
                    timeout=45,
                )

                if response.status_code == 429:
                    log.warning(f"Rate limited, waiting {RETRY_DELAY * (attempt + 1)}s...")
                    time.sleep(RETRY_DELAY * (attempt + 1))
                    continue

                if response.status_code != 200:
                    log.error(f"API error {response.status_code}: {response.text[:200]}")
                    if attempt < MAX_RETRIES:
                        time.sleep(RETRY_DELAY)
                        continue
                    break

                data = response.json()
                text = ""
                for block in data.get("content", []):
                    if block.get("type") == "text":
                        text += block.get("text", "")

                text = text.strip().strip("`").strip()
                if text.startswith("json"):
                    text = text[4:].strip()

                ai_results = json.loads(text)

                output = []
                for idx, (job, kw_score) in enumerate(batch):
                    ai_data = ai_results[idx] if idx < len(ai_results) else {}
                    rejected = ai_data.get("rejected", False)
                    reject_reason = ai_data.get("reject_reason")
                    ai_score = ai_data.get("ai_score", 50)
                    why = ai_data.get("why", "No AI assessment available")

                    if rejected:
                        ai_score = 0
                        final_score = 0
                    else:
                        final_score = int(ai_score * 0.7 + kw_score * 0.3)

                    output.append({
                        "job": job,
                        "ai_score": ai_score,
                        "final_score": final_score,
                        "why": why,
                        "keyword_score": kw_score,
                        "rejected": rejected,
                        "reject_reason": reject_reason,
                    })

                # Log rejections
                rejected_count = sum(1 for r in output if r["rejected"])
                if rejected_count:
                    log.info(f"    AI rejected {rejected_count}/{len(output)} jobs (language/seniority/requirements)")

                return output

            except json.JSONDecodeError as e:
                log.error(f"Failed to parse AI response: {e}")
                log.error(f"Raw text: {text[:300]}")
                if attempt < MAX_RETRIES:
                    time.sleep(RETRY_DELAY)
                    continue
            except Exception as e:
                log.error(f"AI scoring error: {e}")
                if attempt < MAX_RETRIES:
                    time.sleep(RETRY_DELAY)
                    continue

        log.warning(f"AI scoring failed for batch. Using keyword scores.")
        return [
            {
                "job": job,
                "ai_score": None,
                "final_score": kw_score,
                "why": "AI scoring failed, matched by keywords",
                "keyword_score": kw_score,
                "rejected": False,
                "reject_reason": None,
            }
            for job, kw_score in batch
        ]

    def score_single(self, job, friend: dict, keyword_score: int) -> dict:
        """Score a single job. Convenience wrapper around score_batch."""
        results = self.score_batch([(job, keyword_score)], friend)
        return results[0] if results else {
            "job": job,
            "ai_score": None,
            "final_score": keyword_score,
            "why": "AI scoring unavailable",
            "keyword_score": keyword_score,
            "rejected": False,
            "reject_reason": None,
        }

"""
AI MATCHER v2
==============
Two-dimensional job matching:
1. GATE CHECK — hard requirements (language, visa, mandatory certs). Reject if not met.
2. FIT SCORE — can this person get this job? CV vs job requirements.
3. WANT SCORE — does this person want this job? Preferences vs job attributes.
4. FINAL — if fit < 40, reject. Otherwise: 60% fit + 40% want.

Uses Claude Sonnet to evaluate both dimensions.
"""

import os
import json
import time
import logging
import requests

log = logging.getLogger("ai_matcher")

ANTHROPIC_API_KEY = os.getenv("ANTHROPIC_API_KEY", "")
MODEL = "claude-sonnet-4-20250514"
API_URL = "https://api.anthropic.com/v1/messages"
MAX_RETRIES = 2
RETRY_DELAY = 2
BATCH_SIZE = 5  # fewer per batch since prompt is larger now


class AIJobMatcher:

    def __init__(self, api_key: str = None):
        self.api_key = api_key or ANTHROPIC_API_KEY
        if not self.api_key:
            log.warning("No ANTHROPIC_API_KEY found. AI matching disabled.")

    def _build_candidate_summary(self, friend: dict) -> str:
        """Build a full candidate profile: CV + preferences."""
        parts = []

        # What they HAVE (from CV)
        cv = friend.get("cv_parsed") or {}
        parts.append("=== WHAT THIS CANDIDATE HAS (from CV) ===")
        if cv.get("name"):
            parts.append(f"Name: {cv['name']}")
        if cv.get("current_title"):
            parts.append(f"Current/last title: {cv['current_title']}")
        if cv.get("years_experience"):
            parts.append(f"Years of experience: {cv['years_experience']}")
        if cv.get("skills"):
            parts.append(f"Skills: {', '.join(cv['skills'])}")
        if cv.get("languages"):
            parts.append(f"Languages: {', '.join(cv['languages'])}")
        if cv.get("locations"):
            parts.append(f"Locations lived/worked: {', '.join(cv['locations'])}")
        if cv.get("industries"):
            parts.append(f"Industries: {', '.join(cv['industries'])}")
        if cv.get("recent_companies"):
            parts.append(f"Recent companies: {', '.join(cv['recent_companies'])}")
        if cv.get("education"):
            parts.append(f"Education: {', '.join(cv['education'])}")
        if cv.get("summary"):
            parts.append(f"Summary: {cv['summary']}")

        # If no CV parsed, use what we have from profile
        if not cv:
            parts.append(f"Name: {friend['name']}")
            parts.append(f"Languages: {', '.join(friend.get('languages', []))}")
            parts.append("(No CV data available — score fit conservatively)")

        # What they WANT (preferences)
        parts.append("")
        parts.append("=== WHAT THIS CANDIDATE WANTS (preferences) ===")
        parts.append(f"Target roles: {', '.join(friend.get('target_roles', []))}")
        parts.append(f"Preferred locations: {', '.join(friend.get('preferred_locations', []))}")
        parts.append(f"Also accepts: {', '.join(friend.get('accepted_locations', []))}")
        parts.append(f"Company types: {', '.join(friend.get('company_types', []))}")
        parts.append(f"Seniority: {', '.join(friend.get('seniority', []))}")
        if friend.get("min_salary"):
            parts.append(f"Min salary: {friend['min_salary']}€/year")
        if friend.get("target_salary"):
            parts.append(f"Target salary: {friend['target_salary']}€/year")
        if friend.get("bonus_keywords"):
            parts.append(f"Excited about: {', '.join(friend['bonus_keywords'])}")

        return "\n".join(parts)

    def _build_job_summary(self, job) -> str:
        """Compact job summary."""
        title = getattr(job, "title", "") if hasattr(job, "title") else job.get("title", "")
        company = getattr(job, "company", "") if hasattr(job, "company") else job.get("company", "")
        location = getattr(job, "location", "") if hasattr(job, "location") else job.get("location", "")
        desc = getattr(job, "description", "") if hasattr(job, "description") else job.get("description", "")
        if len(desc) > 1000:
            desc = desc[:1000] + "..."
        return f"Title: {title}\nCompany: {company}\nLocation: {location}\nDescription: {desc}"

    def score_batch(self, jobs_with_scores: list[tuple], friend: dict) -> list[dict]:
        """
        Score a batch of jobs for one friend.
        Args: list of (job, keyword_score) tuples
        Returns: list of {job, ai_score, final_score, why, keyword_score, fit, want, rejected, reject_reason}
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
                    "fit": None,
                    "want": None,
                    "rejected": False,
                    "reject_reason": None,
                }
                for job, kw_score in jobs_with_scores
            ]

        results = []
        candidate_summary = self._build_candidate_summary(friend)

        for i in range(0, len(jobs_with_scores), BATCH_SIZE):
            batch = jobs_with_scores[i:i + BATCH_SIZE]
            batch_results = self._score_batch_api(batch, friend, candidate_summary)
            results.extend(batch_results)
            if i + BATCH_SIZE < len(jobs_with_scores):
                time.sleep(1)

        return results

    def _score_batch_api(self, batch: list[tuple], friend: dict, candidate_summary: str) -> list[dict]:
        """Send one API call to score up to BATCH_SIZE jobs."""

        job_entries = []
        for idx, (job, kw_score) in enumerate(batch):
            job_summary = self._build_job_summary(job)
            job_entries.append(f"--- JOB {idx + 1} ---\n{job_summary}")

        jobs_text = "\n\n".join(job_entries)

        prompt = f"""You are a job matching expert. Evaluate each job for this candidate on TWO dimensions.

CANDIDATE:
{candidate_summary}

JOBS TO EVALUATE:
{jobs_text}

For each job, perform this analysis:

STEP 1 — GATE CHECK (hard requirements):
Read the job description. Identify any HARD requirements (language fluency, visa/work permit, mandatory certifications, minimum years of experience stated as "must have" or "required").
Compare against the candidate's CV.
If a hard requirement is clearly not met (e.g., job requires fluent German but candidate has A2), REJECT the job.
Note: "nice to have" or "preferred" requirements are NOT hard requirements.

STEP 2 — FIT SCORE (0-100):
How well does the candidate's CV match what the company is looking for?
Consider: relevant skills, experience level, industry background, tools, education, languages.
A candidate with 2 years in B2B sales applying for a B2B sales role = high fit.
A candidate with marketing experience applying for a data engineering role = low fit.

STEP 3 — WANT SCORE (0-100):
How well does the job match what the candidate wants?
Consider: role type, location, company type, seniority, salary range, bonus keywords.

Return a JSON array, one object per job, in order:
[
  {{
    "job_index": 1,
    "rejected": false,
    "reject_reason": null,
    "fit": 75,
    "want": 80,
    "why": "One sentence explaining the match or rejection"
  }},
  ...
]

If rejected, set rejected:true, reject_reason to a short explanation (e.g. "Requires fluent German, candidate has A2"), and set fit and want to 0.

Scoring guide for FIT:
- 80-100: Strong match. Candidate's experience directly qualifies them.
- 60-79: Good match. Transferable skills, minor gaps.
- 40-59: Stretch. Some overlap but would need to upskill.
- 0-39: Poor fit. Wrong background entirely.

Scoring guide for WANT:
- 80-100: Dream job. Role, location, company type all align.
- 60-79: Good match. Most preferences met.
- 40-59: Acceptable. Some compromises needed.
- 0-39: Not what they're looking for.

The "why" must be ONE sentence, specific, referencing the actual role/company/requirement. Not generic.

Return ONLY the JSON array. No markdown, no backticks."""

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
                    fit = ai_data.get("fit", 0)
                    want = ai_data.get("want", 0)
                    why = ai_data.get("why", "No assessment available")

                    if rejected:
                        final_score = 0
                    elif fit < 40:
                        final_score = 0  # too low fit, don't send
                    else:
                        # 60% fit + 40% want
                        ai_score = int(fit * 0.6 + want * 0.4)
                        final_score = int(ai_score * 0.7 + kw_score * 0.3)

                    output.append({
                        "job": job,
                        "ai_score": int(fit * 0.6 + want * 0.4) if not rejected else 0,
                        "final_score": final_score,
                        "why": f"[REJECTED: {reject_reason}]" if rejected else why,
                        "keyword_score": kw_score,
                        "fit": fit,
                        "want": want,
                        "rejected": rejected,
                        "reject_reason": reject_reason,
                    })
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
                "fit": None,
                "want": None,
                "rejected": False,
                "reject_reason": None,
            }
            for job, kw_score in batch
        ]

    def score_single(self, job, friend: dict, keyword_score: int) -> dict:
        """Score a single job."""
        results = self.score_batch([(job, keyword_score)], friend)
        return results[0] if results else {
            "job": job, "ai_score": None, "final_score": keyword_score,
            "why": "AI scoring unavailable", "keyword_score": keyword_score,
            "fit": None, "want": None, "rejected": False, "reject_reason": None,
        }

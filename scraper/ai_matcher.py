"""
AI MATCHER
===========
Uses Claude Sonnet to score job-profile matches and generate
a one-sentence explanation for each match.
 
Called after the keyword matcher filters to 50+ scores.
Final score = 70% AI + 30% keyword.
 
Usage:
    from ai_matcher import AIJobMatcher
    matcher = AIJobMatcher()
    result = matcher.score(job, friend_profile, keyword_score)
    # result = {"ai_score": 82, "final_score": 71, "why": "...", "keyword_score": 55}
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
BATCH_SIZE = 10  # jobs per API call to save costs
 
 
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
        # Truncate description to save tokens
        if len(desc) > 800:
            desc = desc[:800] + "..."
        return f"Title: {title}\nCompany: {company}\nLocation: {location}\nDescription: {desc}"
 
    def score_batch(self, jobs_with_scores: list[tuple], friend: dict) -> list[dict]:
        """
        Score a batch of jobs for one friend profile.
 
        Args:
            jobs_with_scores: list of (job, keyword_score) tuples
            friend: friend profile dict
 
        Returns:
            list of {job, ai_score, final_score, why, keyword_score} dicts
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
                }
                for job, kw_score in jobs_with_scores
            ]
 
        results = []
        profile_summary = self._build_profile_summary(friend)
 
        # Process in batches
        for i in range(0, len(jobs_with_scores), BATCH_SIZE):
            batch = jobs_with_scores[i:i + BATCH_SIZE]
            batch_results = self._score_batch_api(batch, friend, profile_summary)
            results.extend(batch_results)
            if i + BATCH_SIZE < len(jobs_with_scores):
                time.sleep(1)  # rate limit between batches
 
        return results
 
    def _score_batch_api(self, batch: list[tuple], friend: dict, profile_summary: str) -> list[dict]:
        """Send one API call to score up to BATCH_SIZE jobs."""
 
        # Build job list for prompt
        job_entries = []
        for idx, (job, kw_score) in enumerate(batch):
            job_summary = self._build_job_summary(job)
            job_entries.append(f"--- JOB {idx + 1} ---\n{job_summary}")
 
        jobs_text = "\n\n".join(job_entries)
 
        prompt = f"""You are a job matching expert. Score how well each job fits this candidate's profile.
 
CANDIDATE PROFILE:
{profile_summary}
 
JOBS TO EVALUATE:
{jobs_text}
 
For each job, return a JSON array with one object per job, in order:
[
  {{
    "job_index": 1,
    "ai_score": <0-100 integer>,
    "why": "<one sentence explaining why this job fits or doesn't fit>"
  }},
  ...
]
 
Scoring guide:
- 80-100: Strong match. Role, location, and skills align well.
- 60-79: Decent match. Most criteria met, minor gaps.
- 40-59: Weak match. Some overlap but significant mismatches.
- 0-39: Poor match. Wrong role, location, or seniority.
 
Be honest and critical. A "Growth Manager at a Berlin startup" for someone targeting growth roles in Berlin should score high. A "Senior Director of Engineering in Tokyo" should score low.
 
The "why" must be ONE sentence, specific, and human. Not generic. Reference the actual role, company, or requirement.
 
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
                        "max_tokens": 1500,
                        "messages": [{"role": "user", "content": prompt}],
                    },
                    timeout=30,
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
 
                # Parse JSON response
                text = text.strip().strip("`").strip()
                if text.startswith("json"):
                    text = text[4:].strip()
 
                ai_results = json.loads(text)
 
                # Build results with blended scores
                output = []
                for idx, (job, kw_score) in enumerate(batch):
                    ai_data = ai_results[idx] if idx < len(ai_results) else {}
                    ai_score = ai_data.get("ai_score", 50)
                    why = ai_data.get("why", "No AI assessment available")
                    final_score = int(ai_score * 0.7 + kw_score * 0.3)
 
                    output.append({
                        "job": job,
                        "ai_score": ai_score,
                        "final_score": final_score,
                        "why": why,
                        "keyword_score": kw_score,
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
 
        # Fallback: return keyword scores only
        log.warning(f"AI scoring failed for batch. Using keyword scores.")
        return [
            {
                "job": job,
                "ai_score": None,
                "final_score": kw_score,
                "why": "AI scoring failed, matched by keywords",
                "keyword_score": kw_score,
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
        }

"""
ATS INTEGRATION
================
Hooks into the existing scraper pipeline.
Extracts companies from board results, classifies by sector,
ensures minimum coverage (financial, SaaS, startup, conglomerate),
and supplements with seed companies when needed.
"""

import os
import re
import uuid
import logging
from datetime import datetime
from ats_scraper import ATSDiscovery, ATSJob

log = logging.getLogger("ats_integration")

CACHE_PATH = os.path.join(os.path.dirname(__file__), "ats_cache.json")


# ═══════════════════════════════════════════════════════════════
# SECTOR CLASSIFICATION
# ═══════════════════════════════════════════════════════════════

SECTOR_KEYWORDS = {
    "financial": [
        "bank", "capital", "finance", "financial", "fintech", "invest",
        "asset", "wealth", "trading", "credit", "insurance", "fund",
        "payment", "lending", "securities", "hedge", "private equity",
        "venture", "mortgage",
    ],
    "saas": [
        "saas", "software", "platform", "cloud", "analytics", "data",
        "crm", "erp", "automation", "api", "devtools", "developer",
        "infrastructure", "cyber", "security", "ai ", "machine learning",
        "martech", "adtech", "proptech", "legaltech", "healthtech",
    ],
    "startup": [
        "startup", "seed", "series a", "series b", "early stage",
        "founding", "venture", "incubator", "accelerator", "y combinator",
    ],
    "conglomerate": [
        "nestle", "nestlé", "unilever", "philipp morris", "philip morris",
        "procter", "p&g", "siemens", "bosch", "basf", "bayer",
        "henkel", "beiersdorf", "adidas", "bmw", "daimler", "mercedes",
        "volkswagen", "sap", "allianz", "munich re", "deutsche",
        "roche", "novartis", "lvmh", "loreal", "l'oréal", "danone",
        "ab inbev", "heineken", "diageo", "shell", "bp", "total",
        "schneider", "saint-gobain", "thyssenkrupp", "continental",
        "ericsson", "nokia", "philips", "ikea", "h&m", "inditex",
        "zara", "ferrero", "barilla", "enel", "eni", "telefonica",
    ],
}

# Seed companies per sector — used when board results don't provide enough
# These are companies known to use Greenhouse/Lever/Ashby with European roles
SECTOR_SEEDS = {
    "financial": [
        "N26", "Revolut", "Wise", "SumUp", "Trade Republic",
        "Scalable Capital", "Raisin", "Solaris", "Pleo", "Qonto",
        "Adyen", "Mollie", "Klarna", "Checkout", "Monzo",
        "Stripe", "Plaid", "Robinhood", "Affirm", "Brex",
    ],
    "saas": [
        "Personio", "Celonis", "ContentSquare", "Datadog", "Miro",
        "Notion", "Figma", "Linear", "Vercel", "Airtable",
        "HubSpot", "Segment", "Amplitude", "LaunchDarkly", "Snyk",
        "GitLab", "Confluent", "HashiCorp", "Elastic", "MongoDB",
        "Typeform", "Algolia", "Sentry", "Postman", "Retool",
        "Deel", "Remote", "Oyster", "Factorial", "Leapsome",
    ],
    "startup": [
        "n8n", "Pitch", "Bending Spoons", "Mistral AI",
        "Helsing", "Aleph Alpha", "DeepL", "Forto", "Gorillas",
        "Flink", "Taxfix", "Grover", "TIER Mobility", "Flix",
        "Wefox", "Sennder", "Mambu", "Contentful", "Commercetools",
        "Agicap", "Alan", "BackMarket", "Doctolib", "BlaBlaCar",
        "Glovo", "Cabify", "Jobandtalent", "Wallapop", "Vinted",
        "Bolt", "Wolt", "Oda", "Kahoot", "Pleo",
        "Ankorstore", "Swile", "Pennylane", "Spendesk", "Payfit",
    ],
    "conglomerate": [
        "Nestle", "Unilever", "Siemens", "Bosch", "BASF",
        "Bayer", "SAP", "Adidas", "BMW", "Henkel",
        "Roche", "Novartis", "Philips", "LVMH", "Danone",
        "ABInBev", "Heineken", "Beiersdorf", "Continental", "Schneider Electric",
    ],
}

# Minimum number of companies to process per sector
SECTOR_MINIMUMS = {
    "financial": 5,
    "saas": 10,
    "startup": 20,
    "conglomerate": 5,
}


def classify_company(company_name: str, description_sample: str = "") -> str:
    """
    Classify a company into a sector based on name and context.
    Returns: 'financial', 'saas', 'startup', 'conglomerate', or 'other'
    """
    text = f"{company_name} {description_sample}".lower()

    # Check conglomerate first (exact name matches)
    for kw in SECTOR_KEYWORDS["conglomerate"]:
        if kw in text:
            return "conglomerate"

    # Check financial
    for kw in SECTOR_KEYWORDS["financial"]:
        if kw in text:
            return "financial"

    # Check SaaS
    for kw in SECTOR_KEYWORDS["saas"]:
        if kw in text:
            return "saas"

    # Check startup signals
    for kw in SECTOR_KEYWORDS["startup"]:
        if kw in text:
            return "startup"

    return "other"


def extract_company_names(board_jobs) -> list[str]:
    """Extract unique company names from Job objects."""
    names = set()
    for job in board_jobs:
        company = job.company.strip() if hasattr(job, "company") else ""
        if not company or company.lower() in {"confidential", "n/a", "various", "unknown"}:
            continue
        clean = re.sub(
            r"\s*(GmbH|Inc\.?|Ltd\.?|LLC|AG|S\.?A\.?|B\.?V\.?|SE|plc)\s*$",
            "", company, flags=re.IGNORECASE
        ).strip()
        if clean:
            names.add(clean)
    return sorted(names)


def build_company_list(board_jobs) -> list[str]:
    """
    Build the full company list: board-discovered + sector seeds as needed.
    Ensures minimum coverage per sector.
    """
    discovered = extract_company_names(board_jobs)

    # Classify discovered companies
    sector_counts = {"financial": [], "saas": [], "startup": [], "conglomerate": [], "other": []}
    for company in discovered:
        # Use first job's description for better classification
        desc = ""
        for job in board_jobs:
            if hasattr(job, "company") and job.company.strip() == company:
                desc = getattr(job, "description", "")[:200]
                break
        sector = classify_company(company, desc)
        sector_counts[sector].append(company)

    log.info(f"\n[Sector Classification] From board results:")
    for sector, companies in sector_counts.items():
        if companies:
            log.info(f"  {sector}: {len(companies)} companies")

    # Supplement with seeds where needed
    all_companies = list(discovered)
    already = {c.lower() for c in all_companies}

    for sector, minimum in SECTOR_MINIMUMS.items():
        current = len(sector_counts.get(sector, []))
        if current < minimum:
            needed = minimum - current
            seeds = SECTOR_SEEDS.get(sector, [])
            added = 0
            for seed in seeds:
                if seed.lower() not in already:
                    all_companies.append(seed)
                    already.add(seed.lower())
                    added += 1
                    if added >= needed:
                        break
            if added > 0:
                log.info(f"  {sector}: added {added} seed companies (had {current}, need {minimum})")

    return all_companies


def enrich_with_ats(board_jobs, seen_urls: set = None):
    """
    Main integration point.
    Takes Job objects, discovers ATS, filters, returns new Job objects.
    """
    from scraper import Job

    if seen_urls is None:
        seen_urls = set()

    existing_urls = set(seen_urls) | {j.url for j in board_jobs if hasattr(j, "url")}

    # Build company list with sector minimums
    companies = build_company_list(board_jobs)
    log.info(f"\n[ATS Integration] Processing {len(companies)} companies total")

    if not companies:
        return []

    # Discover and scrape (filters applied inside)
    discovery = ATSDiscovery(cache_path=CACHE_PATH)
    ats_jobs = discovery.discover_and_scrape(companies, apply_filters=True)

    # Deduplicate against board results
    new_jobs = [j for j in ats_jobs if j.url and j.url not in existing_urls]
    log.info(f"[ATS Integration] {len(ats_jobs)} passed filters, {len(new_jobs)} new after dedup")

    # Convert ATSJob → Job
    result = []
    for aj in new_jobs:
        result.append(Job(
            id=str(uuid.uuid4())[:8],
            title=aj.title,
            company=aj.company,
            location=aj.location,
            url=aj.url,
            description=aj.description,
            source=f"ats:{aj.source}",
            salary_min=None,
            salary_max=None,
            posted_date=aj.posted_date or None,
            remote=aj.remote,
            tags=[aj.department] if aj.department else [],
            scraped_at=datetime.now().isoformat(),
        ))
    return result

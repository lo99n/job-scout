"""
Playwright-powered scrapers for JS-heavy job boards (Otta, Glassdoor, WTTJ).
Falls back gracefully if Playwright isn't installed.
"""

import asyncio
import time
from urllib.parse import quote_plus

try:
    from playwright.async_api import async_playwright
    PLAYWRIGHT_AVAILABLE = True
except ImportError:
    PLAYWRIGHT_AVAILABLE = False

from scraper import Job, BaseScraper


class PlaywrightMixin:
    """Shared Playwright browser management."""

    async def _launch_browser(self):
        self._pw = await async_playwright().start()
        self._browser = await self._pw.chromium.launch(headless=True)
        self._context = await self._browser.new_context(
            user_agent="Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            viewport={"width": 1280, "height": 800},
        )

    async def _close_browser(self):
        if hasattr(self, "_browser"):
            await self._browser.close()
        if hasattr(self, "_pw"):
            await self._pw.stop()

    async def _get_page(self, url, wait_selector=None, timeout=15000):
        page = await self._context.new_page()
        try:
            await page.goto(url, wait_until="domcontentloaded", timeout=timeout)
            if wait_selector:
                try:
                    await page.wait_for_selector(wait_selector, timeout=8000)
                except Exception:
                    pass  # Proceed anyway
            await page.wait_for_timeout(2000)  # Let JS render
            return page
        except Exception as e:
            print(f"  [!] Playwright navigation error: {e}")
            await page.close()
            return None


class OttaPlaywrightScraper(PlaywrightMixin, BaseScraper):
    """Otta with full JS rendering."""
    name = "otta"
    base_url = "https://app.otta.com"

    def scrape(self, search_terms, location="Germany"):
        if not PLAYWRIGHT_AVAILABLE:
            print(f"  [{self.name}] Playwright not installed, skipping")
            return []
        return asyncio.run(self._scrape_async(search_terms, location))

    async def _scrape_async(self, search_terms, location):
        jobs = []
        seen_urls = set()
        await self._launch_browser()

        try:
            for term in search_terms[:5]:
                url = f"{self.base_url}/jobs?query={quote_plus(term)}&location=Germany"
                page = await self._get_page(url, wait_selector="a[href*='/jobs/']")
                if not page:
                    continue

                # Scroll to load more
                for _ in range(3):
                    await page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
                    await page.wait_for_timeout(1500)

                cards = await page.query_selector_all("a[href*='/jobs/']")
                for card in cards:
                    href = await card.get_attribute("href")
                    if not href or href in seen_urls:
                        continue
                    if "/jobs/" not in href:
                        continue

                    full_url = href if href.startswith("http") else f"{self.base_url}{href}"
                    seen_urls.add(full_url)

                    text = await card.inner_text()
                    lines = [l.strip() for l in text.split("\n") if l.strip()]
                    title = lines[0] if lines else ""
                    company = lines[1] if len(lines) > 1 else ""

                    # Get full description
                    desc = await self._get_description(full_url)

                    if title:
                        jobs.append(Job(
                            id=self._make_id(title, company, full_url),
                            title=title,
                            company=company,
                            location="Germany",
                            url=full_url,
                            description=desc,
                            source=self.name,
                        ))

                await page.close()
                await asyncio.sleep(2)

        finally:
            await self._close_browser()

        print(f"  [{self.name}] Found {len(jobs)} relevant jobs")
        return jobs

    async def _get_description(self, url):
        page = await self._get_page(url, wait_selector="div[class*='description'], section")
        if not page:
            return ""
        try:
            desc_el = await page.query_selector("div[class*='description'], div[class*='JobDescription'], section[class*='description']")
            if desc_el:
                text = await desc_el.inner_text()
                return text[:5000]

            # Fallback: grab main content
            body = await page.inner_text("main")
            return body[:3000] if body else ""
        finally:
            await page.close()


class GlassdoorPlaywrightScraper(PlaywrightMixin, BaseScraper):
    """Glassdoor with full JS rendering and description extraction."""
    name = "glassdoor"
    base_url = "https://www.glassdoor.com"

    def scrape(self, search_terms, location="Germany"):
        if not PLAYWRIGHT_AVAILABLE:
            print(f"  [{self.name}] Playwright not installed, skipping")
            return []
        return asyncio.run(self._scrape_async(search_terms, location))

    async def _scrape_async(self, search_terms, location):
        jobs = []
        seen_urls = set()
        await self._launch_browser()

        try:
            for term in search_terms[:5]:
                url = f"{self.base_url}/Job/germany-{quote_plus(term)}-jobs-SRCH_IL.0,7_IN96.htm"
                page = await self._get_page(url, wait_selector="li[data-test='jobListing'], div.JobCard")
                if not page:
                    continue

                # Try cookie banner dismissal
                try:
                    accept_btn = await page.query_selector("button#onetrust-accept-btn-handler, button[data-test='accept-cookies']")
                    if accept_btn:
                        await accept_btn.click()
                        await page.wait_for_timeout(500)
                except Exception:
                    pass

                cards = await page.query_selector_all("li[data-test='jobListing'], li.react-job-listing, div.JobCard")
                for card in cards:
                    try:
                        title_el = await card.query_selector("a[data-test='job-title'], a.jobTitle, a.JobCard_jobTitle")
                        if not title_el:
                            continue
                        title = await title_el.inner_text()
                        href = await title_el.get_attribute("href")
                        if not href:
                            continue
                        full_url = href if href.startswith("http") else f"{self.base_url}{href}"
                        if full_url in seen_urls:
                            continue
                        seen_urls.add(full_url)

                        company_el = await card.query_selector("span.EmployerProfile_compactEmployerName__LE242, div.employer-name, a[data-test='employer-name']")
                        location_el = await card.query_selector("div[data-test='emp-location'], span.job-location, div.location")
                        salary_el = await card.query_selector("div[data-test='detailSalary'], span.salary-estimate")

                        company = await company_el.inner_text() if company_el else ""
                        loc = await location_el.inner_text() if location_el else ""
                        salary_text = await salary_el.inner_text() if salary_el else ""

                        # Click to get description in side panel
                        desc = ""
                        try:
                            await title_el.click()
                            await page.wait_for_timeout(2000)
                            desc_el = await page.query_selector("div.jobDescriptionContent, div[class*='JobDetails'], div[data-test='job-description']")
                            if desc_el:
                                desc = await desc_el.inner_text()
                                desc = desc[:5000]
                        except Exception:
                            pass

                        salary_min, salary_max = self._parse_salary_text(salary_text)

                        jobs.append(Job(
                            id=self._make_id(title, company, full_url),
                            title=title,
                            company=company,
                            location=loc,
                            url=full_url,
                            description=desc,
                            source=self.name,
                            salary_min=salary_min,
                            salary_max=salary_max,
                        ))
                    except Exception as e:
                        continue

                await page.close()
                await asyncio.sleep(3)

        finally:
            await self._close_browser()

        print(f"  [{self.name}] Found {len(jobs)} relevant jobs")
        return jobs

    def _parse_salary_text(self, text):
        if not text:
            return None, None
        import re
        numbers = re.findall(r"[\d.,]+", text.replace(",", ""))
        nums = []
        for n in numbers:
            try:
                val = int(float(n.replace(".", "")))
                if val > 1000:
                    nums.append(val)
            except ValueError:
                pass
        if len(nums) >= 2:
            return min(nums), max(nums)
        elif len(nums) == 1:
            return nums[0], nums[0]
        return None, None


class WTTJPlaywrightScraper(PlaywrightMixin, BaseScraper):
    """Welcome to the Jungle with JS rendering for full descriptions."""
    name = "wttj"
    base_url = "https://www.welcometothejungle.com"

    def scrape(self, search_terms, location="Germany"):
        if not PLAYWRIGHT_AVAILABLE:
            print(f"  [{self.name}] Playwright not installed, skipping")
            return []
        return asyncio.run(self._scrape_async(search_terms, location))

    async def _scrape_async(self, search_terms, location):
        jobs = []
        seen_urls = set()
        await self._launch_browser()

        try:
            for term in search_terms[:6]:
                url = f"{self.base_url}/en/jobs?query={quote_plus(term)}&page=1&aroundQuery=Germany"
                page = await self._get_page(url, wait_selector="a[href*='/jobs/']")
                if not page:
                    continue

                # Cookie dismissal
                try:
                    cookie_btn = await page.query_selector("button[data-testid='cookieConsent-accept'], button:has-text('Accept')")
                    if cookie_btn:
                        await cookie_btn.click()
                        await page.wait_for_timeout(500)
                except Exception:
                    pass

                # Scroll to load
                for _ in range(2):
                    await page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
                    await page.wait_for_timeout(1500)

                links = await page.query_selector_all("a[href*='/en/companies/'][href*='/jobs/']")
                for link_el in links:
                    href = await link_el.get_attribute("href")
                    if not href or href in seen_urls:
                        continue
                    full_url = href if href.startswith("http") else f"{self.base_url}{href}"
                    seen_urls.add(full_url)

                    text = await link_el.inner_text()
                    lines = [l.strip() for l in text.split("\n") if l.strip()]
                    title = lines[0] if lines else ""
                    company = lines[1] if len(lines) > 1 else ""

                    if title:
                        desc = await self._get_description(full_url)
                        jobs.append(Job(
                            id=self._make_id(title, company, full_url),
                            title=title,
                            company=company,
                            location="Germany",
                            url=full_url,
                            description=desc,
                            source=self.name,
                        ))

                await page.close()
                await asyncio.sleep(2)

        finally:
            await self._close_browser()

        print(f"  [{self.name}] Found {len(jobs)} relevant jobs")
        return jobs

    async def _get_description(self, url):
        page = await self._get_page(url, wait_selector="div[data-testid='job-section-description']")
        if not page:
            return ""
        try:
            selectors = [
                "div[data-testid='job-section-description']",
                "div[class*='JobDescription']",
                "section[class*='sc-']",
                "main",
            ]
            for sel in selectors:
                el = await page.query_selector(sel)
                if el:
                    text = await el.inner_text()
                    if len(text) > 50:
                        return text[:5000]
            return ""
        finally:
            await page.close()


def get_playwright_scrapers():
    """Returns Playwright scrapers if available, empty list otherwise."""
    if not PLAYWRIGHT_AVAILABLE:
        print("  [!] Playwright not installed. Run: pip install playwright && playwright install chromium")
        return []
    return [
        OttaPlaywrightScraper(),
        GlassdoorPlaywrightScraper(),
        WTTJPlaywrightScraper(),
    ]

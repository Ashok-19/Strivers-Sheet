"""
Interview Experiences Scraper
==============================
https://takeuforward.org/interview

Data is loaded from two sources:
  1. Company list  – embedded in Next.js flight data on the listing page
  2. Experiences   – fetched from TUF's backend REST API (paginated, no auth needed,
                     but requires Origin: https://takeuforward.org header)

API endpoint:
  GET https://backend-go.takeuforward.org/api/v1/interview-experience/by-upvotes
  Params: page (1-based), page_size

Each experience is stored as a `problems` row:
  - source_id    = "interview"
  - name         = "{position} @ {company} [status]"
  - article_url  = "https://takeuforward.org/interview/{slug}"  (queued for article scraping)
  - difficulty   = status (Selected / Rejected / On-hold)
  - extra_json   = full API payload (company, tags, upvotes, rounds, description, author …)

DB hierarchy:
  topic    = company name   (Amazon, Google, …)
  subtopic = company type   (Product Based / Service Based)
"""

import re
import json
import asyncio
import hashlib
import logging

import httpx
from playwright.async_api import async_playwright

from .base import create_browser, close_annoying_dialogs, polite_delay
from ..db.database import Database

logger = logging.getLogger(__name__)

INTERVIEW_URL = "https://takeuforward.org/interview"
API_BASE      = "https://backend-go.takeuforward.org/api/v1/interview-experience"
SOURCE_ID     = "interview"
PAGE_SIZE     = 50

API_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/122.0.0.0 Safari/537.36"
    ),
    "Origin":  "https://takeuforward.org",
    "Referer": "https://takeuforward.org/interview",
    "Accept":  "application/json",
}

_FLIGHT_RE = re.compile(r'self\.__next_f\.push\(\[1,\s*"(.*?)"\]\)', re.DOTALL)


# ─── Entry point ─────────────────────────────────────────────────────────────

async def scrape_interview_experiences(db: Database):
    """Scrape all interview experience entries and persist to DB."""
    print("\n[Interview] Starting...")

    await db.upsert_source(SOURCE_ID, "Interview Experiences", "interview", INTERVIEW_URL)

    # 1. Get company list (for topic/subtopic pre-creation)
    companies = await _get_companies()
    print(f"  Found {len(companies)} companies in listing")

    company_topic_map: dict[str, str]      = {}  # company name → topic_id
    sub_id_map:        dict[tuple, str]    = {}  # (topic_id, type_str) → sub_id

    for c_idx, company in enumerate(companies):
        c_name = company["name"]
        c_type = company.get("type", "Other")

        topic_id = f"{SOURCE_ID}_co_{_short_hash(c_name)}"
        await db.upsert_topic(topic_id, SOURCE_ID, c_name, c_idx)
        company_topic_map[c_name] = topic_id

        key = (topic_id, c_type)
        if key not in sub_id_map:
            sub_id = f"{topic_id}_{_short_hash(c_type)}"
            await db.upsert_subtopic(sub_id, topic_id, c_type, 0)
            sub_id_map[key] = sub_id

    # Build a name→type lookup for companies
    company_type_map = {c["name"]: c.get("type", "Other") for c in companies}

    # 2. Paginate through all experiences via the REST API
    total_stored = await _paginate_experiences(
        db, company_topic_map, sub_id_map, company_type_map
    )

    print(f"  [Interview] ✓ {total_stored} experiences stored")


# ─── API pagination ───────────────────────────────────────────────────────────

async def _paginate_experiences(
    db: Database,
    company_topic_map: dict,
    sub_id_map: dict,
    company_type_map: dict,
) -> int:
    total_stored = 0
    global_idx   = 0

    async with httpx.AsyncClient(timeout=20, headers=API_HEADERS) as client:
        page = 1
        total_items = None

        while True:
            try:
                resp = await client.get(
                    f"{API_BASE}/by-upvotes",
                    params={"page": page, "page_size": PAGE_SIZE},
                )
            except Exception as e:
                logger.error(f"Request failed on page {page}: {e}")
                break

            if resp.status_code != 200:
                logger.error(f"API returned {resp.status_code} on page {page}")
                break

            try:
                data = resp.json()
            except Exception as e:
                logger.error(f"JSON decode failed page {page}: {e}")
                break

            if not data.get("success"):
                logger.error(f"API success=false page {page}: {data.get('message')}")
                break

            exps = data["data"].get("interviewExps", [])
            if total_items is None:
                total_items = data["data"].get("totalItems", 0)
                total_pages = max(1, (total_items + PAGE_SIZE - 1) // PAGE_SIZE)

            if not exps:
                break

            for exp in exps:
                c_name   = exp.get("company", "Unknown")
                position = exp.get("position", "Unknown")
                status   = exp.get("status", "")
                slug     = exp.get("slug", "")
                exp_id_raw = exp.get("id") or slug or f"exp_{global_idx}"

                # Ensure topic exists (may appear in API but not company listing)
                if c_name not in company_topic_map:
                    topic_id = f"{SOURCE_ID}_co_{_short_hash(c_name)}"
                    await db.upsert_topic(
                        topic_id, SOURCE_ID, c_name,
                        order_idx=900 + len(company_topic_map)
                    )
                    company_topic_map[c_name] = topic_id

                topic_id = company_topic_map[c_name]
                c_type   = company_type_map.get(c_name, "Other")

                key = (topic_id, c_type)
                if key not in sub_id_map:
                    sub_id = f"{topic_id}_{_short_hash(c_type)}"
                    await db.upsert_subtopic(sub_id, topic_id, c_type, 0)
                    sub_id_map[key] = sub_id
                sub_id = sub_id_map[key]

                article_url = f"{INTERVIEW_URL}/{slug}" if slug else None
                exp_name    = f"{position} @ {c_name}"
                if status:
                    exp_name += f" [{status}]"

                await db.upsert_problem(
                    problem_id   = f"{SOURCE_ID}_{_short_hash(exp_id_raw)}",
                    name         = exp_name,
                    subtopic_id  = sub_id,
                    source_id    = SOURCE_ID,
                    article_url  = article_url,
                    difficulty   = status or None,
                    order_idx    = global_idx,
                    extra        = {
                        "slug":           slug,
                        "company":        c_name,
                        "position":       position,
                        "status":         status,
                        "tags":           exp.get("tags", []),
                        "upvotes":        exp.get("upvotes", 0),
                        "rounds":         exp.get("rounds", 0),
                        "problems_count": exp.get("problems", 0),
                        "description":    exp.get("description", ""),
                        "author":         exp.get("author", {}),
                        "posted_time":    exp.get("postedTime", ""),
                        "company_logo":   exp.get("companyLogo", ""),
                    },
                )
                global_idx   += 1
                total_stored += 1

            await db.commit()
            print(f"  Page {page}/{total_pages} – {total_stored}/{total_items} stored")

            if len(exps) < PAGE_SIZE or total_stored >= (total_items or 0):
                break

            page += 1
            await polite_delay(0.8)

    return total_stored


# ─── Company list from flight data ────────────────────────────────────────────

def _decode_segment(raw: str) -> str:
    try:
        return json.loads(f'"{raw}"')
    except Exception:
        return raw


async def _get_companies() -> list[dict]:
    """Load the company list from flight data on the interview listing page."""
    try:
        async with async_playwright() as pw:
            browser, context = await create_browser(pw)
            page = await context.new_page()
            await page.goto(INTERVIEW_URL, wait_until="domcontentloaded", timeout=30000)
            await close_annoying_dialogs(page)
            await asyncio.sleep(3)
            html = await page.content()
            await browser.close()

        segs     = _FLIGHT_RE.findall(html)
        combined = "".join(_decode_segment(s) for s in segs)

        m = re.search(r'"companies":\[(\{[^]]*?\}(?:,\{[^]]*?\})*)\]', combined, re.DOTALL)
        if m:
            try:
                return json.loads(f'[{m.group(1)}]')
            except Exception:
                pass

        # Fallback: simpler flat-object extractor
        companies = []
        for obj_m in re.finditer(r'\{[^{}]{20,400}\}', combined):
            try:
                obj = json.loads(obj_m.group())
                if "experiences" in obj and "name" in obj:
                    companies.append(obj)
            except Exception:
                pass
        return companies

    except Exception as e:
        logger.warning(f"Failed to get company list: {e}")
        return []


# ─── Helpers ─────────────────────────────────────────────────────────────────

def _short_hash(text: str) -> str:
    return hashlib.sha1(text.encode()).hexdigest()[:10]

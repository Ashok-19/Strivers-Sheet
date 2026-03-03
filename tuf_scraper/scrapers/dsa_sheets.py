"""
DSA Sheets Scraper
==================
Handles: A2Z, Blind 75, SDE Sheet, Striver 79

All sheets embed their problem data in `self.__next_f` script tags.
Each sheet has its own URL; some have category→subcategory→problems
hierarchy (A2Z, SDE), others are flatter (Blind75, 79).
"""

import re
import json
import asyncio
import hashlib
import logging
from typing import Optional

from playwright.async_api import async_playwright

from .base import create_browser, close_annoying_dialogs, parse_flight_hierarchy, polite_delay
from ..db.database import Database

logger = logging.getLogger(__name__)


# ─── Sheet definitions ───────────────────────────────────────────────────────
# These URLs follow redirects automatically via Playwright
SHEETS = {
    "a2z": {
        "name": "Striver's A2Z DSA Sheet",
        "url": "https://takeuforward.org/dsa/strivers-a2z-sheet-learn-dsa-a-to-z",
        "fallback_urls": [
            "https://takeuforward.org/strivers-a2z-dsa-course/strivers-a2z-dsa-course-sheet-2/",
        ],
    },
    "blind75": {
        "name": "Blind 75 LeetCode Sheet",
        "url": "https://takeuforward.org/dsa/blind-75-leetcode-problems-detailed-video-solutions",
        "fallback_urls": [
            "https://takeuforward.org/interviews/blind-75-leetcode-problems-solution-links/",
        ],
    },
    "sde": {
        "name": "Striver's SDE Sheet",
        "url": "https://takeuforward.org/interviews/strivers-sde-sheet-top-coding-interview-problems/",
        "fallback_urls": [
            "https://takeuforward.org/dsa/sde-sheet",
        ],
    },
    "striver79": {
        "name": "Striver's 79 (Last Month) Sheet",
        "url": "https://takeuforward.org/dsa/strivers-79-last-moment-dsa-sheet-ace-interviews",
        "fallback_urls": [
            "https://takeuforward.org/strivers-79-last-month-dsa-sheet/",
        ],
    },
}


# ─── Main entry point ────────────────────────────────────────────────────────

async def scrape_all_dsa_sheets(db: Database):
    """Scrape all DSA sheets and persist to DB."""
    print("\n[DSA Sheets] Starting...")

    async with async_playwright() as pw:
        browser, context = await create_browser(pw)
        page = await context.new_page()

        for sheet_id, sheet_meta in SHEETS.items():
            print(f"  [{sheet_id}] Scraping '{sheet_meta['name']}'...")
            try:
                count = await _scrape_sheet(page, db, sheet_id, sheet_meta)
                await db.commit()
                print(f"  [{sheet_id}] ✓ {count} problems stored")
            except Exception as e:
                logger.error(f"[{sheet_id}] Failed: {e}", exc_info=True)
                print(f"  [{sheet_id}] ✗ Error: {e}")
            await polite_delay(2)

        await browser.close()


async def _scrape_sheet(page, db: Database, sheet_id: str, meta: dict) -> int:
    """Scrape a single sheet, try primary URL then fallbacks."""
    # Register source
    source_id = f"sheet_{sheet_id}"
    await db.upsert_source(source_id, meta["name"], "dsa_sheet", meta["url"])

    # Try URLs in order
    html = None
    final_url = meta["url"]
    for url in [meta["url"]] + meta.get("fallback_urls", []):
        try:
            await page.goto(url, wait_until="domcontentloaded", timeout=30000)
            await close_annoying_dialogs(page)
            # Wait for flight data to be fully embedded
            await asyncio.sleep(3)
            html = await page.content()
            final_url = page.url
            if _has_flight_problems(html):
                break
        except Exception as e:
            logger.warning(f"  URL failed {url}: {e}")
            continue

    if not html or not _has_flight_problems(html):
        logger.warning(f"  No flight data found for {sheet_id}")
        return 0

    # Parse hierarchy from flight data
    categories = parse_flight_hierarchy(html)

    count = 0
    for cat_idx, category in enumerate(categories):
        cat_id = str(category.get("category_id") or f"{sheet_id}_cat_{cat_idx}")
        cat_name = str(category.get("category_name") or f"Category {cat_idx}")

        topic_id = f"{source_id}_{_short_hash(cat_id)}"
        await db.upsert_topic(topic_id, source_id, cat_name, cat_idx)

        subcategories = category.get("subcategories", [])
        if not subcategories:
            # Flat structure: treat category itself as subtopic
            subcategories = [{"subcategory_id": cat_id, "subcategory_name": cat_name,
                               "problems": category.get("problems", [])}]

        for sub_idx, subcat in enumerate(subcategories):
            sub_id_raw = str(subcat.get("subcategory_id") or f"{cat_id}_sub_{sub_idx}")
            sub_name = str(subcat.get("subcategory_name") or f"Sub {sub_idx}")

            sub_id = f"{topic_id}_{_short_hash(sub_id_raw)}"
            await db.upsert_subtopic(sub_id, topic_id, sub_name, sub_idx)

            problems = subcat.get("problems", [])
            for p_idx, prob in enumerate(problems):
                prob_id_raw = str(prob.get("problem_id") or f"{sub_id}_{p_idx}")
                await db.upsert_problem(
                    problem_id=f"{source_id}_{_short_hash(prob_id_raw)}",
                    name=prob.get("problem_name", "Unknown"),
                    subtopic_id=sub_id,
                    source_id=source_id,
                    article_url=prob.get("article_url"),
                    youtube_url=prob.get("youtube_url"),
                    leetcode_url=prob.get("leetcode_url"),
                    plus_url=prob.get("plus_url"),
                    difficulty=prob.get("difficulty"),
                    order_idx=p_idx,
                    extra={"original_id": prob_id_raw, "sheet": sheet_id},
                )
                count += 1

    return count


def _has_flight_problems(html: str) -> bool:
    return "problem_name" in html and "__next_f" in html


def _short_hash(text: str) -> str:
    return hashlib.sha1(text.encode()).hexdigest()[:10]

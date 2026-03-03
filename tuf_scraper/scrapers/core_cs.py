"""
Core CS Scraper
===============
Handles: Computer Networks, DBMS, Operating Systems

Access levels (per user requirements):
  CN   – names only     (no article / youtube extraction)
  DBMS – article + youtube
  OS   – article only
"""

import asyncio
import hashlib
import logging

from playwright.async_api import async_playwright

from .base import create_browser, close_annoying_dialogs, parse_flight_hierarchy, polite_delay
from ..db.database import Database

logger = logging.getLogger(__name__)


SUBJECTS = {
    "cn": {
        "name": "Computer Networks",
        "url": "https://takeuforward.org/computer-network/most-asked-computer-networks-interview-questions/",
        "fallback_urls": [
            "https://takeuforward.org/networking/complete-computer-networking-full-course-cn-notes/",
        ],
        "scrape_articles": False,
        "scrape_youtube": False,
    },
    "dbms": {
        "name": "Database Management Systems",
        "url": "https://takeuforward.org/dbms/most-asked-dbms-interview-questions/",
        "fallback_urls": [
            "https://takeuforward.org/dbms/dbms-complete-tutorial/",
        ],
        "scrape_articles": True,
        "scrape_youtube": True,
    },
    "os": {
        "name": "Operating Systems",
        "url": "https://takeuforward.org/operating-system/most-asked-operating-system-interview-questions",
        "fallback_urls": [
            "https://takeuforward.org/os/most-asked-os-interview-questions",
        ],
        "scrape_articles": True,
        "scrape_youtube": False,
    },
}


async def scrape_all_core_cs(db: Database):
    """Scrape all Core CS subjects and persist to DB."""
    print("\n[Core CS] Starting...")

    async with async_playwright() as pw:
        browser, context = await create_browser(pw)
        page = await context.new_page()

        for subject_id, meta in SUBJECTS.items():
            print(f"  [{subject_id.upper()}] Scraping '{meta['name']}'...")
            try:
                count = await _scrape_subject(page, db, subject_id, meta)
                await db.commit()
                print(f"  [{subject_id.upper()}] ✓ {count} topics stored")
            except Exception as e:
                logger.error(f"[{subject_id}] Failed: {e}", exc_info=True)
                print(f"  [{subject_id.upper()}] ✗ Error: {e}")
            await polite_delay(2)

        await browser.close()


async def _scrape_subject(page, db: Database, subject_id: str, meta: dict) -> int:
    source_id = f"cs_{subject_id}"
    await db.upsert_source(source_id, meta["name"], "core_cs", meta["url"])

    # Try URLs
    html = None
    for url in [meta["url"]] + meta.get("fallback_urls", []):
        try:
            await page.goto(url, wait_until="domcontentloaded", timeout=30000)
            await close_annoying_dialogs(page)
            await asyncio.sleep(3)
            html = await page.content()
            if _has_flight_data(html):
                break
        except Exception as e:
            logger.warning(f"  URL failed {url}: {e}")

    if not html or not _has_flight_data(html):
        logger.warning(f"  No flight data for {subject_id}")
        return 0

    categories = parse_flight_hierarchy(html)
    count = 0

    for cat_idx, category in enumerate(categories):
        cat_id_raw = str(category.get("category_id") or f"{subject_id}_cat_{cat_idx}")
        cat_name = str(category.get("category_name") or f"Category {cat_idx}")
        topic_id = f"{source_id}_{_short_hash(cat_id_raw)}"
        await db.upsert_topic(topic_id, source_id, cat_name, cat_idx)

        subcategories = category.get("subcategories") or [
            {"subcategory_id": cat_id_raw, "subcategory_name": cat_name,
             "problems": category.get("problems", [])}
        ]

        for sub_idx, subcat in enumerate(subcategories):
            sub_id_raw = str(subcat.get("subcategory_id") or f"{cat_id_raw}_{sub_idx}")
            sub_name = str(subcat.get("subcategory_name") or f"Sub {sub_idx}")
            sub_id = f"{topic_id}_{_short_hash(sub_id_raw)}"
            await db.upsert_subtopic(sub_id, topic_id, sub_name, sub_idx)

            for p_idx, prob in enumerate(subcat.get("problems", [])):
                prob_id_raw = str(prob.get("problem_id") or f"{sub_id}_{p_idx}")
                await db.upsert_problem(
                    problem_id=f"{source_id}_{_short_hash(prob_id_raw)}",
                    name=prob.get("problem_name", "Unknown"),
                    subtopic_id=sub_id,
                    source_id=source_id,
                    article_url=prob.get("article_url") if meta["scrape_articles"] else None,
                    youtube_url=prob.get("youtube_url") if meta["scrape_youtube"] else None,
                    leetcode_url=None,
                    plus_url=prob.get("plus_url"),
                    difficulty=prob.get("difficulty"),
                    order_idx=p_idx,
                    extra={"subject": subject_id, "original_id": prob_id_raw},
                )
                count += 1

    return count


def _has_flight_data(html: str) -> bool:
    return "__next_f" in html


def _short_hash(text: str) -> str:
    return hashlib.sha1(text.encode()).hexdigest()[:10]

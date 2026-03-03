"""
System Design Scraper
=====================
Scrapes the System Design roadmap page.
Captures: topic names, article URLs, youtube URLs.
"""

import asyncio
import hashlib
import logging

from playwright.async_api import async_playwright

from .base import create_browser, close_annoying_dialogs, parse_flight_hierarchy, polite_delay
from ..db.database import Database

logger = logging.getLogger(__name__)


SD_SOURCES = [
    {
        "id": "system_design",
        "name": "System Design Roadmap",
        "url": "https://takeuforward.org/system-design/complete-system-design-roadmap-with-videos-for-sdes/",
        "fallback_urls": [
            "https://takeuforward.org/system-design/",
        ],
    },
]


async def scrape_system_design(db: Database):
    """Scrape System Design topics and persist to DB."""
    print("\n[System Design] Starting...")

    async with async_playwright() as pw:
        browser, context = await create_browser(pw)
        page = await context.new_page()

        for src in SD_SOURCES:
            print(f"  Scraping '{src['name']}'...")
            try:
                count = await _scrape_sd(page, db, src)
                await db.commit()
                print(f"  ✓ {count} topics stored")
            except Exception as e:
                logger.error(f"System design scrape failed: {e}", exc_info=True)
                print(f"  ✗ Error: {e}")

        await browser.close()


async def _scrape_sd(page, db: Database, src: dict) -> int:
    source_id = src["id"]
    await db.upsert_source(source_id, src["name"], "system_design", src["url"])

    html = None
    for url in [src["url"]] + src.get("fallback_urls", []):
        try:
            await page.goto(url, wait_until="domcontentloaded", timeout=30000)
            await close_annoying_dialogs(page)
            await asyncio.sleep(3)
            html = await page.content()
            if "__next_f" in html:
                break
        except Exception as e:
            logger.warning(f"  URL failed {url}: {e}")

    if not html or "__next_f" not in html:
        logger.warning("  No flight data for system design")
        return 0

    categories = parse_flight_hierarchy(html)
    count = 0

    for cat_idx, category in enumerate(categories):
        cat_id_raw = str(category.get("category_id") or f"sd_cat_{cat_idx}")
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
                    article_url=prob.get("article_url"),
                    youtube_url=prob.get("youtube_url"),
                    leetcode_url=None,  # System design: no leetcode
                    plus_url=prob.get("plus_url"),
                    difficulty=prob.get("difficulty"),
                    order_idx=p_idx,
                    extra={"type": "system_design", "original_id": prob_id_raw},
                )
                count += 1

    return count


def _short_hash(text: str) -> str:
    return hashlib.sha1(text.encode()).hexdigest()[:10]

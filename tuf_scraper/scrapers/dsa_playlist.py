"""
DSA Playlist Scraper
====================
Scrapes all topic-based DSA series pages (the "playlist" section on TUF).
Each series page has a structured problem list with article + youtube + leetcode links.

Series URLs discovered from the TUF home page navigation.
"""

import asyncio
import hashlib
import logging

from playwright.async_api import async_playwright

from .base import create_browser, close_annoying_dialogs, parse_flight_hierarchy, polite_delay
from ..db.database import Database

logger = logging.getLogger(__name__)


# All DSA topic series pages (from the TUF /home navigation)
PLAYLIST_SERIES = [
    {
        "id": "playlist_array",
        "name": "Array Series",
        "url": "https://takeuforward.org/array/top-array-interview-questions-structured-path-with-video-solutions",
    },
    {
        "id": "playlist_binary_search",
        "name": "Binary Search Series",
        "url": "https://takeuforward.org/binary-search/top-binary-search-interview-questions-structured-path-with-video-solutions",
    },
    {
        "id": "playlist_dp",
        "name": "Dynamic Programming Series",
        "url": "https://takeuforward.org/dynamic-programming/striver-dp-series-dynamic-programming-problems",
    },
    {
        "id": "playlist_graph",
        "name": "Graph Series",
        "url": "https://takeuforward.org/graph/striver-graph-series-top-graph-interview-questions",
    },
    {
        "id": "playlist_linked_list",
        "name": "Linked List Series",
        "url": "https://takeuforward.org/linked-list/top-linkedlist-interview-questions-structured-path-with-video-solutions",
    },
    {
        "id": "playlist_recursion",
        "name": "Recursion Series",
        "url": "https://takeuforward.org/recursion/top-recursion-interview-questions-structured-path-with-video-solutions",
    },
    {
        "id": "playlist_stack_queue",
        "name": "Stack & Queue Series",
        "url": "https://takeuforward.org/stack-and-queue/top-stack-and-queue-interview-questions-structured-path-with-video-solutions",
    },
    {
        "id": "playlist_string",
        "name": "String Series",
        "url": "https://takeuforward.org/string/top-string-interview-questions-structured-path-with-video-solutions",
    },
    {
        "id": "playlist_tree",
        "name": "Tree Series",
        "url": "https://takeuforward.org/tree-series/top-tree-interview-questions-structured-path-with-video-solutions",
    },
]


async def scrape_dsa_playlist(db: Database):
    """Scrape all DSA topic series pages and persist to DB."""
    print("\n[DSA Playlist] Starting...")

    async with async_playwright() as pw:
        browser, context = await create_browser(pw)
        page = await context.new_page()

        total = 0
        for src in PLAYLIST_SERIES:
            print(f"  Scraping '{src['name']}'...")
            try:
                count = await _scrape_series(page, db, src)
                await db.commit()
                total += count
                print(f"    ✓ {count} problems stored")
            except Exception as e:
                logger.error(f"Series scrape failed {src['id']}: {e}", exc_info=True)
                print(f"    ✗ Error: {e}")
            await polite_delay(2)

        await browser.close()
    print(f"  [DSA Playlist] Total: {total} problems")


async def _scrape_series(page, db: Database, src: dict) -> int:
    source_id = src["id"]
    await db.upsert_source(source_id, src["name"], "dsa_playlist", src["url"])

    html = None
    for url in [src["url"]] + src.get("fallback_urls", []):
        try:
            await page.goto(url, wait_until="domcontentloaded", timeout=30000)
            await close_annoying_dialogs(page)
            await asyncio.sleep(3)
            html = await page.content()
            if "__next_f" in html and "problem_name" in html:
                break
        except Exception as e:
            logger.warning(f"  URL failed {url}: {e}")

    if not html or "__next_f" not in html:
        logger.warning(f"  No flight data for {source_id}")
        return 0

    categories = parse_flight_hierarchy(html)
    count = 0

    for cat_idx, category in enumerate(categories):
        cat_id_raw = str(category.get("category_id") or f"pls_cat_{cat_idx}")
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
                    leetcode_url=prob.get("leetcode_url"),
                    plus_url=prob.get("plus_url"),
                    difficulty=prob.get("difficulty"),
                    order_idx=p_idx,
                    extra={"original_id": prob_id_raw, "series": source_id},
                )
                count += 1

    return count


def _short_hash(text: str) -> str:
    return hashlib.sha1(text.encode()).hexdigest()[:10]

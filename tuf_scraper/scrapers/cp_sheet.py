"""
CP Sheet Scraper
================
Striver's CP Sheet: https://takeuforward.org/competitive-programming/strivers-cp-sheet

Problems are grouped in "sections" (Implementation, Graphs, DP, etc.) and each
problem only has a Codeforces link (stored in the 'leetcode' and 'link' fields).

Data is embedded in self.__next_f flight payloads like all other TUF pages.
"""

import re
import json
import asyncio
import hashlib
import logging

from playwright.async_api import async_playwright

from .base import create_browser, close_annoying_dialogs, polite_delay
from ..db.database import Database

logger = logging.getLogger(__name__)

CP_SHEET_URL = "https://takeuforward.org/competitive-programming/strivers-cp-sheet"
SOURCE_ID    = "cp_striver_cp_sheet"

_FLIGHT_RE = re.compile(r'self\.__next_f\.push\(\[1,\s*"(.*?)"\]\)', re.DOTALL)


# ─── Entry point ─────────────────────────────────────────────────────────────

async def scrape_cp_sheet(db: Database):
    """Scrape Striver's CP Sheet and persist to DB."""
    print("\n[CP Sheet] Starting...")

    await db.upsert_source(SOURCE_ID, "Striver's CP Sheet", "cp_sheet", CP_SHEET_URL)

    async with async_playwright() as pw:
        browser, context = await create_browser(pw)
        page = await context.new_page()

        try:
            await page.goto(CP_SHEET_URL, wait_until="domcontentloaded", timeout=30000)
            await close_annoying_dialogs(page)
            await asyncio.sleep(4)
            html = await page.content()

            if "problem_name" not in html:
                print("  [CP Sheet] ✗ No problem data found in page")
                return

            categories = _parse_cp_hierarchy(html)
            if not categories:
                print("  [CP Sheet] ✗ Could not extract category structure")
                return

            print(f"  [CP Sheet] Found {len(categories)} categories")
            count = await _store_categories(db, categories)
            await db.commit()
            print(f"  [CP Sheet] ✓ {count} problems stored")

        except Exception as e:
            logger.error(f"[CP Sheet] Failed: {e}", exc_info=True)
            print(f"  [CP Sheet] ✗ Error: {e}")
        finally:
            await browser.close()


# ─── HTML parser ─────────────────────────────────────────────────────────────

def _decode_segment(raw: str) -> str:
    try:
        return json.loads(f'"{raw}"')
    except Exception:
        return raw


def _parse_cp_hierarchy(html: str) -> list[dict]:
    """
    Extract the sections array from CP Sheet flight data.
    Structure: {"sections":[{"category_id":"...","category_name":"...","problems":[...]}]}
    """
    segs = _FLIGHT_RE.findall(html)
    combined = "".join(_decode_segment(s) for s in segs)

    # Try "sections" key first (CP sheet), then "categories" (fallback)
    for key in ('"sections":[', '"categories":['):
        pos = combined.find(key)
        if pos == -1:
            continue

        bracket_start = combined.index('[', pos)
        depth = 0
        bracket_end = -1
        for i in range(bracket_start, len(combined)):
            ch = combined[i]
            if ch == '[':
                depth += 1
            elif ch == ']':
                depth -= 1
                if depth == 0:
                    bracket_end = i
                    break

        if bracket_end == -1:
            continue

        raw_arr = combined[bracket_start : bracket_end + 1]
        # Replace JS-style $undefined sentinel with JSON null
        raw_arr = raw_arr.replace('"$undefined"', 'null')
        try:
            return json.loads(raw_arr)
        except Exception as e:
            logger.debug(f"JSON parse failed for key {key!r}: {e}")
            continue

    return []


# ─── Database storage ─────────────────────────────────────────────────────────

async def _store_categories(db: Database, categories: list[dict]) -> int:
    count = 0
    for cat_idx, category in enumerate(categories):
        cat_id   = str(category.get("category_id") or f"cp_cat_{cat_idx}")
        cat_name = str(category.get("category_name") or f"Category {cat_idx}")

        topic_id = f"{SOURCE_ID}_{_short_hash(cat_id)}"
        await db.upsert_topic(topic_id, SOURCE_ID, cat_name, cat_idx)

        # CP sheet categories contain problems directly (no subcategory level)
        sub_id = f"{topic_id}_all"
        await db.upsert_subtopic(sub_id, topic_id, cat_name, 0)

        problems = category.get("problems", [])
        for p_idx, prob in enumerate(problems):
            if not isinstance(prob, dict):
                continue

            prob_id_raw = str(prob.get("problem_id") or f"{cat_id}_{p_idx}")
            name        = (prob.get("problem_name") or "Unknown").strip()

            # Codeforces URL is stored in both 'leetcode' and 'link' fields
            cf_url = _clean(prob.get("link")) or _clean(prob.get("leetcode"))

            await db.upsert_problem(
                problem_id   = f"{SOURCE_ID}_{_short_hash(prob_id_raw)}",
                name         = name,
                subtopic_id  = sub_id,
                source_id    = SOURCE_ID,
                leetcode_url = cf_url,   # Codeforces link is mapped to leetcode_url column
                difficulty   = _clean(prob.get("difficulty")),
                order_idx    = p_idx,
                extra        = {
                    "original_id": prob_id_raw,
                    "cf_url": cf_url,
                    "editorial": _clean(prob.get("editorial")),
                },
            )
            count += 1

    return count


# ─── Helpers ─────────────────────────────────────────────────────────────────

def _clean(val) -> str | None:
    if val is None:
        return None
    s = str(val).strip()
    return s if s and s != "$undefined" else None


def _short_hash(text: str) -> str:
    return hashlib.sha1(text.encode()).hexdigest()[:10]

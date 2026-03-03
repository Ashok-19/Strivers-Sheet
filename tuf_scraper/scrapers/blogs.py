"""
Blogs Scraper
=============
TUF blogs are organised by topic category at /blogs/{category}?page=N.
Individual articles live at /{category-slug}/{article-slug} (NOT /blogs/).
Each category page lists 10 articles, paginated up to ~26 pages.

Flow:
  1. scrape_blogs()        – iterate all 28 categories × all pages, store blog
                             metadata in `blogs` table, queue article URLs.
  2. scrape_blog_articles()– second-pass full content scrape (called from main.py
                             article queue loop, or standalone).
"""

import re
import asyncio
import hashlib
import logging

from playwright.async_api import async_playwright

from .base import (
    create_browser,
    close_annoying_dialogs,
    get_article_content,
    polite_delay,
)
from ..db.database import Database

logger = logging.getLogger(__name__)


# 28 blog categories from TUF /home navigation
BLOG_CATEGORIES = [
    "arrays", "basics", "binary-search", "binary-search-tree",
    "binary-tree", "bit-manipulation", "c", "core",
    "data-structure", "dynamic-programming", "graph", "greedy",
    "hashing", "heap", "interview-experience", "java",
    "js", "linked-list", "maths", "python",
    "queue", "recursion", "sliding-window", "sorting",
    "stack", "string", "trie", "two-pointers",
]

BASE_URL = "https://takeuforward.org/blogs/{category}?page={page}"

_SKIP_HREFS = {
    "https://takeuforward.org/home",
    "https://takeuforward.org/plus/home",
    "https://takeuforward.org/",
    "https://takeuforward.org",
}


async def scrape_blogs(db: Database):
    """Scrape all blog listing pages (metadata only, queues article URLs)."""
    print("\n[Blogs] Starting listing scrape...")

    source_id = "blogs"
    await db.upsert_source(source_id, "TUF Blogs", "blog", "https://takeuforward.org/blogs/")

    async with async_playwright() as pw:
        browser, context = await create_browser(pw)
        page = await context.new_page()

        total = 0
        for cat in BLOG_CATEGORIES:
            try:
                count = await _scrape_category(page, db, source_id, cat)
                total += count
                await db.commit()
                print(f"  [{cat}] {count} posts queued")
            except Exception as e:
                logger.error(f"  [{cat}] failed: {e}", exc_info=True)
                print(f"  [{cat}] Error: {e}")
            await polite_delay(0.8)

        await browser.close()

    print(f"  [Blogs] Total: {total} blog posts queued")


async def _scrape_category(page, db: Database, source_id: str, category: str) -> int:
    """Scrape all pages of one blog category."""
    count = 0
    page_num = 1

    while True:
        url = BASE_URL.format(category=category, page=page_num)
        try:
            await page.goto(url, wait_until="domcontentloaded", timeout=30000)
            await close_annoying_dialogs(page)
            await asyncio.sleep(1.5)
        except Exception as e:
            logger.warning(f"  Failed page {url}: {e}")
            break

        articles = await _extract_article_links(page)
        if not articles:
            break  # No articles -> past last page

        for art in articles:
            blog_id = f"blog_{_short_hash(art['url'])}"
            await db.upsert_blog(
                blog_id=blog_id,
                url=art["url"],
                title=art.get("title", ""),
                source_id=source_id,
                category=category,
                excerpt=art.get("excerpt", ""),
                published_date=None,
            )
            count += 1

        # Check total pages
        total_pages = await _get_total_pages(page)
        if total_pages is not None and page_num >= total_pages:
            break
        # Fallback: check "Next" button
        has_next = await page.evaluate("""
            () => {
                var btns = Array.from(document.querySelectorAll('button'));
                return btns.some(function(b) {
                    return b.textContent.trim() === 'Next' && !b.disabled;
                });
            }
        """)
        if not has_next:
            break

        page_num += 1
        await polite_delay(0.5)

    return count


async def _extract_article_links(page) -> list:
    """Extract article links from current blog listing page."""
    skip = list(_SKIP_HREFS)
    return await page.evaluate("""
        function(skipList) {
            var skipSet = new Set(skipList);
            var results = [];
            var seen = new Set();
            var anchors = Array.from(document.querySelectorAll('a[href]'));
            for (var i = 0; i < anchors.length; i++) {
                var a = anchors[i];
                var href = a.href;
                if (!href) continue;
                if (!href.startsWith('https://takeuforward.org/')) continue;
                if (skipSet.has(href)) continue;
                if (href.indexOf('/blogs/') >= 0) continue;
                if (href.indexOf('/plus/') >= 0) continue;
                if (href.indexOf('/dsa/') >= 0) continue;
                if (href.indexOf('/home') >= 0) continue;
                if (href.charAt(href.length - 1) === '#') continue;
                if (href.indexOf('?') >= 0) continue;
                var text = a.textContent.trim();
                if (!text || text.length < 5) continue;
                if (seen.has(href)) continue;
                seen.add(href);
                var title = text.split('\\n')[0].trim().substring(0, 150);
                var excerpt = '';
                var container = a.parentElement;
                if (container) {
                    var remaining = text.substring(title.length).trim();
                    if (remaining) excerpt = remaining.substring(0, 200);
                }
                results.push({ url: href, title: title, excerpt: excerpt });
            }
            return results;
        }
    """, skip)


async def _get_total_pages(page) -> int:
    """Extract total page count from 'Page X of Y' text."""
    try:
        val = await page.evaluate("""
            () => {
                var body = document.body.innerText;
                var m = body.match(/Page \\d+ of (\\d+)/);
                return m ? parseInt(m[1]) : null;
            }
        """)
        return int(val) if val else None
    except Exception:
        return None


async def scrape_blog_articles(db: Database):
    """
    Second-pass: fetch full article content for each queued blog post.
    Called from main.py article queue loop or standalone.
    """
    print("\n[Blogs] Fetching full article content...")

    async with async_playwright() as pw:
        browser, context = await create_browser(pw)
        page = await context.new_page()

        import httpx
        async with httpx.AsyncClient(
            timeout=30, follow_redirects=True,
            headers={"Referer": "https://takeuforward.org/"}
        ) as client:
            pending = await db.get_pending_blogs()
            done = 0
            for blog_id, url in pending:
                try:
                    result = await get_article_content(page, url)
                    if result["ok"]:
                        await db.update_blog_content(
                            blog_id=blog_id,
                            html_content=result["htmlContent"],
                            text_content=result["textContent"],
                            title=result.get("title") or "",
                        )
                        for i, img in enumerate(result.get("images", [])):
                            try:
                                r = await client.get(img["src"])
                                if r.status_code == 200:
                                    await db.insert_blog_image(
                                        blog_id=blog_id,
                                        src_url=img["src"],
                                        alt_text=img.get("alt", ""),
                                        order_idx=i,
                                        image_data=r.content,
                                    )
                            except Exception as ie:
                                logger.warning(f"Blog image failed {img['src']}: {ie}")
                        await db.commit()
                        done += 1
                        print(f"  [{done}] {url}")
                    else:
                        logger.warning(f"  Extraction failed: {url}")
                except Exception as e:
                    logger.error(f"  Blog article error {url}: {e}")
                await polite_delay(1)

        await browser.close()

    print(f"  [Blogs] Done – {done} articles scraped.")


def _short_hash(text: str) -> str:
    return hashlib.sha1(text.encode()).hexdigest()[:10]

"""
TUF Scraper – Main Entry Point
=================================
Orchestrates all scrapers in order:
  1. DSA Sheets (A2Z, SDE, Blind75, Striver79)
  2. Core CS (CN names-only, DBMS article+youtube, OS article)
  3. System Design (article + youtube)
  4. DSA Playlist (article + leetcode)
  5. Blogs listing (metadata + queue full content)
  6. Article queue processor (full text + images for all queued URLs)
  7. Blog full-content scraper

Usage:
  python main.py [--phase PHASE] [--limit N]

  --phase   Which phase to run: all | sheets | cs | sd | playlist | blogs | articles
              default: all
  --limit   Max article URLs to process per run (default: unlimited)
"""

import asyncio
import argparse
import logging
import time
from pathlib import Path

from tuf_scraper.db.database import init_db
from tuf_scraper.scrapers.dsa_sheets import scrape_all_dsa_sheets
from tuf_scraper.scrapers.core_cs import scrape_all_core_cs
from tuf_scraper.scrapers.system_design import scrape_system_design
from tuf_scraper.scrapers.dsa_playlist import scrape_dsa_playlist
from tuf_scraper.scrapers.blogs import scrape_blogs, scrape_blog_articles
from tuf_scraper.scrapers.cp_sheet import scrape_cp_sheet
from tuf_scraper.scrapers.interview import scrape_interview_experiences
from tuf_scraper.scrapers.base import (
    create_browser,
    close_annoying_dialogs,
    get_article_content,
    download_image,
    polite_delay,
    ASSETS_DIR,
)

# ─── Logging ─────────────────────────────────────────────────────────────────

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(name)s – %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("tuf_scraper.main")


# ─── Article queue processor ─────────────────────────────────────────────────

async def process_article_queue(db, limit: int = 0):
    """
    Drain the scrape_queue table.
    For every pending URL:
      • Visit with Playwright, extract article content + images
      • Store in DB (articles + article_images tables)
      • Mark queue item done
    """
    from playwright.async_api import async_playwright

    print("\n[Articles] Processing scrape queue...")

    async with async_playwright() as pw:
        browser, context = await create_browser(pw)
        page = await context.new_page()

        import httpx
        async with httpx.AsyncClient(
            timeout=30,
            follow_redirects=True,
            headers={"Referer": "https://takeuforward.org/"},
        ) as client:

            batch_size = 20
            total_done = 0

            while True:
                urls = await db.next_pending_urls(limit=batch_size)
                if not urls:
                    break
                if limit and total_done >= limit:
                    print(f"  Reached limit of {limit} articles.")
                    break

                for url in urls:
                    if limit and total_done >= limit:
                        break

                    await db.mark_queue_processing(url)

                    try:
                        result = await get_article_content(page, url)

                        if result["ok"]:
                            article_id = await db.upsert_article(
                                url=url,
                                title=result["title"],
                                html_content=result["htmlContent"],
                                text_content=result["textContent"],
                                scrape_ok=True,
                            )

                            # Download and store images
                            for i, img in enumerate(result.get("images", [])):
                                src = img["src"]
                                img_bytes, local_path = await download_image(
                                    client, src, article_id, i
                                )
                                await db.insert_image(
                                    article_id=article_id,
                                    src_url=src,
                                    local_path=local_path,
                                    alt_text=img.get("alt", ""),
                                    order_idx=i,
                                    content=img_bytes,
                                )

                            await db.mark_queue_done(url)
                            total_done += 1
                            print(f"  [{total_done}] ✓ {url}")
                        else:
                            await db.upsert_article(url=url, scrape_ok=False,
                                                    error_msg="extraction returned ok=False")
                            await db.mark_queue_failed(url, "ok=False")
                            print(f"  ✗ {url}")

                        await db.commit()
                        await polite_delay(1)

                    except Exception as e:
                        logger.error(f"Article failed {url}: {e}", exc_info=True)
                        await db.mark_queue_failed(url, str(e))
                        await db.commit()

        await browser.close()

    print(f"\n  Done – processed {total_done} articles.")


# ─── Stats printer ───────────────────────────────────────────────────────────

async def print_stats(db):
    stats = await db.stats()
    print("\n" + "─" * 50)
    print("  DATABASE SUMMARY")
    print("─" * 50)
    for key, val in stats.items():
        print(f"  {key:<25} {val:>8,}")
    print("─" * 50)


# ─── Main runner ─────────────────────────────────────────────────────────────

async def main(phase: str, limit: int):
    t0 = time.time()
    print("=" * 50)
    print("  TUF SCRAPER")
    print(f"  Phase: {phase}  |  Article limit: {limit or 'unlimited'}")
    print("=" * 50)

    db = await init_db()

    try:
        run_all = phase == "all"

        if run_all or phase == "sheets":
            await scrape_all_dsa_sheets(db)

        if run_all or phase == "cs":
            await scrape_all_core_cs(db)

        if run_all or phase == "sd":
            await scrape_system_design(db)

        if run_all or phase == "playlist":
            await scrape_dsa_playlist(db)

        if run_all or phase == "blogs":
            await scrape_blogs(db)

        if run_all or phase == "cp":
            await scrape_cp_sheet(db)

        if run_all or phase == "interview":
            await scrape_interview_experiences(db)

        if run_all or phase == "articles":
            await process_article_queue(db, limit=limit)
            # Also scrape blog full content
            await scrape_blog_articles(db)

        await print_stats(db)

    finally:
        await db.close()

    elapsed = time.time() - t0
    print(f"\nTotal time: {elapsed:.1f}s")


# ─── CLI ──────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="TUF website scraper")
    parser.add_argument(
        "--phase",
        default="all",
        choices=["all", "sheets", "cs", "sd", "playlist", "blogs", "cp", "interview", "articles"],
        help="Which scraping phase to run (default: all)",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=0,
        help="Max number of articles to process (0 = unlimited)",
    )
    args = parser.parse_args()
    asyncio.run(main(args.phase, args.limit))

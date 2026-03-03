"""
Base scraper utilities for TUF
================================
Key techniques:
  1. parse_flight_data()  – TUF uses Next.js React Server Components which embed
                            all page data in `self.__next_f.push([1, "..."])` script
                            tags. We collect these, decode the escaped JSON and
                            extract all problem objects.
  2. get_article_content()– Articles rendered client-side. The main content lives
                            inside an element that has class "article" (Tailwind).
  3. download_image()     – Async HTTP download of article images.
"""

import re
import json
import asyncio
import hashlib
import logging
from pathlib import Path
from typing import Optional
from urllib.parse import urljoin, urlparse

import httpx
from playwright.async_api import async_playwright, Page, BrowserContext, Browser

logger = logging.getLogger(__name__)

ASSETS_DIR = Path(__file__).parent.parent.parent / "assets" / "images"
ASSETS_DIR.mkdir(parents=True, exist_ok=True)

USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/122.0.0.0 Safari/537.36"
)

# ────────────────────────────────────────────────────────────────────
#  Browser factory
# ────────────────────────────────────────────────────────────────────

async def create_browser(playwright) -> tuple[Browser, BrowserContext]:
    browser = await playwright.chromium.launch(
        headless=True,
        args=["--no-sandbox", "--disable-blink-features=AutomationControlled"]
    )
    context = await browser.new_context(
        user_agent=USER_AGENT,
        viewport={"width": 1440, "height": 900},
        extra_http_headers={"Accept-Language": "en-US,en;q=0.9"},
    )
    # Dismiss any dialogs automatically
    context.on("dialog", lambda d: asyncio.ensure_future(d.dismiss()))
    return browser, context


async def close_annoying_dialogs(page: Page):
    """Close any modals / login popups that TUF shows."""
    for selector in [
        'button:has-text("Continue without login")',
        'button:has-text("Close")',
        'button[aria-label="Close"]',
        '[data-testid="modal-close"]',
    ]:
        try:
            btn = page.locator(selector).first
            if await btn.is_visible(timeout=500):
                await btn.click(timeout=500)
                await asyncio.sleep(0.3)
        except Exception:
            pass


# ────────────────────────────────────────────────────────────────────
#  Next.js "flight" data parser
# ────────────────────────────────────────────────────────────────────

_FLIGHT_RE = re.compile(r'self\.__next_f\.push\(\[1,\s*"(.*?)"\]\)', re.DOTALL)


def _decode_flight_segment(raw: str) -> str:
    """Un-escape a single self.__next_f segment."""
    # The string is JS-escaped. Use json.loads with surrounding quotes.
    try:
        return json.loads(f'"{raw}"')
    except Exception:
        # Fallback: basic unescape
        return raw.encode().decode("unicode_escape", errors="replace")


def parse_flight_data(html: str) -> list[dict]:
    """
    Extract all problem/question objects embedded in Next.js flight data.

    TUF embeds sheet data in many self.__next_f script tags.
    Each problem looks like:
      {"problem_id":"87","problem_name":"...","article":"https://...","youtube":"...","leetcode":"...","difficulty":"Easy"}
    Topics / subcategories wrap these:
      {"category_id":"...","category_name":"...","subcategories":[...]}
      {"subcategory_id":"...","subcategory_name":"...","problems":[...]}
    """
    segments = _FLIGHT_RE.findall(html)
    combined = "".join(_decode_flight_segment(s) for s in segments)
    return _extract_structured_data(combined)


def _extract_structured_data(text: str) -> list[dict]:
    """
    Walk through the combined flight text and pull out category/problem hierarchy.
    Returns a flat list of problems with 'category' and 'subcategory' added.
    """
    problems = []
    
    # Try to find category JSON objects
    # Pattern: JSON objects with "category_id" or "subcategory_id"
    for match in re.finditer(r'\{[^{}]*"category_id"[^{}]*\}', text, re.DOTALL):
        try:
            obj = json.loads(match.group())
            # This is a category - find subcategories in surrounding text
        except Exception:
            pass

    # Better approach: extract ALL JSON objects and classify them
    # Extract problem objects: must have problem_name + (article or leetcode)
    for match in re.finditer(r'\{[^{}]{20,3000}\}', text, re.DOTALL):
        raw = match.group()
        try:
            obj = json.loads(raw)
        except Exception:
            continue

        if not isinstance(obj, dict):
            continue

        # Problem object detection
        if "problem_name" in obj and ("article" in obj or "leetcode" in obj):
            problems.append(_normalize_problem_flight(obj))

    return problems


def _normalize_problem_flight(raw: dict) -> dict:
    """Normalize a TUF flight problem object to a consistent dict."""
    def clean(val):
        if val is None or val == "$undefined":
            return None
        return str(val).strip() or None

    article = clean(raw.get("article"))
    youtube = clean(raw.get("youtube"))
    leetcode = clean(raw.get("leetcode"))
    plus = clean(raw.get("plus"))

    # Skip plus-only articles (locked content)
    if article and "/plus/" in article:
        article = None

    return {
        "problem_id": str(raw.get("problem_id", "")),
        "problem_name": str(raw.get("problem_name", "Unknown")).strip(),
        "article_url": article,
        "youtube_url": youtube,
        "leetcode_url": leetcode,
        "plus_url": plus,
        "difficulty": clean(raw.get("difficulty")),
        "_raw": raw,
    }


def parse_flight_hierarchy(html: str) -> list[dict]:
    """
    Extract the full category -> subcategory -> problems hierarchy from flight data.
    Returns list of category dicts, each with subcategories, each with problems.
    """
    segments = _FLIGHT_RE.findall(html)
    # Use "" join — "\n" join can split JSON token boundaries
    combined = "".join(_decode_flight_segment(s) for s in segments)

    # Balanced-bracket extraction of the "sections" array (same key as CP sheet)
    for key in ('"sections":[', '"categories":['):
        pos = combined.find(key)
        if pos == -1:
            continue
        bracket_start = combined.index('[', pos)
        depth, bracket_end = 0, -1
        for i in range(bracket_start, len(combined)):
            if combined[i] == '[':
                depth += 1
            elif combined[i] == ']':
                depth -= 1
                if depth == 0:
                    bracket_end = i
                    break
        if bracket_end == -1:
            continue
        raw = combined[bracket_start:bracket_end + 1].replace('"$undefined"', 'null')
        try:
            sections = json.loads(raw)
            if sections:
                # Normalize problem objects so downstream code always gets
                # article_url/youtube_url/leetcode_url/plus_url (not article/youtube/...)
                for cat in sections:
                    for subcat in cat.get("subcategories", []):
                        subcat["problems"] = [
                            _normalize_problem_flight(p) for p in subcat.get("problems", [])
                        ]
                    if "problems" in cat:
                        cat["problems"] = [
                            _normalize_problem_flight(p) for p in cat["problems"]
                        ]
                return sections
        except Exception as e:
            logger.debug(f"parse_flight_hierarchy JSON parse failed for key {key!r}: {e}")

    # Fallback: no category structure found, return flat list
    return [{"category_id": "0", "category_name": "All",
             "subcategories": [{"subcategory_id": "0", "subcategory_name": "All",
                                "problems": parse_flight_data(html)}]}]


# ────────────────────────────────────────────────────────────────────
#  Article content extraction
# ────────────────────────────────────────────────────────────────────

async def get_article_content(page: Page, url: str) -> dict:
    """
    Navigate to an article page and extract:
      - title
      - full HTML of the article body
      - plain text
      - list of image src URLs
    """
    try:
        await page.goto(url, wait_until="domcontentloaded", timeout=45000)
        await close_annoying_dialogs(page)
        # Give RSC time to hydrate
        await asyncio.sleep(2)
        await close_annoying_dialogs(page)
    except Exception as e:
        return {"ok": False, "error": str(e), "url": url}

    result = await page.evaluate("""
    () => {
        // ── Title ──────────────────────────────────────────────
        const titleEl = document.querySelector('h1, title, [class*="article"] h2');
        let title = titleEl ? titleEl.innerText?.trim() : document.title;

        // ── Article body ───────────────────────────────────────
        // TUF article container has Tailwind class "article" in its className
        // It's typically: div.text-lg.article.font-medium... or similar
        let articleEl = null;
        
        // Strategy 1: Find by class containing "article"
        const candidates = document.querySelectorAll('[class*=" article "], [class*="article "], [class^="article"]');
        for (const el of candidates) {
            if (el.innerHTML.length > 2000) {
                articleEl = el;
                break;
            }
        }
        
        // Strategy 2: Find div with class="... article ..." exact token
        if (!articleEl) {
            const divs = document.querySelectorAll('div');
            for (const div of divs) {
                const classes = div.className.split(' ');
                if (classes.includes('article') && div.innerHTML.length > 1000) {
                    articleEl = div;
                    break;
                }
            }
        }
        
        // Strategy 3: Fallback to largest meaningful content block
        if (!articleEl) {
            let best = null, bestLen = 0;
            for (const div of document.querySelectorAll('div')) {
                const len = div.innerHTML.length;
                if (len > bestLen && len < 500000 && div.querySelectorAll('p').length > 2) {
                    best = div;
                    bestLen = len;
                }
            }
            articleEl = best;
        }

        if (!articleEl) return { ok: false, error: 'no article element found' };

        // ── Extract HTML (keep images, code, headings) ─────────
        // Remove scripts, buttons, interactive PLUS-locked parts
        const clone = articleEl.cloneNode(true);
        for (const rm of clone.querySelectorAll('script, style, button, [class*="plus"], [class*="locked"]')) {
            rm.remove();
        }
        const htmlContent = clone.innerHTML;

        // ── Plain text ─────────────────────────────────────────
        const textContent = articleEl.innerText?.trim() || '';

        // ── Images ─────────────────────────────────────────────
        const images = [];
        let imgIdx = 0;
        for (const img of articleEl.querySelectorAll('img')) {
            const src = img.src || img.dataset?.src || img.getAttribute('data-src');
            if (!src || src.includes('icon') || src.includes('logo') || src.includes('avatar')) continue;
            // Skip tiny icons
            if (img.naturalWidth > 0 && img.naturalWidth < 30) continue;
            images.push({
                src: src,
                alt: img.alt || '',
                order: imgIdx++,
            });
        }

        return { ok: true, title, htmlContent, textContent, images };
    }
    """)

    result["url"] = url
    return result


# ────────────────────────────────────────────────────────────────────
#  Image downloader
# ────────────────────────────────────────────────────────────────────

async def download_image(
    client: httpx.AsyncClient,
    src_url: str,
    article_id: str,
    order_idx: int,
) -> tuple[Optional[bytes], Optional[str]]:
    """
    Download image bytes and return (bytes, local_relative_path).
    Returns (None, None) on failure.
    """
    try:
        resp = await client.get(src_url, timeout=20, follow_redirects=True)
        if resp.status_code != 200:
            return None, None

        content_type = resp.headers.get("content-type", "image/jpeg")
        ext = _content_type_to_ext(content_type, src_url)
        
        fname = f"{article_id}_{order_idx:03d}{ext}"
        local_path = ASSETS_DIR / article_id / fname
        local_path.parent.mkdir(parents=True, exist_ok=True)
        local_path.write_bytes(resp.content)

        rel_path = str(local_path.relative_to(ASSETS_DIR.parent.parent))
        return resp.content, rel_path
    except Exception as e:
        logger.warning(f"Failed to download image {src_url}: {e}")
        return None, None


def _content_type_to_ext(ct: str, url: str) -> str:
    ct = ct.lower()
    if "png" in ct:
        return ".png"
    elif "gif" in ct:
        return ".gif"
    elif "webp" in ct:
        return ".webp"
    elif "svg" in ct:
        return ".svg"
    # Try from URL
    path = urlparse(url).path
    if "." in path.split("/")[-1]:
        return "." + path.rsplit(".", 1)[-1][:5].lower()
    return ".jpg"


# ────────────────────────────────────────────────────────────────────
#  Polite delay
# ────────────────────────────────────────────────────────────────────

async def polite_delay(seconds: float = 1.5):
    await asyncio.sleep(seconds)

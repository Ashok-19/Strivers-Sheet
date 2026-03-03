"""
SQLite Database Layer for TUF Scraper
======================================
Schema:
  sources       -> top-level content origins (a2z, sde, blogs, etc.)
  topics        -> sections/categories within a source
  subtopics     -> sub-sections within a topic
  problems      -> individual problems/questions
  articles      -> scraped article full content
  article_images -> images extracted from articles (BLOB + metadata)
  blogs         -> blog-specific metadata
"""

import sqlite3
import aiosqlite
import asyncio
import hashlib
import json
import logging
import time
from pathlib import Path
from typing import Optional, Any

logger = logging.getLogger(__name__)

DB_PATH = Path(__file__).parent.parent.parent / "tuf_data.db"


SCHEMA = """
PRAGMA journal_mode = WAL;
PRAGMA foreign_keys = ON;

-- ─────────────────────────────────────────────────────
-- Sources
-- ─────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS sources (
    id          TEXT PRIMARY KEY,
    name        TEXT NOT NULL,
    type        TEXT NOT NULL,
    url         TEXT NOT NULL,
    created_at  REAL DEFAULT (strftime('%s','now'))
);

-- ─────────────────────────────────────────────────────
-- Topics  (categories inside a source)
-- ─────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS topics (
    id          TEXT PRIMARY KEY,
    source_id   TEXT NOT NULL REFERENCES sources(id) ON DELETE CASCADE,
    name        TEXT NOT NULL,
    order_idx   INTEGER DEFAULT 0
);

-- ─────────────────────────────────────────────────────
-- Subtopics  (subcategories inside a topic)
-- ─────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS subtopics (
    id          TEXT PRIMARY KEY,
    topic_id    TEXT NOT NULL REFERENCES topics(id) ON DELETE CASCADE,
    name        TEXT NOT NULL,
    order_idx   INTEGER DEFAULT 0
);

-- ─────────────────────────────────────────────────────
-- Problems / Questions
-- ─────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS problems (
    id              TEXT PRIMARY KEY,
    subtopic_id     TEXT REFERENCES subtopics(id) ON DELETE CASCADE,
    source_id       TEXT REFERENCES sources(id) ON DELETE CASCADE,
    name            TEXT NOT NULL,
    article_url     TEXT,
    youtube_url     TEXT,
    leetcode_url    TEXT,
    plus_url        TEXT,
    difficulty      TEXT,
    order_idx       INTEGER DEFAULT 0,
    article_id      TEXT REFERENCES articles(id),
    extra_json      TEXT        -- JSON blob for additional metadata
);

CREATE INDEX IF NOT EXISTS idx_problems_source  ON problems(source_id);
CREATE INDEX IF NOT EXISTS idx_problems_article ON problems(article_url);

-- ─────────────────────────────────────────────────────
-- Articles  (full scraped content)
-- ─────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS articles (
    id              TEXT PRIMARY KEY,
    url             TEXT UNIQUE NOT NULL,
    title           TEXT,
    html_content    TEXT,       -- Full article HTML (sanitised, no scripts)
    text_content    TEXT,       -- Plain-text version for search
    scraped_at      REAL,       -- Unix timestamp
    scrape_ok       INTEGER DEFAULT 0,  -- 1 = success, 0 = not done / failed
    error_msg       TEXT
);

CREATE INDEX IF NOT EXISTS idx_articles_url ON articles(url);

-- ─────────────────────────────────────────────────────
-- Article Images
-- ─────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS article_images (
    id          TEXT PRIMARY KEY,
    article_id  TEXT NOT NULL REFERENCES articles(id) ON DELETE CASCADE,
    src_url     TEXT NOT NULL,          -- original CDN URL
    local_path  TEXT,                   -- relative path under assets/images/
    alt_text    TEXT,
    order_idx   INTEGER DEFAULT 0,
    width       INTEGER,
    height      INTEGER,
    content     BLOB                    -- binary image bytes
);

CREATE INDEX IF NOT EXISTS idx_images_article ON article_images(article_id);

-- ─────────────────────────────────────────────────────
-- Blogs  (additional blog metadata)
-- ─────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS blogs (
    id          TEXT PRIMARY KEY,
    source_id   TEXT REFERENCES sources(id),
    title       TEXT NOT NULL,
    url         TEXT UNIQUE NOT NULL,
    category    TEXT,
    excerpt     TEXT,
    pub_date    TEXT,
    html_content TEXT,
    text_content TEXT,
    scraped_at  REAL,
    scrape_ok   INTEGER DEFAULT 0,
    article_id  TEXT REFERENCES articles(id)
);

CREATE INDEX IF NOT EXISTS idx_blogs_url     ON blogs(url);
CREATE INDEX IF NOT EXISTS idx_blogs_source  ON blogs(source_id);

-- ─────────────────────────────────────────────────────
-- System Design Topics  (extra metadata)
-- ─────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS system_design_topics (
    id          TEXT PRIMARY KEY,
    problem_id  TEXT REFERENCES problems(id) ON DELETE CASCADE,
    section     TEXT,
    has_article INTEGER DEFAULT 0,
    has_youtube INTEGER DEFAULT 0
);

-- ─────────────────────────────────────────────────────
-- Scrape Queue  (for resumable scraping)
-- ─────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS scrape_queue (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    url         TEXT UNIQUE NOT NULL,
    priority    INTEGER DEFAULT 5,
    status      TEXT DEFAULT 'pending' CHECK(status IN ('pending','processing','done','failed')),
    attempts    INTEGER DEFAULT 0,
    last_tried  REAL,
    error_msg   TEXT
);

CREATE INDEX IF NOT EXISTS idx_queue_status ON scrape_queue(status, priority DESC);
"""


def _make_id(text: str) -> str:
    """Create a stable short ID from any string."""
    return hashlib.sha1(text.encode()).hexdigest()[:16]


class Database:
    """Async wrapper around aiosqlite for TUF data."""

    def __init__(self, path: Path = DB_PATH):
        self.path = path
        self._conn: Optional[aiosqlite.Connection] = None

    async def connect(self):
        self._conn = await aiosqlite.connect(self.path)
        self._conn.row_factory = aiosqlite.Row
        await self._conn.executescript(SCHEMA)
        await self._conn.commit()
        await self._run_migrations()
        return self

    async def _run_migrations(self):
        """Apply schema migrations for existing databases."""
        # Check if sources table still has the original narrow CHECK constraint
        # that only allows the first 5 type values.
        row = await self.fetchone(
            "SELECT sql FROM sqlite_master WHERE type='table' AND name='sources'"
        )
        if not row:
            return
        current_sql = row[0] or ""
        old_check = "CHECK(type IN"
        if old_check not in current_sql:
            return  # already migrated (new install has no CHECK, existing is already wide)
        # Recreate sources without the restrictive CHECK so new types are accepted
        await self._conn.executescript("""
            PRAGMA foreign_keys = OFF;
            CREATE TABLE sources_v2 (
                id          TEXT PRIMARY KEY,
                name        TEXT NOT NULL,
                type        TEXT NOT NULL,
                url         TEXT NOT NULL,
                created_at  REAL DEFAULT (strftime('%s','now'))
            );
            INSERT OR IGNORE INTO sources_v2 SELECT * FROM sources;
            DROP TABLE sources;
            ALTER TABLE sources_v2 RENAME TO sources;
            PRAGMA foreign_keys = ON;
        """)
        await self._conn.commit()
        logger.info("DB migration: sources.type CHECK constraint removed")

    async def close(self):
        if self._conn:
            await self._conn.close()

    async def __aenter__(self):
        return await self.connect()

    async def __aexit__(self, *args):
        await self.close()

    # ─── Generic helpers ─────────────────────────────────────────────────────

    async def execute(self, sql: str, params=()) -> aiosqlite.Cursor:
        return await self._conn.execute(sql, params)

    async def executemany(self, sql: str, data: list):
        await self._conn.executemany(sql, data)

    async def commit(self):
        await self._conn.commit()

    async def fetchall(self, sql: str, params=()) -> list:
        cur = await self._conn.execute(sql, params)
        return await cur.fetchall()

    async def fetchone(self, sql: str, params=()) -> Optional[Any]:
        cur = await self._conn.execute(sql, params)
        return await cur.fetchone()

    # ─── Sources ─────────────────────────────────────────────────────────────

    async def upsert_source(self, source_id: str, name: str, stype: str, url: str):
        await self._conn.execute(
            """INSERT INTO sources(id, name, type, url) VALUES(?,?,?,?)
               ON CONFLICT(id) DO UPDATE SET name=excluded.name, url=excluded.url""",
            (source_id, name, stype, url)
        )

    # ─── Topics / Subtopics ──────────────────────────────────────────────────

    async def upsert_topic(self, topic_id: str, source_id: str, name: str, order_idx: int = 0):
        await self._conn.execute(
            """INSERT INTO topics(id, source_id, name, order_idx) VALUES(?,?,?,?)
               ON CONFLICT(id) DO UPDATE SET name=excluded.name""",
            (topic_id, source_id, name, order_idx)
        )

    async def upsert_subtopic(self, sub_id: str, topic_id: str, name: str, order_idx: int = 0):
        await self._conn.execute(
            """INSERT INTO subtopics(id, topic_id, name, order_idx) VALUES(?,?,?,?)
               ON CONFLICT(id) DO UPDATE SET name=excluded.name""",
            (sub_id, topic_id, name, order_idx)
        )

    # ─── Problems ────────────────────────────────────────────────────────────

    async def upsert_problem(
        self,
        problem_id: str,
        name: str,
        *,
        subtopic_id: Optional[str] = None,
        source_id: Optional[str] = None,
        article_url: Optional[str] = None,
        youtube_url: Optional[str] = None,
        leetcode_url: Optional[str] = None,
        plus_url: Optional[str] = None,
        difficulty: Optional[str] = None,
        order_idx: int = 0,
        extra: Optional[dict] = None,
    ):
        await self._conn.execute(
            """INSERT INTO problems(
                id, subtopic_id, source_id, name,
                article_url, youtube_url, leetcode_url, plus_url,
                difficulty, order_idx, extra_json
               ) VALUES(?,?,?,?,?,?,?,?,?,?,?)
               ON CONFLICT(id) DO UPDATE SET
                subtopic_id=excluded.subtopic_id,
                article_url=excluded.article_url,
                youtube_url=excluded.youtube_url,
                leetcode_url=excluded.leetcode_url,
                plus_url=excluded.plus_url,
                difficulty=excluded.difficulty,
                order_idx=excluded.order_idx""",
            (
                problem_id, subtopic_id, source_id, name,
                article_url, youtube_url, leetcode_url, plus_url,
                difficulty, order_idx,
                json.dumps(extra) if extra else None,
            )
        )
        # Queue article for scraping
        if article_url:
            await self.queue_url(article_url)

    # ─── Articles ────────────────────────────────────────────────────────────

    async def upsert_article(
        self,
        url: str,
        title: Optional[str] = None,
        html_content: Optional[str] = None,
        text_content: Optional[str] = None,
        scrape_ok: bool = True,
        error_msg: Optional[str] = None,
    ) -> str:
        article_id = _make_id(url)
        await self._conn.execute(
            """INSERT INTO articles(id, url, title, html_content, text_content, scraped_at, scrape_ok, error_msg)
               VALUES(?,?,?,?,?,?,?,?)
               ON CONFLICT(url) DO UPDATE SET
                title=excluded.title,
                html_content=excluded.html_content,
                text_content=excluded.text_content,
                scraped_at=excluded.scraped_at,
                scrape_ok=excluded.scrape_ok,
                error_msg=excluded.error_msg""",
            (article_id, url, title, html_content, text_content, time.time(), int(scrape_ok), error_msg)
        )
        # Update problems that reference this article URL
        await self._conn.execute(
            "UPDATE problems SET article_id=? WHERE article_url=?",
            (article_id, url)
        )
        return article_id

    async def get_article_id(self, url: str) -> Optional[str]:
        row = await self.fetchone("SELECT id FROM articles WHERE url=?", (url,))
        return row[0] if row else None

    async def article_already_scraped(self, url: str) -> bool:
        row = await self.fetchone("SELECT scrape_ok FROM articles WHERE url=? AND scrape_ok=1", (url,))
        return row is not None

    # ─── Article Images ──────────────────────────────────────────────────────

    async def insert_image(
        self,
        article_id: str,
        src_url: str,
        *,
        local_path: Optional[str] = None,
        alt_text: Optional[str] = None,
        order_idx: int = 0,
        content: Optional[bytes] = None,
    ) -> str:
        img_id = _make_id(f"{article_id}:{src_url}:{order_idx}")
        await self._conn.execute(
            """INSERT OR REPLACE INTO article_images(id, article_id, src_url, local_path, alt_text, order_idx, content)
               VALUES(?,?,?,?,?,?,?)""",
            (img_id, article_id, src_url, local_path, alt_text, order_idx, content)
        )
        return img_id

    # ─── Blogs ───────────────────────────────────────────────────────────────

    async def upsert_blog(
        self,
        *,
        blog_id: Optional[str] = None,
        url: str,
        title: str,
        source_id: Optional[str] = None,
        category: Optional[str] = None,
        excerpt: Optional[str] = None,
        published_date: Optional[str] = None,
        article_id: Optional[str] = None,
    ):
        if not blog_id:
            blog_id = _make_id(url)
        await self._conn.execute(
            """INSERT INTO blogs(id, source_id, title, url, category, excerpt, pub_date, article_id)
               VALUES(?,?,?,?,?,?,?,?)
               ON CONFLICT(url) DO UPDATE SET
                title=excluded.title,
                source_id=COALESCE(excluded.source_id, blogs.source_id),
                category=excluded.category,
                excerpt=excluded.excerpt,
                pub_date=excluded.pub_date,
                article_id=COALESCE(excluded.article_id, blogs.article_id)""",
            (blog_id, source_id, title, url, category, excerpt, published_date, article_id)
        )
        # Queue blog URL for full article scrape
        await self.queue_url(url, priority=3)

    async def get_pending_blogs(self, limit: int = 50) -> list[tuple[str, str]]:
        """Return (blog_id, url) pairs not yet scraped."""
        rows = await self.fetchall(
            """SELECT id, url FROM blogs WHERE scrape_ok = 0 LIMIT ?""",
            (limit,)
        )
        return [(r[0], r[1]) for r in rows]

    async def update_blog_content(
        self,
        blog_id: str,
        *,
        html_content: Optional[str] = None,
        text_content: Optional[str] = None,
        title: Optional[str] = None,
    ):
        await self._conn.execute(
            """UPDATE blogs SET html_content=?, text_content=?,
               title=COALESCE(?, title),
               scraped_at=?, scrape_ok=1
               WHERE id=?""",
            (html_content, text_content, title, time.time(), blog_id)
        )

    async def insert_blog_image(
        self,
        blog_id: str,
        src_url: str,
        alt_text: Optional[str] = None,
        order_idx: int = 0,
        image_data: Optional[bytes] = None,
    ) -> str:
        img_id = _make_id(f"blog:{blog_id}:{src_url}:{order_idx}")
        await self._conn.execute(
            """INSERT OR REPLACE INTO article_images(
                id, article_id, src_url, alt_text, order_idx, content
               ) VALUES(?,?,?,?,?,?)""",
            (img_id, blog_id, src_url, alt_text, order_idx, image_data)
        )
        return img_id

    # ─── Scrape Queue ────────────────────────────────────────────────────────

    async def queue_url(self, url: str, priority: int = 5):
        await self._conn.execute(
            """INSERT OR IGNORE INTO scrape_queue(url, priority) VALUES(?,?)""",
            (url, priority)
        )

    async def next_pending_urls(self, limit: int = 20) -> list:
        rows = await self.fetchall(
            """SELECT url FROM scrape_queue
               WHERE status='pending' AND attempts < 3
               ORDER BY priority DESC, id ASC
               LIMIT ?""",
            (limit,)
        )
        return [r[0] for r in rows]

    async def mark_queue_processing(self, url: str):
        await self._conn.execute(
            "UPDATE scrape_queue SET status='processing', last_tried=?, attempts=attempts+1 WHERE url=?",
            (time.time(), url)
        )

    async def mark_queue_done(self, url: str):
        await self._conn.execute(
            "UPDATE scrape_queue SET status='done' WHERE url=?", (url,)
        )

    async def mark_queue_failed(self, url: str, error: str):
        await self._conn.execute(
            "UPDATE scrape_queue SET status='failed', error_msg=? WHERE url=?", (error, url)
        )

    # ─── System Design ───────────────────────────────────────────────────────

    async def upsert_sd_extra(self, problem_id: str, section: str, has_article: bool, has_youtube: bool):
        sd_id = _make_id(problem_id)
        await self._conn.execute(
            """INSERT OR REPLACE INTO system_design_topics(id, problem_id, section, has_article, has_youtube)
               VALUES(?,?,?,?,?)""",
            (sd_id, problem_id, section, int(has_article), int(has_youtube))
        )

    # ─── Stats ───────────────────────────────────────────────────────────────

    async def stats(self) -> dict:
        results = {}
        for table in ["sources", "topics", "subtopics", "problems", "articles", "article_images", "blogs"]:
            row = await self.fetchone(f"SELECT COUNT(*) FROM {table}")
            results[table] = row[0] if row else 0
        row = await self.fetchone("SELECT COUNT(*) FROM articles WHERE scrape_ok=1")
        results["articles_scraped"] = row[0] if row else 0
        row = await self.fetchone("SELECT COUNT(*) FROM scrape_queue WHERE status='pending'")
        results["queue_pending"] = row[0] if row else 0
        return results


async def init_db(path: Path = DB_PATH) -> Database:
    """Initialize and return a connected Database instance."""
    db = Database(path)
    await db.connect()
    return db

"""
TUF Archive Web App
===================
FastAPI + Jinja2 web viewer for the scraped TUF data.

Run:
    cd webapp && uvicorn app:app --reload --port 8000
Or:
    python webapp/app.py
"""

import os
import re
import json
import sqlite3
import smtplib
from email.mime.text import MIMEText
from contextlib import contextmanager
from typing import Optional

from fastapi import FastAPI, Request, Query, Form, HTTPException
from fastapi.responses import HTMLResponse, Response, JSONResponse
from fastapi.templating import Jinja2Templates
import uvicorn

# ── email config (set env vars to enable the feature-request form) ───────────
SMTP_USER    = os.environ.get("SMTP_USER", "")
SMTP_PASS    = os.environ.get("SMTP_PASS", "")
FEATURE_MAIL = "ashokraja1910@gmail.com"

# ── paths ──────────────────────────────────────────────────────────────────────
BASE_DIR   = os.path.dirname(os.path.abspath(__file__))
DB_PATH    = os.path.join(BASE_DIR, "../tuf_data.db")
TMPL_DIR   = os.path.join(BASE_DIR, "templates")

app       = FastAPI(title="TUF Archive")
templates = Jinja2Templates(directory=TMPL_DIR)

# ── source metadata ────────────────────────────────────────────────────────────
NAV = {
    "sheets": {
        "label": "DSA Sheets",
        "icon":  "layers",
        "items": ["sheet_a2z", "sheet_sde", "sheet_blind75", "sheet_striver79"],
    },
    "cs": {
        "label": "Core CS",
        "icon":  "cpu",
        "items": ["cs_cn", "cs_dbms", "cs_os"],
    },
    "sd": {
        "label": "System Design",
        "icon":  "network",
        "items": ["system_design"],
    },
    "playlist": {
        "label": "DSA Playlist",
        "icon":  "play-circle",
        "items": [
            "playlist_array", "playlist_binary_search", "playlist_dp",
            "playlist_graph", "playlist_linked_list", "playlist_recursion",
            "playlist_stack_queue", "playlist_string", "playlist_tree",
        ],
    },
    "cp": {
        "label": "CP Sheet",
        "icon":  "zap",
        "items": ["cp_striver_cp_sheet"],
    },
    "interview": {
        "label": "Interviews",
        "icon":  "briefcase",
        "items": [],
    },
}

# ── db helpers ─────────────────────────────────────────────────────────────────
@contextmanager
def get_db():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    try:
        yield conn
    finally:
        conn.close()


def row2dict(row):
    return dict(row) if row else None


def rows2list(rows):
    return [dict(r) for r in rows]


def _get_pcnt() -> dict:
    with get_db() as db:
        return _problem_counts(db)


def _load_nav_sources(db) -> dict:
    """Return source metadata dict keyed by id."""
    rows = db.execute("SELECT id, name, type FROM sources").fetchall()
    return {r["id"]: dict(r) for r in rows}


def _problem_counts(db) -> dict:
    rows = db.execute("SELECT source_id, COUNT(*) as cnt FROM problems GROUP BY source_id").fetchall()
    return {r["source_id"]: r["cnt"] for r in rows}


# ── inject nav globals on startup ────────────────────────────────────────────
@app.on_event("startup")
def _startup():
    templates.env.globals["pcnt"] = _get_pcnt()
    templates.env.globals["nav"]  = NAV


# ── routes ─────────────────────────────────────────────────────────────────────

@app.get("/", response_class=HTMLResponse)
def home(request: Request):
    with get_db() as db:
        stats = {
            t: db.execute(f"SELECT COUNT(*) FROM {t}").fetchone()[0]
            for t in ("problems", "articles", "blogs", "sources")
        }
        stats["articles_with_content"] = db.execute(
            "SELECT COUNT(*) FROM articles WHERE html_content IS NOT NULL"
        ).fetchone()[0]
        pcnt = _problem_counts(db)
        blog_cats = rows2list(
            db.execute("SELECT category, COUNT(*) as cnt FROM blogs GROUP BY category ORDER BY cnt DESC").fetchall()
        )
    # also refresh global so sidebar is up-to-date
    templates.env.globals["pcnt"] = pcnt
    return templates.TemplateResponse("home.html", {
        "request": request, "stats": stats,
        "pcnt": pcnt, "blog_cats": blog_cats,
    })


@app.get("/problems/{source_id}", response_class=HTMLResponse)
def problems(request: Request, source_id: str,
             topic: Optional[str] = None,
             diff: Optional[str] = None,
             q: Optional[str] = None):
    with get_db() as db:
        src = row2dict(db.execute("SELECT * FROM sources WHERE id=?", (source_id,)).fetchone())
        if not src:
            raise HTTPException(404, "Source not found")

        # topics for filter sidebar
        topic_rows = rows2list(
            db.execute("SELECT DISTINCT t.name FROM topics t WHERE t.source_id=? ORDER BY t.order_idx", (source_id,)).fetchall()
        )
        topics = [r["name"] for r in topic_rows]

        # build query
        sql = """
            SELECT p.id, p.name, p.difficulty, p.article_url, p.youtube_url,
                   p.leetcode_url, p.article_id,
                   t.name AS topic_name, st.name AS subtopic_name,
                   p.order_idx
            FROM problems p
            LEFT JOIN subtopics st ON p.subtopic_id = st.id
            LEFT JOIN topics t ON st.topic_id = t.id
            WHERE p.source_id = ?
        """
        params = [source_id]

        if topic and topic != "All":
            sql += " AND t.name = ?"
            params.append(topic)
        if diff:
            sql += " AND LOWER(p.difficulty) = LOWER(?)"
            params.append(diff)
        if q:
            sql += " AND LOWER(p.name) LIKE ?"
            params.append(f"%{q.lower()}%")

        sql += " ORDER BY t.order_idx, st.order_idx, p.order_idx"
        probs = rows2list(db.execute(sql, params).fetchall())

        # group by topic → subtopic (nested)
        nested: dict[str, dict[str, list]] = {}
        for p in probs:
            t  = (p["topic_name"]    or "General").strip()
            st = (p["subtopic_name"] or "").strip()
            # flatten subtopic when it's the same as the topic or generic
            if st in (t, "All", "General", ""):
                st = ""
            nested.setdefault(t, {}).setdefault(st, []).append(p)

        srcs = _load_nav_sources(db)

    return templates.TemplateResponse("problems.html", {
        "request": request, "src": src, "nested": nested,
        "topics": topics, "active_topic": topic, "active_diff": diff,
        "q": q or "", "srcs": srcs, "nav": NAV,
        "total": len(probs),
    })


@app.get("/article/{article_id}", response_class=HTMLResponse)
def article(request: Request, article_id: str, back: Optional[str] = None):
    with get_db() as db:
        art = row2dict(db.execute("SELECT * FROM articles WHERE id=?", (article_id,)).fetchone())
        if not art:
            raise HTTPException(404, "Article not found")

        imgs = rows2list(
            db.execute(
                "SELECT * FROM article_images WHERE article_id=? ORDER BY order_idx",
                (article_id,)
            ).fetchall()
        )

        # replace original src with our /img/ route
        html = art.get("html_content") or ""
        for img in imgs:
            old = img.get("src_url", "")
            new = f"/img/{article_id}/{img['order_idx']}"
            if old:
                html = html.replace(f'src="{old}"', f'src="{new}"')
                html = html.replace(f"src='{old}'", f'src="{new}"')
        art["rendered_html"] = html

        # find back-link (which problem links here?)
        prob = row2dict(db.execute(
            "SELECT p.*, s.name as source_name, p.source_id FROM problems p "
            "JOIN sources s ON p.source_id=s.id WHERE p.article_id=?",
            (article_id,)
        ).fetchone())

        srcs = _load_nav_sources(db)

    return templates.TemplateResponse("article.html", {
        "request": request, "art": art, "prob": prob,
        "back": back, "srcs": srcs, "nav": NAV,
    })


@app.get("/img/{article_id}/{order_idx}")
def serve_image(article_id: str, order_idx: int):
    with get_db() as db:
        row = db.execute(
            "SELECT content, src_url FROM article_images WHERE article_id=? AND order_idx=?",
            (article_id, order_idx)
        ).fetchone()
    if not row or not row["content"]:
        raise HTTPException(404, "Image not found")
    # detect content type from magic bytes (reliable, extension-free URLs)
    data = row["content"]
    if data[:8] == b'\x89PNG\r\n\x1a\n':
        ctype = "image/png"
    elif data[:2] == b'\xff\xd8':
        ctype = "image/jpeg"
    elif data[:4] == b'RIFF' and data[8:12] == b'WEBP':
        ctype = "image/webp"
    elif data[:3] == b'GIF':
        ctype = "image/gif"
    elif b'<svg' in data[:64]:
        ctype = "image/svg+xml"
    else:
        # fall back to URL extension
        url = (row["src_url"] or "").lower()
        if url.endswith(".png"):   ctype = "image/png"
        elif url.endswith(".gif"): ctype = "image/gif"
        elif url.endswith(".webp"):ctype = "image/webp"
        elif url.endswith(".svg"): ctype = "image/svg+xml"
        else:                      ctype = "image/jpeg"
    return Response(content=data, media_type=ctype,
                    headers={"Cache-Control": "max-age=86400"})


# ── Interview experience routes ────────────────────────────────────────────────

@app.get("/interviews", response_class=HTMLResponse)
def interviews(request: Request, company: Optional[str] = None, q: Optional[str] = None):
    with get_db() as db:
        sql = """
            SELECT p.id, p.name, p.difficulty, p.article_id, p.extra_json,
                   t.name AS company_name
            FROM problems p
            LEFT JOIN subtopics st ON p.subtopic_id = st.id
            LEFT JOIN topics t ON st.topic_id = t.id
            WHERE p.source_id = 'interview'
        """
        params: list = []
        if company:
            sql += " AND t.name = ?"
            params.append(company)
        if q:
            sql += " AND LOWER(p.name) LIKE ?"
            params.append(f"%{q.lower()}%")
        sql += " ORDER BY p.order_idx"
        rows = rows2list(db.execute(sql, params).fetchall())

        exps = []
        for r in rows:
            extra = {}
            try:
                extra = json.loads(r["extra_json"] or "{}")
            except Exception:
                pass
            exps.append({**r, "extra": extra})

        companies_q = rows2list(db.execute(
            "SELECT t.name, COUNT(*) as cnt "
            "FROM problems p JOIN subtopics st ON p.subtopic_id=st.id "
            "JOIN topics t ON st.topic_id=t.id "
            "WHERE p.source_id='interview' "
            "GROUP BY t.name ORDER BY cnt DESC"
        ).fetchall())

        srcs = _load_nav_sources(db)

    return templates.TemplateResponse("interview.html", {
        "request":   request,
        "exps":      exps,
        "companies": companies_q,
        "active_co": company,
        "q":         q or "",
        "total":     len(exps),
        "srcs":      srcs,
        "nav":       NAV,
    })


@app.get("/interview/{exp_id}", response_class=HTMLResponse)
def interview_exp(request: Request, exp_id: str):
    with get_db() as db:
        prob = row2dict(db.execute(
            "SELECT p.*, t.name AS company_name "
            "FROM problems p "
            "LEFT JOIN subtopics st ON p.subtopic_id=st.id "
            "LEFT JOIN topics t ON st.topic_id=t.id "
            "WHERE p.id=? AND p.source_id='interview'",
            (exp_id,)
        ).fetchone())
        if not prob:
            raise HTTPException(404, "Experience not found")

        extra = {}
        try:
            extra = json.loads(prob.get("extra_json") or "{}")
        except Exception:
            pass

        art_html = None
        if prob.get("article_id"):
            art = row2dict(db.execute(
                "SELECT html_content FROM articles WHERE id=?",
                (prob["article_id"],)
            ).fetchone())
            if art and art.get("html_content"):
                html = art["html_content"]
                imgs = rows2list(db.execute(
                    "SELECT * FROM article_images WHERE article_id=? ORDER BY order_idx",
                    (prob["article_id"],)
                ).fetchall())
                for img in imgs:
                    old = img.get("src_url", "")
                    new = f"/img/{prob['article_id']}/{img['order_idx']}"
                    if old:
                        html = html.replace(f'src="{old}"', f'src="{new}"')
                        html = html.replace(f"src='{old}'", f'src="{new}"')
                art_html = html

        srcs = _load_nav_sources(db)

    return templates.TemplateResponse("interview_exp.html", {
        "request":  request,
        "prob":     prob,
        "extra":    extra,
        "art_html": art_html,
        "srcs":     srcs,
        "nav":      NAV,
    })


@app.get("/blogs", response_class=HTMLResponse)
def blogs_home(request: Request):
    with get_db() as db:
        cats = rows2list(
            db.execute(
                "SELECT category, COUNT(*) as cnt FROM blogs GROUP BY category ORDER BY cnt DESC"
            ).fetchall()
        )
        recent = rows2list(
            db.execute(
                "SELECT id, title, category, url, excerpt FROM blogs "
                "WHERE title != '' ORDER BY rowid DESC LIMIT 12"
            ).fetchall()
        )
        srcs = _load_nav_sources(db)
    return templates.TemplateResponse("blogs.html", {
        "request": request, "cats": cats, "recent": recent,
        "srcs": srcs, "nav": NAV,
        "total": sum(c["cnt"] for c in cats),
    })


@app.get("/blogs/{category}", response_class=HTMLResponse)
def blog_category(request: Request, category: str,
                  page: int = 1, q: Optional[str] = None):
    PAGE_SIZE = 20
    with get_db() as db:
        sql = "SELECT id, title, url, excerpt, scrape_ok FROM blogs WHERE category=?"
        params: list = [category]
        if q:
            sql += " AND LOWER(title) LIKE ?"
            params.append(f"%{q.lower()}%")
        sql += " ORDER BY rowid DESC"

        total = db.execute(
            f"SELECT COUNT(*) FROM ({sql})", params
        ).fetchone()[0]
        offset = (page - 1) * PAGE_SIZE
        posts = rows2list(
            db.execute(sql + f" LIMIT {PAGE_SIZE} OFFSET {offset}", params).fetchall()
        )
        total_pages = max(1, (total + PAGE_SIZE - 1) // PAGE_SIZE)
        srcs = _load_nav_sources(db)

    return templates.TemplateResponse("blog_category.html", {
        "request": request, "category": category,
        "posts": posts, "page": page, "total_pages": total_pages,
        "total": total, "q": q or "",
        "srcs": srcs, "nav": NAV,
    })


@app.get("/blog/{blog_id}", response_class=HTMLResponse)
def blog_post(request: Request, blog_id: str):
    with get_db() as db:
        post = row2dict(db.execute("SELECT * FROM blogs WHERE id=?", (blog_id,)).fetchone())
        if not post:
            raise HTTPException(404, "Blog post not found")

        # render html with local images (via article_id if linked)
        html = post.get("html_content") or ""
        if post.get("article_id"):
            imgs = rows2list(
                db.execute(
                    "SELECT * FROM article_images WHERE article_id=? ORDER BY order_idx",
                    (post["article_id"],)
                ).fetchall()
            )
            for img in imgs:
                old = img.get("src_url", "")
                new = f"/img/{post['article_id']}/{img['order_idx']}"
                if old:
                    html = html.replace(f'src="{old}"', f'src="{new}"')
                    html = html.replace(f"src='{old}'", f'src="{new}"')
        post["rendered_html"] = html

        srcs = _load_nav_sources(db)

    return templates.TemplateResponse("blog_post.html", {
        "request": request, "post": post,
        "srcs": srcs, "nav": NAV,
    })


@app.post("/api/feature-request")
async def feature_request(feature: str = Form(...)):
    """Forward a feature request to the owner's email."""
    if not SMTP_USER or not SMTP_PASS:
        return JSONResponse({"ok": False, "error": "Email not configured on this server. Please use the GitHub Issue tab instead."})
    try:
        msg = MIMEText(f"Feature Request:\n\n{feature}", "plain", "utf-8")
        msg["Subject"] = "TUF Archive \u2013 Feature Request"
        msg["From"]    = SMTP_USER
        msg["To"]      = FEATURE_MAIL
        with smtplib.SMTP_SSL("smtp.gmail.com", 465) as s:
            s.login(SMTP_USER, SMTP_PASS)
            s.sendmail(SMTP_USER, FEATURE_MAIL, msg.as_string())
        return JSONResponse({"ok": True})
    except Exception as e:
        return JSONResponse({"ok": False, "error": str(e)})


@app.get("/search", response_class=HTMLResponse)
def search(request: Request, q: str = Query(default="")):
    results = {"problems": [], "articles": [], "blogs": []}
    if q and len(q.strip()) >= 2:
        pat = f"%{q.lower()}%"
        with get_db() as db:
            results["problems"] = rows2list(db.execute("""
                SELECT p.id, p.name, p.difficulty, p.source_id, p.article_id,
                       s.name as source_name
                FROM problems p
                JOIN sources s ON p.source_id = s.id
                WHERE LOWER(p.name) LIKE ?
                ORDER BY p.name LIMIT 30
            """, (pat,)).fetchall())

            results["articles"] = rows2list(db.execute("""
                SELECT id, title, url
                FROM articles
                WHERE LOWER(title) LIKE ? AND html_content IS NOT NULL
                ORDER BY title LIMIT 20
            """, (pat,)).fetchall())

            results["blogs"] = rows2list(db.execute("""
                SELECT id, title, category, url, excerpt
                FROM blogs
                WHERE LOWER(title) LIKE ? OR LOWER(excerpt) LIKE ?
                ORDER BY title LIMIT 20
            """, (pat, pat)).fetchall())

            srcs = _load_nav_sources(db)
    else:
        with get_db() as db:
            srcs = _load_nav_sources(db)

    total = sum(len(v) for v in results.values())
    return templates.TemplateResponse("search.html", {
        "request": request, "q": q,
        "results": results, "total": total,
        "srcs": srcs, "nav": NAV,
    })


# ── dev server ─────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    uvicorn.run("app:app", host="0.0.0.0", port=8000, reload=True,
                reload_dirs=[BASE_DIR])

# TUF Archive

A scraper + local web viewer for all **free** content on [takeuforward.org](https://takeuforward.org).

| Section | What's scraped |
|---|---|
| DSA Sheets (A2Z, SDE, Blind75, Striver79) | Problem names, article URLs, YouTube links, LeetCode links, difficulty |
| Core CS – Computer Networks | Topic / question names |
| Core CS – DBMS | Names, article URLs, YouTube links |
| Core CS – Operating Systems | Names, article URLs |
| System Design | Names, article URLs, YouTube links |
| DSA Playlist | Names, article URLs, LeetCode links |
| Striver's CP Sheet | Problem names + Codeforces links |
| Interview Experiences | Company, position, rounds, tags, description |
| Blogs | Full article text + images |
| All articles | Full HTML, plain text, embedded images |

---

# Preview

### Home Page

![Home Page](assets/preview/a2z-sheet.png)

### A2Z sheet

![A2Z sheet](assets/preview/a2z-sheet.png)

### Interview Experiences

![Interview Experiences](assets/preview/interview-experiences.png)

### Blogs

![Blogs](assets/preview/blogs.png)

### Search

![Search](assets/preview/search.png)

## How It Works

TUF is a **Next.js App Router** application. Page data is embedded in
`self.__next_f.push([1, "..."])` script tags (no `__NEXT_DATA__` on the new site).

1. **Playwright** (headless Chromium) loads each listing page
2. Raw HTML is parsed with regex to extract `__next_f` flight payloads
3. Payloads are decoded and walked recursively to collect problem objects
4. Article URLs are queued in a `scrape_queue` DB table
5. A second pass fetches each article, extracts `div.article` content and downloads embedded images
6. Interview experiences are fetched via the TUF backend API (paginated, no auth required)

---

## Setup

```bash
git clone https://github.com/Ashok-19/TUF-Archive.git
pip install -r requirements.txt
playwright install chromium
```

---

## Usage

### Run the full scraper
```bash
python main.py                               # Takes longer time
```

### Run a specific phase
```bash
python main.py --phase sheets                # DSA sheets
python main.py --phase cs                    # Core CS
python main.py --phase sd                    # System Design
python main.py --phase playlist              # DSA Playlist
python main.py --phase blogs                 # Blogs
python main.py --phase cp                    # Striver's CP Sheet
python main.py --phase interview             # Interview experiences
python main.py --phase articles              # Fetch all queued article content
python main.py --phase articles --limit 50   # Process at most 50 articles (resumable)
```

### Launch the web viewer
```bash
python webapp/app.py
```
Open **http://localhost:8000** in your browser.

The viewer includes:
- Problem list for every sheet / playlist with difficulty filters and topic grouping
- Inline article reader with all scraped images served locally
- CP Sheet browser (Codeforces links)
- Interview experience browser with company filter
- Blog listing by category with search
- Global search across all problems, articles, and blogs

---

## Project Structure

```
main.py                              Entry point / orchestrator
requirements.txt
tuf_data.db                          SQLite database 

assets/
  images/                            Image assets of articles
  preview/                           README Preview images

tuf_scraper/
  db/
    database.py                      SQLite schema + async CRUD (aiosqlite)
  scrapers/
    base.py                          Browser factory, flight-data parser, article extractor
    dsa_sheets.py                    A2Z / SDE / Blind75 / Striver79
    core_cs.py                       CN / DBMS / OS
    system_design.py                 System Design roadmap
    dsa_playlist.py                  DSA Playlist
    cp_sheet.py                      Striver's CP Sheet (Codeforces)
    interview.py                     Interview experiences (TUF backend API)
    blogs.py                         Blog listing + full article scrape

webapp/
  app.py                             FastAPI web viewer
  templates/
    base.html                        Layout: dark sidebar + sticky header
    home.html                        Dashboard with stats + source cards
    problems.html                    Problem list (difficulty / topic filters, search)
    article.html                     Article reader with local image serving
    blogs.html                       Blog home (category grid + recent posts)
    blog_category.html               Posts in a single category
    blog_post.html                   Full blog article reader
    interview.html                   Interview experience listing with company filter
    interview_exp.html               Individual interview experience detail
    search.html                      Global search across problems, articles, blogs

assets/images/                       Downloaded article images (git-ignored, auto-generated)
```

---

## Database Schema

```
sources              top-level origin (sheet / cs subject / blog / cp / interview)
topics               categories within a source
subtopics            sub-sections within a topic
problems             individual problems / questions / interview experiences
articles             full scraped article HTML + plain text
article_images       image BLOBs + local file paths
blogs                blog post metadata + full content
scrape_queue         resumable URL queue (pending / processing / done / failed)
```

---

## Notes

- Polite ~1.5 s delay between requests
- `scrape_queue` enables **resumable article scraping** — re-run `--phase articles` after a failure and it resumes where it left off (max 3 attempts per URL)
- Images are stored **both** as BLOBs in the DB and as files under `assets/images/<article_id>/`
- Plus-only (`/plus/`) links are automatically excluded
- `tuf_data.db` is tracked via Git LFS

---

## Troubleshooting

**Empty results** — increase the `wait_until` timeout in `base.py` or run without headless mode (`headless=False`)

**Rate limiting** — increase `polite_delay` in `base.py` (default 1.5 s) or add random jitter

**Content locked behind TUF+** — the scraper automatically skips URLs containing `/plus/`


## Support 

 ***𓃠 Kindly Star the Repo if you like this work***

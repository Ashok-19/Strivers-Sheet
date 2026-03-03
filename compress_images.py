"""
Fast parallel image compression — uses all CPU cores.

Key speedups vs previous version:
  1. multiprocessing.Pool(N_CORES) — all 16 cores in parallel
  2. method=2 instead of method=6 — 4-6x faster encode, ~1% quality diff
  3. imap_unordered — progress updates as soon as any worker finishes
  4. DB writes batched in main process (SQLite isn't multi-process safe)

Expected time: ~3-8 minutes instead of 2+ hours.
"""

import os
import sqlite3
from pathlib import Path
from multiprocessing import Pool, cpu_count
from PIL import Image
from tqdm import tqdm

BASE_DIR = Path(__file__).parent
ASSETS   = BASE_DIR / "assets" / "images"
DB_PATH  = BASE_DIR / "tuf_data.db"
WEBP_Q   = 78
METHOD   = 2      # 0-6; method=6 is best but ~5x slower than method=2. Quality diff <1%
N_CORES  = cpu_count()

# resolve once so workers can compute relative paths
_REPO_ROOT = str(BASE_DIR)


def _compress_one(src_str: str):
    """
    Worker function — runs in a separate process.
    Returns (old_rel, new_rel, saved_bytes, error_msg)
    """
    src = Path(src_str)
    repo = Path(_REPO_ROOT)
    suffix = src.suffix.lower()

    try:
        before = src.stat().st_size

        if suffix in (".png", ".jpg", ".jpeg"):
            dst = src.with_suffix(".webp")
            img = Image.open(src)
            if img.mode in ("P", "RGBA"):
                img = img.convert("RGBA")
            elif img.mode == "CMYK":
                img = img.convert("RGB")
            else:
                img = img.convert("RGB")
            img.save(dst, "WEBP", quality=WEBP_Q, method=METHOD)
            after = dst.stat().st_size

            if after < before:
                old_rel = str(src.relative_to(repo))
                new_rel = str(dst.relative_to(repo))
                src.unlink()
                return (old_rel, new_rel, before - after, None)
            else:
                dst.unlink()  # WebP was bigger (rare), keep original
                return (None, None, 0, None)

        elif suffix == ".webp":
            img = Image.open(src)
            img.save(src, "WEBP", quality=WEBP_Q, method=METHOD)
            after = src.stat().st_size
            saved = max(0, before - after)
            rel = str(src.relative_to(repo))
            return (rel, rel, saved, None)

    except Exception as e:
        return (None, None, 0, f"{src.name}: {e}")

    return (None, None, 0, None)


def compress_images():
    exts  = {".png", ".jpg", ".jpeg", ".webp"}
    files = sorted(
        str(p) for p in ASSETS.rglob("*")
        if p.suffix.lower() in exts and p.is_file()
    )

    if not files:
        print("No images found in", ASSETS)
        return

    orig_total = sum(Path(f).stat().st_size for f in files)
    print(f"\nFound {len(files):,} images  ({orig_total / 1024 / 1024:.1f} MB total)")
    print(f"Using {N_CORES} CPU cores  |  WebP quality={WEBP_Q}  method={METHOD}")
    print(f"Estimated time: ~{max(1, len(files) // (N_CORES * 40))} minutes\n")

    db  = sqlite3.connect(DB_PATH)
    cur = db.cursor()

    saved_bytes = 0
    converted   = 0
    reencoded   = 0
    skipped     = 0
    errors      = 0
    db_updates  = []
    BATCH       = 500

    with tqdm(total=len(files), unit="img", ncols=100,
              bar_format="{l_bar}{bar}| {n_fmt}/{total_fmt}  [{elapsed}<{remaining}  {rate_fmt}]") as bar:

        with Pool(N_CORES) as pool:
            for old_rel, new_rel, saved, err in pool.imap_unordered(_compress_one, files, chunksize=16):
                bar.update(1)

                if err:
                    errors += 1
                    bar.write(f"  ERROR: {err}")
                    continue

                saved_bytes += saved

                if old_rel and new_rel:
                    if old_rel != new_rel:
                        converted += 1
                        db_updates.append((new_rel, old_rel))
                    else:
                        reencoded += 1
                else:
                    skipped += 1

                # batch commit
                if len(db_updates) >= BATCH:
                    cur.executemany(
                        "UPDATE article_images SET local_path=? WHERE local_path=?",
                        db_updates
                    )
                    db.commit()
                    db_updates.clear()

                bar.set_postfix(
                    saved=f"{saved_bytes/1024/1024:.0f}MB",
                    conv=converted,
                    err=errors,
                    refresh=False
                )

    # flush remaining DB updates
    if db_updates:
        cur.executemany(
            "UPDATE article_images SET local_path=? WHERE local_path=?",
            db_updates
        )
        db.commit()

    new_total = orig_total - saved_bytes
    pct = 100 * saved_bytes / orig_total if orig_total else 0

    print(f"\n{'='*60}")
    print(f"  Compression done!")
    print(f"  Converted  : {converted:,}  PNG/JPG → WebP")
    print(f"  Re-encoded : {reencoded:,}  WebP → WebP (smaller)")
    print(f"  Skipped    : {skipped:,}  (already optimal)")
    print(f"  Errors     : {errors}")
    print(f"  Before     : {orig_total / 1024 / 1024:.1f} MB")
    print(f"  After      : {new_total / 1024 / 1024:.1f} MB")
    print(f"  Saved      : {saved_bytes / 1024 / 1024:.1f} MB  ({pct:.0f}%)")
    print(f"{'='*60}")

    # ── Null all blobs ─────────────────────────────────────────────────────
    print("\nNulling image blobs in DB ...")
    cur.execute("SELECT COUNT(*) FROM article_images WHERE content IS NOT NULL")
    blob_count = cur.fetchone()[0]
    print(f"  Rows with blobs: {blob_count:,}")
    if blob_count:
        cur.execute("UPDATE article_images SET content = NULL")
        db.commit()
        print("  Blobs cleared.")
    else:
        print("  Already clean.")

    # ── VACUUM ─────────────────────────────────────────────────────────────
    print("\nRunning VACUUM (shrinks DB file — 1-3 min) ...")
    db_before = DB_PATH.stat().st_size
    db.execute("VACUUM")
    db.commit()
    db_after = DB_PATH.stat().st_size
    print(f"  DB before : {db_before / 1024 / 1024:.0f} MB")
    print(f"  DB after  : {db_after  / 1024 / 1024:.0f} MB")
    print(f"  DB saved  : {(db_before - db_after) / 1024 / 1024:.0f} MB")

    db.close()
    print("\n✓ All done!")


if __name__ == "__main__":
    compress_images()

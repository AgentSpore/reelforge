from __future__ import annotations
import asyncio
import json
import random
from datetime import datetime, timezone

import aiosqlite

SQL = """
CREATE TABLE IF NOT EXISTS reel_jobs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    title TEXT NOT NULL,
    photo_urls TEXT NOT NULL,
    style TEXT NOT NULL DEFAULT 'dynamic',
    aspect_ratio TEXT NOT NULL DEFAULT '9:16',
    caption TEXT,
    music_genre TEXT,
    brand_color TEXT,
    cta_text TEXT,
    duration_target INTEGER NOT NULL DEFAULT 15,
    brand_id INTEGER,
    status TEXT NOT NULL DEFAULT 'queued',
    output_url TEXT,
    duration_seconds REAL,
    render_log TEXT,
    created_at TEXT NOT NULL,
    completed_at TEXT
);

CREATE TABLE IF NOT EXISTS brands (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT NOT NULL,
    brand_color TEXT,
    logo_url TEXT,
    default_cta TEXT,
    default_music_genre TEXT,
    default_style TEXT NOT NULL DEFAULT 'dynamic',
    created_at TEXT NOT NULL
);
"""

MIGRATION_BRAND_ID = "ALTER TABLE reel_jobs ADD COLUMN brand_id INTEGER;"

STYLE_CATALOG = {
    "dynamic":   {"description": "Fast cuts, energetic transitions", "best_for": "Sports, tech gadgets, fashion",
                  "transitions": ["zoom_in", "slide_left", "whip_pan", "flash"]},
    "minimal":   {"description": "Clean fades, lots of white space", "best_for": "Skincare, luxury goods, food",
                  "transitions": ["fade", "dissolve", "cross_fade"]},
    "luxury":    {"description": "Slow pans, gold accents, elegant text", "best_for": "Jewellery, watches, premium brands",
                  "transitions": ["slow_zoom", "reveal", "iris_in"]},
    "playful":   {"description": "Bouncy animations, bright colours", "best_for": "Toys, lifestyle, gifts",
                  "transitions": ["bounce", "spin", "pop", "shake"]},
    "cinematic": {"description": "Letterbox bars, dramatic zoom, film grain", "best_for": "Perfumes, cars, fashion",
                  "transitions": ["letterbox_reveal", "slow_push", "ken_burns"]},
}


async def init_db(path: str) -> aiosqlite.Connection:
    db = await aiosqlite.connect(path)
    db.row_factory = aiosqlite.Row
    await db.executescript(SQL)
    try:
        await db.execute("SELECT brand_id FROM reel_jobs LIMIT 1")
    except Exception:
        try:
            await db.execute(MIGRATION_BRAND_ID)
        except Exception:
            pass
    await db.commit()
    return db


def _job_row(r: aiosqlite.Row) -> dict:
    render_log = None
    if r["render_log"]:
        try:
            render_log = json.loads(r["render_log"])
        except Exception:
            pass
    return {
        "id": r["id"], "title": r["title"],
        "photo_urls": json.loads(r["photo_urls"]),
        "style": r["style"], "aspect_ratio": r["aspect_ratio"],
        "status": r["status"], "brand_id": r["brand_id"],
        "output_url": r["output_url"],
        "duration_seconds": r["duration_seconds"],
        "render_log": render_log,
        "created_at": r["created_at"], "completed_at": r["completed_at"],
    }


def _brand_row(r: aiosqlite.Row, reels_count: int = 0) -> dict:
    return {
        "id": r["id"], "name": r["name"],
        "brand_color": r["brand_color"], "logo_url": r["logo_url"],
        "default_cta": r["default_cta"], "default_music_genre": r["default_music_genre"],
        "default_style": r["default_style"], "reels_count": reels_count,
        "created_at": r["created_at"],
    }


async def get_brand(db: aiosqlite.Connection, brand_id: int) -> dict | None:
    rows = await db.execute_fetchall("SELECT * FROM brands WHERE id = ?", (brand_id,))
    if not rows:
        return None
    cnt = await db.execute_fetchall(
        "SELECT COUNT(*) as c FROM reel_jobs WHERE brand_id = ?", (brand_id,))
    return _brand_row(rows[0], cnt[0]["c"] if cnt else 0)


async def create_brand(db: aiosqlite.Connection, data: dict) -> dict:
    now = datetime.now(timezone.utc).isoformat()
    cur = await db.execute(
        """INSERT INTO brands (name, brand_color, logo_url, default_cta, default_music_genre, default_style, created_at)
           VALUES (?, ?, ?, ?, ?, ?, ?)""",
        (data["name"], data.get("brand_color"), data.get("logo_url"),
         data.get("default_cta"), data.get("default_music_genre"),
         data.get("default_style", "dynamic"), now)
    )
    await db.commit()
    return await get_brand(db, cur.lastrowid)


async def list_brands(db: aiosqlite.Connection) -> list[dict]:
    rows = await db.execute_fetchall("SELECT * FROM brands ORDER BY name ASC")
    result = []
    for r in rows:
        cnt = await db.execute_fetchall(
            "SELECT COUNT(*) as c FROM reel_jobs WHERE brand_id = ?", (r["id"],))
        result.append(_brand_row(r, cnt[0]["c"] if cnt else 0))
    return result


async def update_brand(db: aiosqlite.Connection, brand_id: int, updates: dict) -> dict | None:
    fields = {k: v for k, v in updates.items() if v is not None}
    if not fields:
        return await get_brand(db, brand_id)
    set_clause = ", ".join(f"{k} = ?" for k in fields)
    values = list(fields.values()) + [brand_id]
    cur = await db.execute(f"UPDATE brands SET {set_clause} WHERE id = ?", values)
    await db.commit()
    if cur.rowcount == 0:
        return None
    return await get_brand(db, brand_id)


async def delete_brand(db: aiosqlite.Connection, brand_id: int) -> bool:
    rows = await db.execute_fetchall("SELECT id FROM brands WHERE id = ?", (brand_id,))
    if not rows:
        return False
    await db.execute("UPDATE reel_jobs SET brand_id = NULL WHERE brand_id = ?", (brand_id,))
    await db.execute("DELETE FROM brands WHERE id = ?", (brand_id,))
    await db.commit()
    return True


async def apply_brand_defaults(db: aiosqlite.Connection, data: dict) -> dict:
    """Apply brand profile defaults to reel creation data. Explicit values override brand defaults."""
    brand_id = data.get("brand_id")
    if not brand_id:
        return data
    brand = await get_brand(db, brand_id)
    if not brand:
        return data
    # Only fill if user didn't provide explicit value
    if not data.get("brand_color") and brand["brand_color"]:
        data["brand_color"] = brand["brand_color"]
    if not data.get("cta_text") and brand["default_cta"]:
        data["cta_text"] = brand["default_cta"]
    if not data.get("music_genre") and brand["default_music_genre"]:
        data["music_genre"] = brand["default_music_genre"]
    if data.get("style") == "dynamic" and brand["default_style"] != "dynamic":
        data["style"] = brand["default_style"]
    return data


async def create_job(db: aiosqlite.Connection, data: dict) -> dict:
    data = await apply_brand_defaults(db, data)
    now = datetime.now(timezone.utc).isoformat()
    cur = await db.execute(
        """INSERT INTO reel_jobs
           (title, photo_urls, style, aspect_ratio, caption, music_genre, brand_color,
            cta_text, duration_target, brand_id, status, created_at)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'queued', ?)""",
        (data["title"], json.dumps(data["photo_urls"]), data.get("style", "dynamic"),
         data.get("aspect_ratio", "9:16"), data.get("caption"), data.get("music_genre"),
         data.get("brand_color"), data.get("cta_text"), data.get("duration_target", 15),
         data.get("brand_id"), now)
    )
    await db.commit()
    rows = await db.execute_fetchall("SELECT * FROM reel_jobs WHERE id = ?", (cur.lastrowid,))
    return _job_row(rows[0])


async def list_jobs(db: aiosqlite.Connection, status: str | None = None, limit: int = 50) -> list[dict]:
    q = "SELECT * FROM reel_jobs"
    params = []
    if status:
        q += " WHERE status = ?"
        params.append(status)
    q += " ORDER BY created_at DESC LIMIT ?"
    params.append(limit)
    rows = await db.execute_fetchall(q, params)
    return [{
        "id": r["id"], "title": r["title"], "style": r["style"],
        "aspect_ratio": r["aspect_ratio"], "status": r["status"],
        "photo_count": len(json.loads(r["photo_urls"])),
        "created_at": r["created_at"],
    } for r in rows]


async def get_job(db: aiosqlite.Connection, job_id: int) -> dict | None:
    rows = await db.execute_fetchall("SELECT * FROM reel_jobs WHERE id = ?", (job_id,))
    return _job_row(rows[0]) if rows else None


async def get_render_log(db: aiosqlite.Connection, job_id: int) -> dict | None:
    rows = await db.execute_fetchall(
        "SELECT id, status, render_log, output_url, duration_seconds, completed_at FROM reel_jobs WHERE id = ?",
        (job_id,),
    )
    if not rows:
        return None
    r = rows[0]
    render_log = None
    if r["render_log"]:
        try:
            render_log = json.loads(r["render_log"])
        except Exception:
            pass
    return {
        "job_id": r["id"], "status": r["status"],
        "render_log": render_log, "output_url": r["output_url"],
        "duration_seconds": r["duration_seconds"], "completed_at": r["completed_at"],
    }


async def process_job(db: aiosqlite.Connection, job_id: int):
    await db.execute("UPDATE reel_jobs SET status = 'processing' WHERE id = ?", (job_id,))
    await db.commit()
    rows = await db.execute_fetchall("SELECT * FROM reel_jobs WHERE id = ?", (job_id,))
    if not rows:
        return
    job = rows[0]
    photos = json.loads(job["photo_urls"])
    style_cfg = STYLE_CATALOG.get(job["style"], STYLE_CATALOG["dynamic"])
    await asyncio.sleep(0.5 + len(photos) * 0.2)
    log = {
        "photos_processed": len(photos),
        "style": job["style"],
        "transitions_applied": style_cfg["transitions"][:len(photos) - 1] if len(photos) > 1 else [],
        "audio_track": job["music_genre"] or "none",
        "cta": job["cta_text"] or "",
        "brand_color": job["brand_color"] or "#000000",
        "resolution": "1080x1920" if job["aspect_ratio"] == "9:16" else "1080x1080",
    }
    output_url = f"https://cdn.reelforge.io/renders/{job_id}/output.mp4"
    duration = min(job["duration_target"], len(photos) * 3.5)
    now = datetime.now(timezone.utc).isoformat()
    await db.execute(
        """UPDATE reel_jobs SET status = 'completed', output_url = ?,
           duration_seconds = ?, render_log = ?, completed_at = ? WHERE id = ?""",
        (output_url, duration, json.dumps(log), now, job_id)
    )
    await db.commit()


async def delete_job(db: aiosqlite.Connection, job_id: int) -> bool:
    cur = await db.execute("DELETE FROM reel_jobs WHERE id = ?", (job_id,))
    await db.commit()
    return cur.rowcount > 0


async def get_stats(db: aiosqlite.Connection) -> dict:
    rows = await db.execute_fetchall("SELECT * FROM reel_jobs")
    brand_cnt = await db.execute_fetchall("SELECT COUNT(*) as c FROM brands")
    total_brands = brand_cnt[0]["c"] if brand_cnt else 0
    if not rows:
        return {"total_jobs": 0, "completed": 0, "processing": 0, "failed": 0,
                "avg_duration_seconds": 0.0, "most_used_style": None, "most_used_ratio": None,
                "total_brands": total_brands}
    total = len(rows)
    completed = sum(1 for r in rows if r["status"] == "completed")
    processing = sum(1 for r in rows if r["status"] == "processing")
    failed = sum(1 for r in rows if r["status"] == "failed")
    durations = [r["duration_seconds"] for r in rows if r["duration_seconds"]]
    avg_dur = round(sum(durations) / len(durations), 1) if durations else 0.0
    style_counts: dict = {}
    ratio_counts: dict = {}
    for r in rows:
        style_counts[r["style"]] = style_counts.get(r["style"], 0) + 1
        ratio_counts[r["aspect_ratio"]] = ratio_counts.get(r["aspect_ratio"], 0) + 1
    return {
        "total_jobs": total, "completed": completed,
        "processing": processing, "failed": failed,
        "avg_duration_seconds": avg_dur,
        "most_used_style": max(style_counts, key=style_counts.get) if style_counts else None,
        "most_used_ratio": max(ratio_counts, key=ratio_counts.get) if ratio_counts else None,
        "total_brands": total_brands,
    }


async def duplicate_job(db: aiosqlite.Connection, job_id: int, overrides: dict) -> dict | None:
    rows = await db.execute_fetchall("SELECT * FROM reel_jobs WHERE id = ?", (job_id,))
    if not rows:
        return None
    src = rows[0]
    now = datetime.now(timezone.utc).isoformat()
    cur = await db.execute(
        """INSERT INTO reel_jobs
           (title, photo_urls, style, aspect_ratio, caption, music_genre, brand_color,
            cta_text, duration_target, brand_id, status, created_at)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'queued', ?)""",
        (
            overrides.get("title") or f"{src['title']} (copy)",
            src["photo_urls"],
            overrides.get("style") or src["style"],
            overrides.get("aspect_ratio") or src["aspect_ratio"],
            overrides.get("caption") if "caption" in overrides else src["caption"],
            overrides.get("music_genre") if "music_genre" in overrides else src["music_genre"],
            overrides.get("brand_color") if "brand_color" in overrides else src["brand_color"],
            overrides.get("cta_text") if "cta_text" in overrides else src["cta_text"],
            overrides.get("duration_target") or src["duration_target"],
            src["brand_id"],
            now,
        )
    )
    await db.commit()
    new_rows = await db.execute_fetchall("SELECT * FROM reel_jobs WHERE id = ?", (cur.lastrowid,))
    return _job_row(new_rows[0]) if new_rows else None


async def get_stats_by_style(db: aiosqlite.Connection) -> list[dict]:
    rows = await db.execute_fetchall("SELECT * FROM reel_jobs")
    buckets: dict[str, dict] = {}
    for r in rows:
        s = r["style"]
        if s not in buckets:
            buckets[s] = {"style": s, "total": 0, "completed": 0, "failed": 0, "durations": []}
        buckets[s]["total"] += 1
        if r["status"] == "completed":
            buckets[s]["completed"] += 1
        if r["status"] == "failed":
            buckets[s]["failed"] += 1
        if r["duration_seconds"]:
            buckets[s]["durations"].append(r["duration_seconds"])
    result = []
    for b in buckets.values():
        durs = b.pop("durations")
        b["avg_duration_seconds"] = round(sum(durs) / len(durs), 1) if durs else 0.0
        b["success_rate_pct"] = round(b["completed"] / b["total"] * 100, 1) if b["total"] else 0.0
        result.append(b)
    return sorted(result, key=lambda x: x["total"], reverse=True)

from __future__ import annotations
import asyncio
import json
import random
from datetime import datetime, timezone, timedelta

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

VALID_PRIORITIES = {"low", "normal", "high", "urgent"}
PRIORITY_ORDER = {"urgent": 0, "high": 1, "normal": 2, "low": 3}

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

REEL_PRESETS = {
    "product_showcase": {
        "description": "Classic product showcase — hero shot, features, CTA",
        "recommended_photos": 4,
        "recommended_style": "dynamic",
        "recommended_aspect": "9:16",
        "recommended_duration": 15,
        "recommended_music": "upbeat",
        "scene_flow": ["hero_shot", "feature_1", "feature_2", "cta"],
    },
    "unboxing": {
        "description": "Unboxing experience — packaging, reveal, product, reaction",
        "recommended_photos": 5,
        "recommended_style": "playful",
        "recommended_aspect": "9:16",
        "recommended_duration": 20,
        "recommended_music": "energetic",
        "scene_flow": ["package_exterior", "opening", "reveal", "product_detail", "reaction"],
    },
    "before_after": {
        "description": "Before/after transformation — great for beauty, cleaning, renovation",
        "recommended_photos": 4,
        "recommended_style": "minimal",
        "recommended_aspect": "9:16",
        "recommended_duration": 12,
        "recommended_music": "dramatic",
        "scene_flow": ["before_wide", "before_close", "after_close", "after_wide"],
    },
    "tutorial": {
        "description": "Step-by-step tutorial or how-to guide",
        "recommended_photos": 6,
        "recommended_style": "minimal",
        "recommended_aspect": "9:16",
        "recommended_duration": 30,
        "recommended_music": "calm",
        "scene_flow": ["intro", "step_1", "step_2", "step_3", "result", "cta"],
    },
    "testimonial": {
        "description": "Customer testimonial — quote, product, social proof",
        "recommended_photos": 3,
        "recommended_style": "luxury",
        "recommended_aspect": "1:1",
        "recommended_duration": 10,
        "recommended_music": "ambient",
        "scene_flow": ["quote", "product_in_use", "social_proof"],
    },
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
    await _migrate_engagement(db)
    await _migrate_priority(db)
    await _migrate_collections(db)
    await _migrate_ab_tests(db)
    await db.commit()
    return db


async def _migrate_engagement(db: aiosqlite.Connection):
    await db.execute("""
        CREATE TABLE IF NOT EXISTS engagement_events (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            reel_id INTEGER NOT NULL,
            event_type TEXT NOT NULL,
            source TEXT,
            created_at TEXT NOT NULL,
            FOREIGN KEY (reel_id) REFERENCES reel_jobs(id) ON DELETE CASCADE
        )
    """)


async def _migrate_priority(db: aiosqlite.Connection):
    try:
        await db.execute("SELECT priority FROM reel_jobs LIMIT 1")
    except Exception:
        try:
            await db.execute("ALTER TABLE reel_jobs ADD COLUMN priority TEXT NOT NULL DEFAULT 'normal'")
        except Exception:
            pass


async def _migrate_collections(db: aiosqlite.Connection):
    await db.executescript("""
        CREATE TABLE IF NOT EXISTS collections (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL,
            description TEXT,
            created_at TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS collection_reels (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            collection_id INTEGER NOT NULL REFERENCES collections(id) ON DELETE CASCADE,
            reel_id INTEGER NOT NULL REFERENCES reel_jobs(id) ON DELETE CASCADE,
            added_at TEXT NOT NULL,
            UNIQUE(collection_id, reel_id)
        );
        CREATE INDEX IF NOT EXISTS idx_cr_collection ON collection_reels(collection_id);
        CREATE INDEX IF NOT EXISTS idx_cr_reel ON collection_reels(reel_id);
    """)


async def _migrate_ab_tests(db: aiosqlite.Connection):
    await db.executescript("""
        CREATE TABLE IF NOT EXISTS ab_tests (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL,
            status TEXT NOT NULL DEFAULT 'active',
            winner_id INTEGER,
            created_at TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS ab_test_reels (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            test_id INTEGER NOT NULL REFERENCES ab_tests(id) ON DELETE CASCADE,
            reel_id INTEGER NOT NULL REFERENCES reel_jobs(id) ON DELETE CASCADE,
            UNIQUE(test_id, reel_id)
        );
        CREATE INDEX IF NOT EXISTS idx_abtr_test ON ab_test_reels(test_id);
    """)


def _job_row(r: aiosqlite.Row) -> dict:
    render_log = None
    if r["render_log"]:
        try:
            render_log = json.loads(r["render_log"])
        except Exception:
            pass
    priority = "normal"
    try:
        priority = r["priority"]
    except (IndexError, KeyError):
        pass
    return {
        "id": r["id"], "title": r["title"],
        "photo_urls": json.loads(r["photo_urls"]),
        "style": r["style"], "aspect_ratio": r["aspect_ratio"],
        "status": r["status"], "priority": priority or "normal",
        "brand_id": r["brand_id"],
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
    brand_id = data.get("brand_id")
    if not brand_id:
        return data
    brand = await get_brand(db, brand_id)
    if not brand:
        return data
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
    priority = data.get("priority", "normal")
    if priority not in VALID_PRIORITIES:
        priority = "normal"
    now = datetime.now(timezone.utc).isoformat()
    cur = await db.execute(
        """INSERT INTO reel_jobs
           (title, photo_urls, style, aspect_ratio, caption, music_genre, brand_color,
            cta_text, duration_target, brand_id, priority, status, created_at)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'queued', ?)""",
        (data["title"], json.dumps(data["photo_urls"]), data.get("style", "dynamic"),
         data.get("aspect_ratio", "9:16"), data.get("caption"), data.get("music_genre"),
         data.get("brand_color"), data.get("cta_text"), data.get("duration_target", 15),
         data.get("brand_id"), priority, now)
    )
    await db.commit()
    rows = await db.execute_fetchall("SELECT * FROM reel_jobs WHERE id = ?", (cur.lastrowid,))
    return _job_row(rows[0])


async def batch_create_jobs(db: aiosqlite.Connection, data: dict) -> list[dict]:
    styles = data["styles"]
    base = {k: v for k, v in data.items() if k != "styles"}
    jobs = []
    for style in styles:
        job_data = {**base, "style": style}
        job = await create_job(db, job_data)
        jobs.append(job)
    return jobs


async def list_jobs(db: aiosqlite.Connection, status: str | None = None, limit: int = 50) -> list[dict]:
    q = "SELECT * FROM reel_jobs"
    params = []
    if status:
        q += " WHERE status = ?"
        params.append(status)
    q += " ORDER BY created_at DESC LIMIT ?"
    params.append(limit)
    rows = await db.execute_fetchall(q, params)
    result = []
    for r in rows:
        priority = "normal"
        try:
            priority = r["priority"]
        except (IndexError, KeyError):
            pass
        result.append({
            "id": r["id"], "title": r["title"], "style": r["style"],
            "aspect_ratio": r["aspect_ratio"], "status": r["status"],
            "priority": priority or "normal",
            "photo_count": len(json.loads(r["photo_urls"])),
            "created_at": r["created_at"],
        })
    return result


async def search_jobs(db: aiosqlite.Connection, query: str, limit: int = 50) -> list[dict]:
    rows = await db.execute_fetchall(
        "SELECT * FROM reel_jobs WHERE title LIKE ? ORDER BY created_at DESC LIMIT ?",
        (f"%{query}%", limit),
    )
    result = []
    for r in rows:
        priority = "normal"
        try:
            priority = r["priority"]
        except (IndexError, KeyError):
            pass
        result.append({
            "id": r["id"], "title": r["title"], "style": r["style"],
            "aspect_ratio": r["aspect_ratio"], "status": r["status"],
            "priority": priority or "normal",
            "photo_count": len(json.loads(r["photo_urls"])),
            "created_at": r["created_at"],
        })
    return result


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
    coll_cnt = await db.execute_fetchall("SELECT COUNT(*) as c FROM collections")
    total_collections = coll_cnt[0]["c"] if coll_cnt else 0
    if not rows:
        return {"total_jobs": 0, "completed": 0, "processing": 0, "failed": 0,
                "avg_duration_seconds": 0.0, "most_used_style": None, "most_used_ratio": None,
                "total_brands": total_brands, "total_collections": total_collections}
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
        "total_collections": total_collections,
    }


async def duplicate_job(db: aiosqlite.Connection, job_id: int, overrides: dict) -> dict | None:
    rows = await db.execute_fetchall("SELECT * FROM reel_jobs WHERE id = ?", (job_id,))
    if not rows:
        return None
    src = rows[0]
    now = datetime.now(timezone.utc).isoformat()
    priority = "normal"
    try:
        priority = src["priority"] or "normal"
    except (IndexError, KeyError):
        pass
    cur = await db.execute(
        """INSERT INTO reel_jobs
           (title, photo_urls, style, aspect_ratio, caption, music_genre, brand_color,
            cta_text, duration_target, brand_id, priority, status, created_at)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'queued', ?)""",
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
            priority,
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


async def get_daily_analytics(db: aiosqlite.Connection, days: int = 30) -> list[dict]:
    since = (datetime.utcnow() - timedelta(days=days)).date().isoformat()
    rows = await db.execute_fetchall(
        "SELECT * FROM reel_jobs WHERE date(created_at) >= ?", (since,)
    )
    buckets: dict[str, dict] = {}
    for r in rows:
        day = r["created_at"][:10]
        if day not in buckets:
            buckets[day] = {"date": day, "created": 0, "completed": 0, "failed": 0, "styles": {}}
        buckets[day]["created"] += 1
        if r["status"] == "completed":
            buckets[day]["completed"] += 1
        elif r["status"] == "failed":
            buckets[day]["failed"] += 1
        s = r["style"]
        buckets[day]["styles"][s] = buckets[day]["styles"].get(s, 0) + 1
    result = []
    for b in sorted(buckets.values(), key=lambda x: x["date"]):
        styles = b.pop("styles")
        b["top_style"] = max(styles, key=styles.get) if styles else None
        result.append(b)
    return result


# ── Engagement Tracking ────────────────────────────────────────────────────

VALID_ENGAGEMENT_TYPES = {"view", "like", "share", "click", "save"}


async def record_engagement(db: aiosqlite.Connection, reel_id: int,
                            event_type: str, source: str | None = None) -> dict | None:
    rows = await db.execute_fetchall("SELECT id FROM reel_jobs WHERE id = ?", (reel_id,))
    if not rows:
        return None
    now = datetime.now(timezone.utc).isoformat()
    await db.execute(
        "INSERT INTO engagement_events (reel_id, event_type, source, created_at) VALUES (?, ?, ?, ?)",
        (reel_id, event_type, source, now),
    )
    await db.commit()
    return await get_reel_engagement(db, reel_id)


async def get_reel_engagement(db: aiosqlite.Connection, reel_id: int) -> dict | None:
    rows = await db.execute_fetchall("SELECT id FROM reel_jobs WHERE id = ?", (reel_id,))
    if not rows:
        return None
    events = await db.execute_fetchall(
        "SELECT event_type, COUNT(*) as cnt FROM engagement_events WHERE reel_id = ? GROUP BY event_type",
        (reel_id,),
    )
    counts = {et: 0 for et in VALID_ENGAGEMENT_TYPES}
    total = 0
    for e in events:
        counts[e["event_type"]] = e["cnt"]
        total += e["cnt"]
    views = counts.get("view", 0)
    interactions = counts.get("like", 0) + counts.get("share", 0) + counts.get("save", 0) + counts.get("click", 0)
    engagement_rate = round(interactions / views * 100, 1) if views > 0 else 0.0
    return {
        "reel_id": reel_id,
        "views": counts["view"],
        "likes": counts["like"],
        "shares": counts["share"],
        "clicks": counts["click"],
        "saves": counts["save"],
        "total_events": total,
        "engagement_rate": engagement_rate,
    }


async def get_engagement_analytics(db: aiosqlite.Connection, limit: int = 20,
                                   sort_by: str = "engagement_rate") -> list[dict]:
    reel_ids = await db.execute_fetchall(
        "SELECT DISTINCT reel_id FROM engagement_events"
    )
    results = []
    for row in reel_ids:
        rid = row["reel_id"]
        eng = await get_reel_engagement(db, rid)
        if eng:
            job = await get_job(db, rid)
            if job:
                eng["title"] = job["title"]
                eng["style"] = job["style"]
                eng["status"] = job["status"]
                results.append(eng)
    valid_sorts = {"engagement_rate", "views", "likes", "shares", "clicks", "saves", "total_events"}
    if sort_by not in valid_sorts:
        sort_by = "engagement_rate"
    results.sort(key=lambda x: x.get(sort_by, 0), reverse=True)
    return results[:limit]


# ── Brand Analytics ────────────────────────────────────────────────────────

async def get_brand_analytics(db: aiosqlite.Connection) -> list[dict]:
    brands = await db.execute_fetchall("SELECT * FROM brands ORDER BY name ASC")
    results = []
    for b in brands:
        bid = b["id"]
        reels = await db.execute_fetchall(
            "SELECT * FROM reel_jobs WHERE brand_id = ?", (bid,)
        )
        total = len(reels)
        completed = sum(1 for r in reels if r["status"] == "completed")
        failed = sum(1 for r in reels if r["status"] == "failed")
        durations = [r["duration_seconds"] for r in reels if r["duration_seconds"]]
        avg_dur = round(sum(durations) / len(durations), 1) if durations else 0.0
        completion_rate = round(completed / total * 100, 1) if total else 0.0
        style_counts: dict[str, int] = {}
        for r in reels:
            style_counts[r["style"]] = style_counts.get(r["style"], 0) + 1
        top_style = max(style_counts, key=style_counts.get) if style_counts else None
        engagement = await db.execute_fetchall(
            """SELECT event_type, COUNT(*) as cnt
               FROM engagement_events WHERE reel_id IN (SELECT id FROM reel_jobs WHERE brand_id = ?)
               GROUP BY event_type""",
            (bid,),
        )
        eng_counts = {e["event_type"]: e["cnt"] for e in engagement}
        total_views = eng_counts.get("view", 0)
        total_interactions = sum(eng_counts.get(t, 0) for t in ["like", "share", "save", "click"])
        eng_rate = round(total_interactions / total_views * 100, 1) if total_views > 0 else 0.0
        results.append({
            "brand_id": bid,
            "brand_name": b["name"],
            "total_reels": total,
            "completed": completed,
            "failed": failed,
            "completion_rate": completion_rate,
            "avg_duration_seconds": avg_dur,
            "top_style": top_style,
            "total_views": total_views,
            "engagement_rate": eng_rate,
        })
    return results


# ── Collections ────────────────────────────────────────────────────────────

async def create_collection(db: aiosqlite.Connection, data: dict) -> dict:
    now = datetime.now(timezone.utc).isoformat()
    cur = await db.execute(
        "INSERT INTO collections (name, description, created_at) VALUES (?, ?, ?)",
        (data["name"], data.get("description"), now),
    )
    await db.commit()
    return await get_collection(db, cur.lastrowid)


async def get_collection(db: aiosqlite.Connection, collection_id: int) -> dict | None:
    rows = await db.execute_fetchall("SELECT * FROM collections WHERE id = ?", (collection_id,))
    if not rows:
        return None
    cnt = await db.execute_fetchall(
        "SELECT COUNT(*) as c FROM collection_reels WHERE collection_id = ?", (collection_id,))
    r = rows[0]
    return {
        "id": r["id"], "name": r["name"], "description": r["description"],
        "reel_count": cnt[0]["c"] if cnt else 0, "created_at": r["created_at"],
    }


async def list_collections(db: aiosqlite.Connection) -> list[dict]:
    rows = await db.execute_fetchall("SELECT * FROM collections ORDER BY created_at DESC")
    result = []
    for r in rows:
        cnt = await db.execute_fetchall(
            "SELECT COUNT(*) as c FROM collection_reels WHERE collection_id = ?", (r["id"],))
        result.append({
            "id": r["id"], "name": r["name"], "description": r["description"],
            "reel_count": cnt[0]["c"] if cnt else 0, "created_at": r["created_at"],
        })
    return result


async def delete_collection(db: aiosqlite.Connection, collection_id: int) -> bool:
    rows = await db.execute_fetchall("SELECT id FROM collections WHERE id = ?", (collection_id,))
    if not rows:
        return False
    await db.execute("DELETE FROM collection_reels WHERE collection_id = ?", (collection_id,))
    await db.execute("DELETE FROM collections WHERE id = ?", (collection_id,))
    await db.commit()
    return True


async def add_reel_to_collection(db: aiosqlite.Connection, collection_id: int, reel_id: int) -> dict | str | None:
    coll = await get_collection(db, collection_id)
    if not coll:
        return None
    reel = await get_job(db, reel_id)
    if not reel:
        return "reel_not_found"
    now = datetime.now(timezone.utc).isoformat()
    try:
        await db.execute(
            "INSERT INTO collection_reels (collection_id, reel_id, added_at) VALUES (?, ?, ?)",
            (collection_id, reel_id, now),
        )
        await db.commit()
    except Exception:
        pass  # already exists
    return await get_collection(db, collection_id)


async def remove_reel_from_collection(db: aiosqlite.Connection, collection_id: int, reel_id: int) -> dict | None:
    coll = await get_collection(db, collection_id)
    if not coll:
        return None
    await db.execute(
        "DELETE FROM collection_reels WHERE collection_id = ? AND reel_id = ?",
        (collection_id, reel_id),
    )
    await db.commit()
    return await get_collection(db, collection_id)


async def get_collection_analytics(db: aiosqlite.Connection, collection_id: int) -> dict | None:
    coll = await get_collection(db, collection_id)
    if not coll:
        return None
    reel_ids = await db.execute_fetchall(
        "SELECT reel_id FROM collection_reels WHERE collection_id = ?", (collection_id,))
    if not reel_ids:
        return {
            "collection_id": collection_id, "collection_name": coll["name"],
            "total_reels": 0, "completed": 0, "failed": 0, "processing": 0,
            "completion_rate": 0.0, "total_views": 0, "total_likes": 0,
            "total_shares": 0, "engagement_rate": 0.0, "top_style": None,
        }
    ids = [r["reel_id"] for r in reel_ids]
    placeholders = ",".join("?" * len(ids))
    reels = await db.execute_fetchall(
        f"SELECT * FROM reel_jobs WHERE id IN ({placeholders})", ids)
    total = len(reels)
    completed = sum(1 for r in reels if r["status"] == "completed")
    failed = sum(1 for r in reels if r["status"] == "failed")
    processing = sum(1 for r in reels if r["status"] == "processing")
    style_counts: dict[str, int] = {}
    for r in reels:
        style_counts[r["style"]] = style_counts.get(r["style"], 0) + 1
    top_style = max(style_counts, key=style_counts.get) if style_counts else None
    # Engagement
    eng = await db.execute_fetchall(
        f"""SELECT event_type, COUNT(*) as cnt FROM engagement_events
            WHERE reel_id IN ({placeholders}) GROUP BY event_type""", ids)
    eng_counts = {e["event_type"]: e["cnt"] for e in eng}
    views = eng_counts.get("view", 0)
    likes = eng_counts.get("like", 0)
    shares = eng_counts.get("share", 0)
    clicks = eng_counts.get("click", 0)
    saves = eng_counts.get("save", 0)
    interactions = likes + shares + clicks + saves
    eng_rate = round(interactions / views * 100, 1) if views > 0 else 0.0
    return {
        "collection_id": collection_id,
        "collection_name": coll["name"],
        "total_reels": total,
        "completed": completed,
        "failed": failed,
        "processing": processing,
        "completion_rate": round(completed / total * 100, 1) if total else 0.0,
        "total_views": views,
        "total_likes": likes,
        "total_shares": shares,
        "engagement_rate": eng_rate,
        "top_style": top_style,
    }


# ── A/B Tests ──────────────────────────────────────────────────────────────

async def create_ab_test(db: aiosqlite.Connection, data: dict) -> dict | str:
    """Create an A/B test comparing multiple reels. Returns test or error string."""
    reel_ids = data["reel_ids"]
    for rid in reel_ids:
        reel = await get_job(db, rid)
        if not reel:
            return f"reel_not_found:{rid}"
    now = datetime.now(timezone.utc).isoformat()
    cur = await db.execute(
        "INSERT INTO ab_tests (name, created_at) VALUES (?, ?)",
        (data["name"], now),
    )
    test_id = cur.lastrowid
    for rid in reel_ids:
        await db.execute(
            "INSERT INTO ab_test_reels (test_id, reel_id) VALUES (?, ?)",
            (test_id, rid),
        )
    await db.commit()
    return await get_ab_test(db, test_id)


async def get_ab_test(db: aiosqlite.Connection, test_id: int) -> dict | None:
    rows = await db.execute_fetchall("SELECT * FROM ab_tests WHERE id = ?", (test_id,))
    if not rows:
        return None
    test = rows[0]
    reel_rows = await db.execute_fetchall(
        "SELECT reel_id FROM ab_test_reels WHERE test_id = ?", (test_id,))
    reels = []
    best_rate = -1
    winner_id = None
    for rr in reel_rows:
        rid = rr["reel_id"]
        job = await get_job(db, rid)
        eng = await get_reel_engagement(db, rid)
        if not job:
            continue
        rate = eng["engagement_rate"] if eng else 0.0
        reels.append({
            "reel_id": rid,
            "title": job["title"],
            "style": job["style"],
            "views": eng["views"] if eng else 0,
            "likes": eng["likes"] if eng else 0,
            "shares": eng["shares"] if eng else 0,
            "clicks": eng["clicks"] if eng else 0,
            "saves": eng["saves"] if eng else 0,
            "engagement_rate": rate,
            "is_winner": False,
        })
        if rate > best_rate:
            best_rate = rate
            winner_id = rid
    # Mark winner (only if there's meaningful data)
    has_data = any(r["views"] > 0 for r in reels)
    if has_data and winner_id is not None:
        for r in reels:
            r["is_winner"] = r["reel_id"] == winner_id
    else:
        winner_id = None
    return {
        "id": test["id"],
        "name": test["name"],
        "status": test["status"],
        "reels": reels,
        "winner_id": winner_id,
        "created_at": test["created_at"],
    }


async def list_ab_tests(db: aiosqlite.Connection) -> list[dict]:
    rows = await db.execute_fetchall("SELECT * FROM ab_tests ORDER BY created_at DESC")
    result = []
    for r in rows:
        test = await get_ab_test(db, r["id"])
        if test:
            result.append(test)
    return result


async def complete_ab_test(db: aiosqlite.Connection, test_id: int) -> dict | None:
    test = await get_ab_test(db, test_id)
    if not test:
        return None
    await db.execute(
        "UPDATE ab_tests SET status = 'completed', winner_id = ? WHERE id = ?",
        (test["winner_id"], test_id),
    )
    await db.commit()
    return await get_ab_test(db, test_id)


# ── Render Queue ───────────────────────────────────────────────────────────

async def get_render_queue(db: aiosqlite.Connection) -> list[dict]:
    """Get ordered render queue: queued jobs sorted by priority then creation time."""
    rows = await db.execute_fetchall(
        "SELECT * FROM reel_jobs WHERE status IN ('queued', 'processing') ORDER BY created_at ASC"
    )
    result = []
    for r in rows:
        priority = "normal"
        try:
            priority = r["priority"] or "normal"
        except (IndexError, KeyError):
            pass
        result.append({
            "id": r["id"],
            "title": r["title"],
            "style": r["style"],
            "priority": priority,
            "status": r["status"],
            "created_at": r["created_at"],
            "position": 0,
        })
    # Sort by priority order, then by creation time
    result.sort(key=lambda x: (PRIORITY_ORDER.get(x["priority"], 2), x["created_at"]))
    for i, item in enumerate(result):
        item["position"] = i + 1
    return result

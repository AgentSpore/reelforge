from __future__ import annotations
import asyncio
import json
import random
import hashlib
import hmac
import secrets
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

VALID_PLATFORMS = {"instagram", "tiktok", "youtube", "facebook"}

VALID_TEMPLATE_CATEGORIES = {
    "product", "lifestyle", "tutorial", "testimonial",
    "announcement", "seasonal", "promotion", "general",
}

SHARE_BASE_URL = "https://reelforge.io/share"


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


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
    await _migrate_tags(db)
    await _migrate_webhooks(db)
    await _migrate_scheduled(db)
    await _migrate_templates(db)
    await _migrate_comments(db)
    await _migrate_share_links(db)
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


async def _migrate_tags(db: aiosqlite.Connection):
    await db.executescript("""
        CREATE TABLE IF NOT EXISTS reel_tags (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            reel_id INTEGER NOT NULL REFERENCES reel_jobs(id) ON DELETE CASCADE,
            tag TEXT NOT NULL,
            created_at TEXT NOT NULL,
            UNIQUE(reel_id, tag)
        );
        CREATE INDEX IF NOT EXISTS idx_reel_tags_tag ON reel_tags(tag);
        CREATE INDEX IF NOT EXISTS idx_reel_tags_reel ON reel_tags(reel_id);
    """)


async def _migrate_webhooks(db: aiosqlite.Connection):
    await db.execute("""
        CREATE TABLE IF NOT EXISTS webhooks (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            url TEXT NOT NULL,
            events TEXT NOT NULL DEFAULT '["all"]',
            secret TEXT,
            is_active INTEGER NOT NULL DEFAULT 1,
            deliveries INTEGER NOT NULL DEFAULT 0,
            last_delivery_at TEXT,
            created_at TEXT NOT NULL
        )
    """)


async def _migrate_scheduled(db: aiosqlite.Connection):
    await db.executescript("""
        CREATE TABLE IF NOT EXISTS scheduled_publishes (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            reel_id INTEGER NOT NULL REFERENCES reel_jobs(id) ON DELETE CASCADE,
            publish_at TEXT NOT NULL,
            platform TEXT NOT NULL DEFAULT 'instagram',
            caption TEXT,
            status TEXT NOT NULL DEFAULT 'scheduled',
            created_at TEXT NOT NULL,
            UNIQUE(reel_id, platform)
        );
        CREATE INDEX IF NOT EXISTS idx_sched_publish ON scheduled_publishes(publish_at);
        CREATE INDEX IF NOT EXISTS idx_sched_reel ON scheduled_publishes(reel_id);
    """)


async def _migrate_templates(db: aiosqlite.Connection):
    await db.executescript("""
        CREATE TABLE IF NOT EXISTS templates (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL,
            description TEXT,
            source_reel_id INTEGER,
            style TEXT NOT NULL DEFAULT 'dynamic',
            aspect_ratio TEXT NOT NULL DEFAULT '9:16',
            music_genre TEXT,
            brand_color TEXT,
            cta_text TEXT,
            duration_target INTEGER NOT NULL DEFAULT 15,
            brand_id INTEGER,
            category TEXT NOT NULL DEFAULT 'general',
            times_used INTEGER NOT NULL DEFAULT 0,
            created_at TEXT NOT NULL
        );
        CREATE INDEX IF NOT EXISTS idx_templates_category ON templates(category);
        CREATE INDEX IF NOT EXISTS idx_templates_brand ON templates(brand_id);
    """)


async def _migrate_comments(db: aiosqlite.Connection):
    await db.executescript("""
        CREATE TABLE IF NOT EXISTS reel_comments (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            reel_id INTEGER NOT NULL REFERENCES reel_jobs(id) ON DELETE CASCADE,
            author TEXT NOT NULL,
            content TEXT NOT NULL,
            is_resolved INTEGER NOT NULL DEFAULT 0,
            parent_id INTEGER,
            created_at TEXT NOT NULL,
            updated_at TEXT,
            FOREIGN KEY (parent_id) REFERENCES reel_comments(id) ON DELETE CASCADE
        );
        CREATE INDEX IF NOT EXISTS idx_comments_reel ON reel_comments(reel_id);
        CREATE INDEX IF NOT EXISTS idx_comments_parent ON reel_comments(parent_id);
        CREATE INDEX IF NOT EXISTS idx_comments_author ON reel_comments(author);
    """)


async def _migrate_share_links(db: aiosqlite.Connection):
    await db.executescript("""
        CREATE TABLE IF NOT EXISTS share_links (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            reel_id INTEGER NOT NULL REFERENCES reel_jobs(id) ON DELETE CASCADE,
            token TEXT NOT NULL UNIQUE,
            expires_at TEXT,
            password_hash TEXT,
            allow_download INTEGER NOT NULL DEFAULT 1,
            view_count INTEGER NOT NULL DEFAULT 0,
            download_count INTEGER NOT NULL DEFAULT 0,
            created_at TEXT NOT NULL,
            last_accessed_at TEXT
        );
        CREATE UNIQUE INDEX IF NOT EXISTS idx_share_token ON share_links(token);
        CREATE INDEX IF NOT EXISTS idx_share_reel ON share_links(reel_id);
    """)


# -- Row helpers -------------------------------------------------------------

def _job_row(r: aiosqlite.Row, tags: list[str] | None = None) -> dict:
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
        "tags": tags or [],
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


async def _get_reel_tags(db: aiosqlite.Connection, reel_id: int) -> list[str]:
    rows = await db.execute_fetchall(
        "SELECT tag FROM reel_tags WHERE reel_id = ? ORDER BY tag ASC", (reel_id,))
    return [r["tag"] for r in rows]


def _template_row(r: aiosqlite.Row) -> dict:
    return {
        "id": r["id"],
        "name": r["name"],
        "description": r["description"],
        "source_reel_id": r["source_reel_id"],
        "style": r["style"],
        "aspect_ratio": r["aspect_ratio"],
        "music_genre": r["music_genre"],
        "brand_color": r["brand_color"],
        "cta_text": r["cta_text"],
        "duration_target": r["duration_target"],
        "brand_id": r["brand_id"],
        "category": r["category"],
        "times_used": r["times_used"],
        "created_at": r["created_at"],
    }


def _comment_row(r: aiosqlite.Row, replies_count: int = 0) -> dict:
    return {
        "id": r["id"],
        "reel_id": r["reel_id"],
        "author": r["author"],
        "content": r["content"],
        "is_resolved": bool(r["is_resolved"]),
        "parent_id": r["parent_id"],
        "replies_count": replies_count,
        "created_at": r["created_at"],
        "updated_at": r["updated_at"],
    }


def _share_link_row(r: aiosqlite.Row) -> dict:
    expires_at = r["expires_at"]
    is_expired = False
    if expires_at:
        try:
            exp_dt = datetime.fromisoformat(expires_at)
            is_expired = datetime.now(timezone.utc) > exp_dt
        except (ValueError, TypeError):
            pass
    return {
        "id": r["id"],
        "reel_id": r["reel_id"],
        "token": r["token"],
        "expires_at": expires_at,
        "allow_download": bool(r["allow_download"]),
        "view_count": r["view_count"],
        "download_count": r["download_count"],
        "is_expired": is_expired,
        "share_url": f"{SHARE_BASE_URL}/{r['token']}",
        "created_at": r["created_at"],
        "last_accessed_at": r["last_accessed_at"],
    }


# -- Brands ------------------------------------------------------------------

async def get_brand(db: aiosqlite.Connection, brand_id: int) -> dict | None:
    rows = await db.execute_fetchall("SELECT * FROM brands WHERE id = ?", (brand_id,))
    if not rows:
        return None
    cnt = await db.execute_fetchall(
        "SELECT COUNT(*) as c FROM reel_jobs WHERE brand_id = ?", (brand_id,))
    return _brand_row(rows[0], cnt[0]["c"] if cnt else 0)


async def create_brand(db: aiosqlite.Connection, data: dict) -> dict:
    now = _now()
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


# -- Reel Jobs ---------------------------------------------------------------

async def create_job(db: aiosqlite.Connection, data: dict) -> dict:
    data = await apply_brand_defaults(db, data)
    priority = data.get("priority", "normal")
    if priority not in VALID_PRIORITIES:
        priority = "normal"
    now = _now()
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
    reel_id = cur.lastrowid
    # Auto-add tags from request
    for tag in data.get("tags", []):
        tag = tag.strip().lower()
        if tag:
            try:
                await db.execute(
                    "INSERT INTO reel_tags (reel_id, tag, created_at) VALUES (?, ?, ?)",
                    (reel_id, tag, now))
            except Exception:
                pass
    await db.commit()
    rows = await db.execute_fetchall("SELECT * FROM reel_jobs WHERE id = ?", (reel_id,))
    tags = await _get_reel_tags(db, reel_id)
    return _job_row(rows[0], tags)


async def batch_create_jobs(db: aiosqlite.Connection, data: dict) -> list[dict]:
    styles = data["styles"]
    base = {k: v for k, v in data.items() if k != "styles"}
    jobs = []
    for style in styles:
        job_data = {**base, "style": style}
        job = await create_job(db, job_data)
        jobs.append(job)
    return jobs


async def list_jobs(db: aiosqlite.Connection, status: str | None = None,
                    tag: str | None = None, limit: int = 50) -> list[dict]:
    if tag:
        q = """SELECT rj.* FROM reel_jobs rj
               JOIN reel_tags rt ON rj.id = rt.reel_id
               WHERE rt.tag = ?"""
        params: list = [tag]
        if status:
            q += " AND rj.status = ?"
            params.append(status)
        q += " ORDER BY rj.created_at DESC LIMIT ?"
        params.append(limit)
    else:
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
        tags = await _get_reel_tags(db, r["id"])
        result.append({
            "id": r["id"], "title": r["title"], "style": r["style"],
            "aspect_ratio": r["aspect_ratio"], "status": r["status"],
            "priority": priority or "normal",
            "photo_count": len(json.loads(r["photo_urls"])),
            "tags": tags,
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
        tags = await _get_reel_tags(db, r["id"])
        result.append({
            "id": r["id"], "title": r["title"], "style": r["style"],
            "aspect_ratio": r["aspect_ratio"], "status": r["status"],
            "priority": priority or "normal",
            "photo_count": len(json.loads(r["photo_urls"])),
            "tags": tags,
            "created_at": r["created_at"],
        })
    return result


async def get_job(db: aiosqlite.Connection, job_id: int) -> dict | None:
    rows = await db.execute_fetchall("SELECT * FROM reel_jobs WHERE id = ?", (job_id,))
    if not rows:
        return None
    tags = await _get_reel_tags(db, job_id)
    return _job_row(rows[0], tags)


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
    now = _now()
    await db.execute(
        """UPDATE reel_jobs SET status = 'completed', output_url = ?,
           duration_seconds = ?, render_log = ?, completed_at = ? WHERE id = ?""",
        (output_url, duration, json.dumps(log), now, job_id)
    )
    await db.commit()
    # Trigger webhooks for render_complete
    await _fire_webhooks(db, "render_complete", {
        "reel_id": job_id, "title": job["title"], "style": job["style"],
        "output_url": output_url, "duration_seconds": duration,
    })


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
    tag_cnt = await db.execute_fetchall("SELECT COUNT(DISTINCT tag) as c FROM reel_tags")
    total_tags = tag_cnt[0]["c"] if tag_cnt else 0
    wh_cnt = await db.execute_fetchall("SELECT COUNT(*) as c FROM webhooks WHERE is_active = 1")
    total_webhooks = wh_cnt[0]["c"] if wh_cnt else 0
    sched_cnt = await db.execute_fetchall(
        "SELECT COUNT(*) as c FROM scheduled_publishes WHERE status = 'scheduled'")
    scheduled_count = sched_cnt[0]["c"] if sched_cnt else 0

    if not rows:
        return {"total_jobs": 0, "completed": 0, "processing": 0, "failed": 0,
                "avg_duration_seconds": 0.0, "most_used_style": None, "most_used_ratio": None,
                "total_brands": total_brands, "total_collections": total_collections,
                "total_tags": total_tags, "total_webhooks": total_webhooks,
                "scheduled_count": scheduled_count}
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
        "total_tags": total_tags,
        "total_webhooks": total_webhooks,
        "scheduled_count": scheduled_count,
    }


async def duplicate_job(db: aiosqlite.Connection, job_id: int, overrides: dict) -> dict | None:
    rows = await db.execute_fetchall("SELECT * FROM reel_jobs WHERE id = ?", (job_id,))
    if not rows:
        return None
    src = rows[0]
    now = _now()
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
    new_id = cur.lastrowid
    # Copy tags
    src_tags = await _get_reel_tags(db, job_id)
    for tag in src_tags:
        try:
            await db.execute(
                "INSERT INTO reel_tags (reel_id, tag, created_at) VALUES (?, ?, ?)",
                (new_id, tag, now))
        except Exception:
            pass
    await db.commit()
    new_rows = await db.execute_fetchall("SELECT * FROM reel_jobs WHERE id = ?", (new_id,))
    tags = await _get_reel_tags(db, new_id)
    return _job_row(new_rows[0], tags) if new_rows else None


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
    since = (datetime.now(timezone.utc) - timedelta(days=days)).date().isoformat()
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


# -- Engagement Tracking ----------------------------------------------------

VALID_ENGAGEMENT_TYPES = {"view", "like", "share", "click", "save"}


async def record_engagement(db: aiosqlite.Connection, reel_id: int,
                            event_type: str, source: str | None = None) -> dict | None:
    rows = await db.execute_fetchall("SELECT id FROM reel_jobs WHERE id = ?", (reel_id,))
    if not rows:
        return None
    now = _now()
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


# -- Brand Analytics ---------------------------------------------------------

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


# -- Collections -------------------------------------------------------------

async def create_collection(db: aiosqlite.Connection, data: dict) -> dict:
    now = _now()
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
    now = _now()
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


# -- A/B Tests ---------------------------------------------------------------

async def create_ab_test(db: aiosqlite.Connection, data: dict) -> dict | str:
    reel_ids = data["reel_ids"]
    for rid in reel_ids:
        reel = await get_job(db, rid)
        if not reel:
            return f"reel_not_found:{rid}"
    now = _now()
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
            "reel_id": rid, "title": job["title"], "style": job["style"],
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
    has_data = any(r["views"] > 0 for r in reels)
    if has_data and winner_id is not None:
        for r in reels:
            r["is_winner"] = r["reel_id"] == winner_id
    else:
        winner_id = None
    return {
        "id": test["id"], "name": test["name"], "status": test["status"],
        "reels": reels, "winner_id": winner_id, "created_at": test["created_at"],
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


# -- Render Queue ------------------------------------------------------------

async def get_render_queue(db: aiosqlite.Connection) -> list[dict]:
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
            "id": r["id"], "title": r["title"], "style": r["style"],
            "priority": priority, "status": r["status"],
            "created_at": r["created_at"], "position": 0,
        })
    result.sort(key=lambda x: (PRIORITY_ORDER.get(x["priority"], 2), x["created_at"]))
    for i, item in enumerate(result):
        item["position"] = i + 1
    return result


# -- Tags --------------------------------------------------------------------

async def add_tag(db: aiosqlite.Connection, reel_id: int, tag: str) -> list[str] | None:
    rows = await db.execute_fetchall("SELECT id FROM reel_jobs WHERE id = ?", (reel_id,))
    if not rows:
        return None
    tag = tag.strip().lower()
    now = _now()
    try:
        await db.execute(
            "INSERT INTO reel_tags (reel_id, tag, created_at) VALUES (?, ?, ?)",
            (reel_id, tag, now))
        await db.commit()
    except Exception:
        pass  # duplicate
    return await _get_reel_tags(db, reel_id)


async def remove_tag(db: aiosqlite.Connection, reel_id: int, tag: str) -> list[str] | None:
    rows = await db.execute_fetchall("SELECT id FROM reel_jobs WHERE id = ?", (reel_id,))
    if not rows:
        return None
    tag = tag.strip().lower()
    cur = await db.execute(
        "DELETE FROM reel_tags WHERE reel_id = ? AND tag = ?", (reel_id, tag))
    await db.commit()
    if cur.rowcount == 0:
        return "tag_not_found"
    return await _get_reel_tags(db, reel_id)


async def list_all_tags(db: aiosqlite.Connection) -> list[dict]:
    rows = await db.execute_fetchall(
        "SELECT tag, COUNT(*) as cnt FROM reel_tags GROUP BY tag ORDER BY cnt DESC")
    return [{"tag": r["tag"], "count": r["cnt"]} for r in rows]


async def get_tag_analytics(db: aiosqlite.Connection, tag: str) -> dict | None:
    tag = tag.strip().lower()
    reel_ids = await db.execute_fetchall(
        "SELECT reel_id FROM reel_tags WHERE tag = ?", (tag,))
    if not reel_ids:
        return None
    ids = [r["reel_id"] for r in reel_ids]
    placeholders = ",".join("?" * len(ids))
    reels = await db.execute_fetchall(
        f"SELECT * FROM reel_jobs WHERE id IN ({placeholders})", ids)
    total = len(reels)
    completed = sum(1 for r in reels if r["status"] == "completed")
    failed = sum(1 for r in reels if r["status"] == "failed")
    style_counts: dict[str, int] = {}
    for r in reels:
        style_counts[r["style"]] = style_counts.get(r["style"], 0) + 1
    top_style = max(style_counts, key=style_counts.get) if style_counts else None
    # Avg engagement
    total_rate = 0.0
    eng_count = 0
    for rid in ids:
        eng = await get_reel_engagement(db, rid)
        if eng and eng["views"] > 0:
            total_rate += eng["engagement_rate"]
            eng_count += 1
    avg_eng = round(total_rate / eng_count, 1) if eng_count > 0 else 0.0
    return {
        "tag": tag, "total_reels": total, "completed": completed,
        "failed": failed, "avg_engagement_rate": avg_eng, "top_style": top_style,
    }


# -- Webhooks ----------------------------------------------------------------

async def create_webhook(db: aiosqlite.Connection, data: dict) -> dict:
    now = _now()
    events_json = json.dumps(data["events"])
    cur = await db.execute(
        "INSERT INTO webhooks (url, events, secret, created_at) VALUES (?, ?, ?, ?)",
        (data["url"], events_json, data.get("secret"), now),
    )
    await db.commit()
    return await get_webhook(db, cur.lastrowid)


async def get_webhook(db: aiosqlite.Connection, wh_id: int) -> dict | None:
    rows = await db.execute_fetchall("SELECT * FROM webhooks WHERE id = ?", (wh_id,))
    if not rows:
        return None
    return _webhook_row(rows[0])


async def list_webhooks(db: aiosqlite.Connection) -> list[dict]:
    rows = await db.execute_fetchall("SELECT * FROM webhooks ORDER BY created_at DESC")
    return [_webhook_row(r) for r in rows]


async def update_webhook(db: aiosqlite.Connection, wh_id: int, updates: dict) -> dict | None:
    fields = {}
    if "url" in updates and updates["url"] is not None:
        fields["url"] = updates["url"]
    if "events" in updates and updates["events"] is not None:
        fields["events"] = json.dumps(updates["events"])
    if "is_active" in updates and updates["is_active"] is not None:
        fields["is_active"] = int(updates["is_active"])
    if not fields:
        return await get_webhook(db, wh_id)
    set_clause = ", ".join(f"{k} = ?" for k in fields)
    values = list(fields.values()) + [wh_id]
    cur = await db.execute(f"UPDATE webhooks SET {set_clause} WHERE id = ?", values)
    await db.commit()
    if cur.rowcount == 0:
        return None
    return await get_webhook(db, wh_id)


async def delete_webhook(db: aiosqlite.Connection, wh_id: int) -> bool:
    cur = await db.execute("DELETE FROM webhooks WHERE id = ?", (wh_id,))
    await db.commit()
    return cur.rowcount > 0


def _webhook_row(r: aiosqlite.Row) -> dict:
    try:
        events = json.loads(r["events"])
    except (json.JSONDecodeError, TypeError):
        events = ["all"]
    return {
        "id": r["id"], "url": r["url"], "events": events,
        "is_active": bool(r["is_active"]),
        "deliveries": r["deliveries"],
        "last_delivery_at": r["last_delivery_at"],
        "created_at": r["created_at"],
    }


async def _fire_webhooks(db: aiosqlite.Connection, event: str, payload: dict):
    """Fire all matching webhooks (best-effort, no actual HTTP in MVP)."""
    rows = await db.execute_fetchall(
        "SELECT * FROM webhooks WHERE is_active = 1")
    now = _now()
    for r in rows:
        try:
            events = json.loads(r["events"])
        except Exception:
            events = []
        if "all" in events or event in events:
            # In production, would POST to r["url"] with payload + HMAC signature.
            # For MVP, just record the delivery.
            await db.execute(
                "UPDATE webhooks SET deliveries = deliveries + 1, last_delivery_at = ? WHERE id = ?",
                (now, r["id"]))
    await db.commit()


# -- Scheduled Publishing ---------------------------------------------------

async def schedule_publish(db: aiosqlite.Connection, reel_id: int, data: dict) -> dict | str | None:
    rows = await db.execute_fetchall("SELECT * FROM reel_jobs WHERE id = ?", (reel_id,))
    if not rows:
        return None
    reel = rows[0]
    platform = data.get("platform", "instagram")
    if platform not in VALID_PLATFORMS:
        return "invalid_platform"
    # Check reel is completed
    if reel["status"] != "completed":
        return "not_completed"
    now = _now()
    try:
        cur = await db.execute(
            """INSERT INTO scheduled_publishes (reel_id, publish_at, platform, caption, created_at)
               VALUES (?, ?, ?, ?, ?)""",
            (reel_id, data["publish_at"], platform, data.get("caption"), now),
        )
        await db.commit()
    except Exception:
        return "already_scheduled"
    return await _get_schedule(db, cur.lastrowid)


async def _get_schedule(db: aiosqlite.Connection, sched_id: int) -> dict | None:
    rows = await db.execute_fetchall(
        """SELECT sp.*, rj.title as reel_title FROM scheduled_publishes sp
           JOIN reel_jobs rj ON sp.reel_id = rj.id
           WHERE sp.id = ?""", (sched_id,))
    if not rows:
        return None
    r = rows[0]
    return {
        "id": r["id"], "reel_id": r["reel_id"], "reel_title": r["reel_title"],
        "publish_at": r["publish_at"], "platform": r["platform"],
        "caption": r["caption"], "status": r["status"],
        "created_at": r["created_at"],
    }


async def list_scheduled(db: aiosqlite.Connection, status: str | None = None) -> list[dict]:
    q = """SELECT sp.*, rj.title as reel_title FROM scheduled_publishes sp
           JOIN reel_jobs rj ON sp.reel_id = rj.id"""
    params: list = []
    if status:
        q += " WHERE sp.status = ?"
        params.append(status)
    q += " ORDER BY sp.publish_at ASC"
    rows = await db.execute_fetchall(q, params)
    return [{
        "id": r["id"], "reel_id": r["reel_id"], "reel_title": r["reel_title"],
        "publish_at": r["publish_at"], "platform": r["platform"],
        "caption": r["caption"], "status": r["status"],
        "created_at": r["created_at"],
    } for r in rows]


async def cancel_schedule(db: aiosqlite.Connection, reel_id: int, platform: str | None = None) -> bool:
    if platform:
        cur = await db.execute(
            "UPDATE scheduled_publishes SET status = 'cancelled' WHERE reel_id = ? AND platform = ? AND status = 'scheduled'",
            (reel_id, platform))
    else:
        cur = await db.execute(
            "UPDATE scheduled_publishes SET status = 'cancelled' WHERE reel_id = ? AND status = 'scheduled'",
            (reel_id,))
    await db.commit()
    return cur.rowcount > 0


# -- Reel Templates (v0.9.0) ------------------------------------------------

async def create_template(db: aiosqlite.Connection, data: dict) -> dict | str | None:
    """Create a reusable reel template, optionally copying settings from an existing reel."""
    category = data.get("category", "general")
    if category not in VALID_TEMPLATE_CATEGORIES:
        return "invalid_category"

    source_reel_id = data.get("source_reel_id")
    style = data.get("style", "dynamic")
    aspect_ratio = data.get("aspect_ratio", "9:16")
    music_genre = data.get("music_genre")
    brand_color = data.get("brand_color")
    cta_text = data.get("cta_text")
    duration_target = data.get("duration_target", 15)
    brand_id = data.get("brand_id")

    # If source_reel_id provided, copy settings from that reel
    if source_reel_id is not None:
        reel = await get_job(db, source_reel_id)
        if not reel:
            return "reel_not_found"
        # Copy reel settings, but allow explicit overrides
        if data.get("style") is None:
            style = reel["style"]
        if data.get("aspect_ratio") is None:
            aspect_ratio = reel["aspect_ratio"]
        if data.get("brand_id") is None:
            brand_id = reel.get("brand_id")
        # For optional fields, only copy from reel if not explicitly provided
        if "music_genre" not in data or data["music_genre"] is None:
            # Try to get music_genre from render_log
            if reel.get("render_log") and isinstance(reel["render_log"], dict):
                music_genre = reel["render_log"].get("audio_track")
                if music_genre == "none":
                    music_genre = None
        if "brand_color" not in data or data["brand_color"] is None:
            if reel.get("render_log") and isinstance(reel["render_log"], dict):
                bc = reel["render_log"].get("brand_color")
                if bc and bc != "#000000":
                    brand_color = bc
        if "cta_text" not in data or data["cta_text"] is None:
            if reel.get("render_log") and isinstance(reel["render_log"], dict):
                cta_text = reel["render_log"].get("cta") or None
        if data.get("duration_target") is None and reel.get("duration_seconds"):
            duration_target = int(reel["duration_seconds"])

    # Validate style
    if style not in STYLE_CATALOG:
        return "invalid_style"

    # Validate brand_id if provided
    if brand_id is not None:
        brand = await get_brand(db, brand_id)
        if not brand:
            return "brand_not_found"

    now = _now()
    cur = await db.execute(
        """INSERT INTO templates
           (name, description, source_reel_id, style, aspect_ratio, music_genre,
            brand_color, cta_text, duration_target, brand_id, category, created_at)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
        (data["name"], data.get("description"), source_reel_id, style,
         aspect_ratio, music_genre, brand_color, cta_text,
         duration_target, brand_id, category, now),
    )
    await db.commit()
    return await get_template(db, cur.lastrowid)


async def get_template(db: aiosqlite.Connection, template_id: int) -> dict | None:
    rows = await db.execute_fetchall("SELECT * FROM templates WHERE id = ?", (template_id,))
    if not rows:
        return None
    return _template_row(rows[0])


async def list_templates(db: aiosqlite.Connection, category: str | None = None,
                         brand_id: int | None = None) -> list[dict]:
    q = "SELECT * FROM templates"
    params: list = []
    conditions = []
    if category:
        conditions.append("category = ?")
        params.append(category)
    if brand_id is not None:
        conditions.append("brand_id = ?")
        params.append(brand_id)
    if conditions:
        q += " WHERE " + " AND ".join(conditions)
    q += " ORDER BY times_used DESC, created_at DESC"
    rows = await db.execute_fetchall(q, params)
    return [_template_row(r) for r in rows]


async def update_template(db: aiosqlite.Connection, template_id: int, updates: dict) -> dict | str | None:
    existing = await get_template(db, template_id)
    if not existing:
        return None
    fields = {k: v for k, v in updates.items() if v is not None}
    if not fields:
        return existing
    # Validate category if being updated
    if "category" in fields and fields["category"] not in VALID_TEMPLATE_CATEGORIES:
        return "invalid_category"
    set_clause = ", ".join(f"{k} = ?" for k in fields)
    values = list(fields.values()) + [template_id]
    cur = await db.execute(f"UPDATE templates SET {set_clause} WHERE id = ?", values)
    await db.commit()
    if cur.rowcount == 0:
        return None
    return await get_template(db, template_id)


async def delete_template(db: aiosqlite.Connection, template_id: int) -> bool:
    cur = await db.execute("DELETE FROM templates WHERE id = ?", (template_id,))
    await db.commit()
    return cur.rowcount > 0


async def create_reel_from_template(db: aiosqlite.Connection, template_id: int, data: dict) -> dict | str | None:
    """Create a reel job using a template's settings, then increment times_used."""
    template = await get_template(db, template_id)
    if not template:
        return None

    priority = data.get("priority", "normal")
    if priority not in VALID_PRIORITIES:
        return "invalid_priority"

    job_data = {
        "title": data["title"],
        "photo_urls": data["photo_urls"],
        "style": template["style"],
        "aspect_ratio": template["aspect_ratio"],
        "caption": data.get("caption"),
        "music_genre": template["music_genre"],
        "brand_color": template["brand_color"],
        "cta_text": template["cta_text"],
        "duration_target": template["duration_target"],
        "brand_id": template["brand_id"],
        "priority": priority,
        "tags": data.get("tags", []),
    }
    job = await create_job(db, job_data)

    # Increment times_used
    await db.execute(
        "UPDATE templates SET times_used = times_used + 1 WHERE id = ?", (template_id,))
    await db.commit()

    return job


# -- Reel Comments / Collaboration (v0.9.0) ---------------------------------

async def add_comment(db: aiosqlite.Connection, reel_id: int, data: dict) -> dict | str | None:
    """Add a comment to a reel. Supports threaded replies via parent_id."""
    # Validate reel exists
    reel = await get_job(db, reel_id)
    if not reel:
        return None

    parent_id = data.get("parent_id")
    if parent_id is not None:
        # Validate parent comment exists and belongs to same reel
        parent_rows = await db.execute_fetchall(
            "SELECT * FROM reel_comments WHERE id = ?", (parent_id,))
        if not parent_rows:
            return "parent_not_found"
        if parent_rows[0]["reel_id"] != reel_id:
            return "parent_wrong_reel"
        # Don't allow nested replies (only one level of threading)
        if parent_rows[0]["parent_id"] is not None:
            return "nested_reply_not_allowed"

    now = _now()
    cur = await db.execute(
        """INSERT INTO reel_comments (reel_id, author, content, parent_id, created_at)
           VALUES (?, ?, ?, ?, ?)""",
        (reel_id, data["author"], data["content"], parent_id, now),
    )
    await db.commit()
    return await get_comment(db, cur.lastrowid)


async def get_comment(db: aiosqlite.Connection, comment_id: int) -> dict | None:
    rows = await db.execute_fetchall("SELECT * FROM reel_comments WHERE id = ?", (comment_id,))
    if not rows:
        return None
    # Count replies
    reply_cnt = await db.execute_fetchall(
        "SELECT COUNT(*) as c FROM reel_comments WHERE parent_id = ?", (comment_id,))
    count = reply_cnt[0]["c"] if reply_cnt else 0
    return _comment_row(rows[0], count)


async def list_comments(db: aiosqlite.Connection, reel_id: int,
                        author: str | None = None,
                        is_resolved: bool | None = None) -> list[dict]:
    """List top-level comments for a reel with reply counts. Filters by author and resolved status."""
    # Validate reel exists
    reel_rows = await db.execute_fetchall("SELECT id FROM reel_jobs WHERE id = ?", (reel_id,))
    if not reel_rows:
        return None  # type: ignore[return-value]

    q = "SELECT * FROM reel_comments WHERE reel_id = ? AND parent_id IS NULL"
    params: list = [reel_id]
    if author:
        q += " AND author = ?"
        params.append(author)
    if is_resolved is not None:
        q += " AND is_resolved = ?"
        params.append(int(is_resolved))
    q += " ORDER BY created_at DESC"
    rows = await db.execute_fetchall(q, params)
    result = []
    for r in rows:
        reply_cnt = await db.execute_fetchall(
            "SELECT COUNT(*) as c FROM reel_comments WHERE parent_id = ?", (r["id"],))
        count = reply_cnt[0]["c"] if reply_cnt else 0
        result.append(_comment_row(r, count))
    return result


async def update_comment(db: aiosqlite.Connection, comment_id: int, updates: dict) -> dict | None:
    existing = await get_comment(db, comment_id)
    if not existing:
        return None
    fields = {}
    if "content" in updates and updates["content"] is not None:
        fields["content"] = updates["content"]
    if "is_resolved" in updates and updates["is_resolved"] is not None:
        fields["is_resolved"] = int(updates["is_resolved"])
    if not fields:
        return existing
    fields["updated_at"] = _now()
    set_clause = ", ".join(f"{k} = ?" for k in fields)
    values = list(fields.values()) + [comment_id]
    await db.execute(f"UPDATE reel_comments SET {set_clause} WHERE id = ?", values)
    await db.commit()
    return await get_comment(db, comment_id)


async def delete_comment(db: aiosqlite.Connection, comment_id: int) -> bool:
    """Delete a comment and cascade-delete its replies."""
    rows = await db.execute_fetchall("SELECT id FROM reel_comments WHERE id = ?", (comment_id,))
    if not rows:
        return False
    # Delete replies first
    await db.execute("DELETE FROM reel_comments WHERE parent_id = ?", (comment_id,))
    # Delete the comment itself
    await db.execute("DELETE FROM reel_comments WHERE id = ?", (comment_id,))
    await db.commit()
    return True


async def resolve_comment(db: aiosqlite.Connection, comment_id: int) -> dict | None:
    """Mark a comment as resolved."""
    existing = await get_comment(db, comment_id)
    if not existing:
        return None
    now = _now()
    await db.execute(
        "UPDATE reel_comments SET is_resolved = 1, updated_at = ? WHERE id = ?",
        (now, comment_id))
    await db.commit()
    return await get_comment(db, comment_id)


async def get_comment_stats(db: aiosqlite.Connection) -> dict:
    """Get global comment statistics: totals, resolved/unresolved, top commenters."""
    total_row = await db.execute_fetchall("SELECT COUNT(*) as c FROM reel_comments")
    total = total_row[0]["c"] if total_row else 0

    resolved_row = await db.execute_fetchall(
        "SELECT COUNT(*) as c FROM reel_comments WHERE is_resolved = 1")
    resolved = resolved_row[0]["c"] if resolved_row else 0

    unresolved = total - resolved

    top_commenters_rows = await db.execute_fetchall(
        """SELECT author, COUNT(*) as comment_count
           FROM reel_comments
           GROUP BY author
           ORDER BY comment_count DESC
           LIMIT 10""")
    top_commenters = [
        {"author": r["author"], "comment_count": r["comment_count"]}
        for r in top_commenters_rows
    ]

    return {
        "total_comments": total,
        "resolved": resolved,
        "unresolved": unresolved,
        "top_commenters": top_commenters,
    }


# -- Export & Share Links (v0.9.0) -------------------------------------------

def _hash_password(password: str) -> str:
    """Hash a password using SHA-256."""
    return hashlib.sha256(password.encode("utf-8")).hexdigest()


async def create_share_link(db: aiosqlite.Connection, reel_id: int, data: dict) -> dict | str | None:
    """Generate a shareable link for a reel with optional expiry and password protection."""
    # Validate reel exists
    reel = await get_job(db, reel_id)
    if not reel:
        return None

    # Only allow sharing completed reels
    if reel["status"] != "completed":
        return "not_completed"

    now = _now()
    token = secrets.token_urlsafe(32)

    expires_in_hours = data.get("expires_in_hours", 72)
    expires_at = None
    if expires_in_hours is not None:
        expires_at = (datetime.now(timezone.utc) + timedelta(hours=expires_in_hours)).isoformat()

    password_hash = None
    if data.get("password"):
        password_hash = _hash_password(data["password"])

    allow_download = data.get("allow_download", True)

    cur = await db.execute(
        """INSERT INTO share_links
           (reel_id, token, expires_at, password_hash, allow_download, created_at)
           VALUES (?, ?, ?, ?, ?, ?)""",
        (reel_id, token, expires_at, password_hash, int(allow_download), now),
    )
    await db.commit()
    return await _get_share_link_by_id(db, cur.lastrowid)


async def _get_share_link_by_id(db: aiosqlite.Connection, link_id: int) -> dict | None:
    rows = await db.execute_fetchall("SELECT * FROM share_links WHERE id = ?", (link_id,))
    if not rows:
        return None
    return _share_link_row(rows[0])


async def list_share_links(db: aiosqlite.Connection, reel_id: int) -> list[dict] | None:
    """List all share links for a reel."""
    # Validate reel exists
    reel_rows = await db.execute_fetchall("SELECT id FROM reel_jobs WHERE id = ?", (reel_id,))
    if not reel_rows:
        return None
    rows = await db.execute_fetchall(
        "SELECT * FROM share_links WHERE reel_id = ? ORDER BY created_at DESC", (reel_id,))
    return [_share_link_row(r) for r in rows]


async def get_share_link_by_token(db: aiosqlite.Connection, token: str) -> dict | None:
    rows = await db.execute_fetchall("SELECT * FROM share_links WHERE token = ?", (token,))
    if not rows:
        return None
    return _share_link_row(rows[0])


async def access_share_link(db: aiosqlite.Connection, token: str, password: str | None = None) -> dict | str | None:
    """Access a share link: check expiry, verify password, increment view_count, update last_accessed_at."""
    rows = await db.execute_fetchall("SELECT * FROM share_links WHERE token = ?", (token,))
    if not rows:
        return None

    link = rows[0]

    # Check expiry
    if link["expires_at"]:
        try:
            exp_dt = datetime.fromisoformat(link["expires_at"])
            if datetime.now(timezone.utc) > exp_dt:
                return "expired"
        except (ValueError, TypeError):
            pass

    # Verify password if set
    if link["password_hash"]:
        if not password:
            return "password_required"
        if _hash_password(password) != link["password_hash"]:
            return "invalid_password"

    # Increment view_count and update last_accessed_at
    now = _now()
    await db.execute(
        "UPDATE share_links SET view_count = view_count + 1, last_accessed_at = ? WHERE id = ?",
        (now, link["id"]))
    await db.commit()

    # Return reel info along with link data
    reel = await get_job(db, link["reel_id"])
    link_data = await _get_share_link_by_id(db, link["id"])
    return {
        "link": link_data,
        "reel": reel,
    }


async def record_download(db: aiosqlite.Connection, token: str, password: str | None = None) -> dict | str | None:
    """Record a download: check expiry, verify password, check allow_download, increment download_count."""
    rows = await db.execute_fetchall("SELECT * FROM share_links WHERE token = ?", (token,))
    if not rows:
        return None

    link = rows[0]

    # Check expiry
    if link["expires_at"]:
        try:
            exp_dt = datetime.fromisoformat(link["expires_at"])
            if datetime.now(timezone.utc) > exp_dt:
                return "expired"
        except (ValueError, TypeError):
            pass

    # Verify password if set
    if link["password_hash"]:
        if not password:
            return "password_required"
        if _hash_password(password) != link["password_hash"]:
            return "invalid_password"

    # Check if download is allowed
    if not link["allow_download"]:
        return "download_not_allowed"

    # Increment download_count and update last_accessed_at
    now = _now()
    await db.execute(
        """UPDATE share_links SET download_count = download_count + 1, last_accessed_at = ?
           WHERE id = ?""",
        (now, link["id"]))
    await db.commit()

    reel = await get_job(db, link["reel_id"])
    return {
        "download_url": reel["output_url"] if reel else None,
        "reel_id": link["reel_id"],
        "download_count": link["download_count"] + 1,
    }


async def revoke_share_link(db: aiosqlite.Connection, link_id: int) -> bool:
    """Revoke (delete) a share link."""
    cur = await db.execute("DELETE FROM share_links WHERE id = ?", (link_id,))
    await db.commit()
    return cur.rowcount > 0


async def get_share_stats(db: aiosqlite.Connection) -> dict:
    """Get aggregated share link statistics."""
    total_row = await db.execute_fetchall("SELECT COUNT(*) as c FROM share_links")
    total_links = total_row[0]["c"] if total_row else 0

    views_row = await db.execute_fetchall("SELECT COALESCE(SUM(view_count), 0) as s FROM share_links")
    total_views = views_row[0]["s"] if views_row else 0

    downloads_row = await db.execute_fetchall("SELECT COALESCE(SUM(download_count), 0) as s FROM share_links")
    total_downloads = downloads_row[0]["s"] if downloads_row else 0

    # Most shared reels (by number of share links + total views)
    most_shared_rows = await db.execute_fetchall("""
        SELECT sl.reel_id, rj.title,
               COUNT(sl.id) as link_count,
               COALESCE(SUM(sl.view_count), 0) as total_views,
               COALESCE(SUM(sl.download_count), 0) as total_downloads
        FROM share_links sl
        JOIN reel_jobs rj ON sl.reel_id = rj.id
        GROUP BY sl.reel_id
        ORDER BY total_views DESC
        LIMIT 10
    """)
    most_shared_reels = [
        {
            "reel_id": r["reel_id"],
            "title": r["title"],
            "link_count": r["link_count"],
            "total_views": r["total_views"],
            "total_downloads": r["total_downloads"],
        }
        for r in most_shared_rows
    ]

    return {
        "total_links": total_links,
        "total_views": total_views,
        "total_downloads": total_downloads,
        "most_shared_reels": most_shared_reels,
    }

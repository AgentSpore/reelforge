from __future__ import annotations
import os
from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException, Query, BackgroundTasks
from fastapi.middleware.cors import CORSMiddleware

from models import (
    CreateReelRequest, BatchCreateRequest, DuplicateReelRequest,
    ReelJob, ReelListItem,
    StyleInfo, StatsResponse, RenderLogResponse,
    BrandCreate, BrandUpdate, BrandResponse,
    DailyAnalyticsEntry,
)
from engine import (
    init_db, create_job, batch_create_jobs, list_jobs, search_jobs,
    get_job, get_render_log, process_job,
    delete_job, get_stats, STYLE_CATALOG,
    duplicate_job, get_stats_by_style, get_daily_analytics,
    create_brand, list_brands, get_brand, update_brand, delete_brand,
)

DB_PATH = os.getenv("DB_PATH", "reelforge.db")


@asynccontextmanager
async def lifespan(app: FastAPI):
    app.state.db = await init_db(DB_PATH)
    yield
    await app.state.db.close()


app = FastAPI(
    title="ReelForge",
    description="Product photo to marketing reel generator. Submit photos, choose style, get a ready-to-post reel.",
    version="0.5.0",
    lifespan=lifespan,
)
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])


@app.get("/health")
async def health():
    return {"status": "ok", "version": "0.5.0"}


# ── Brands ───────────────────────────────────────────────────────────────

@app.post("/brands", response_model=BrandResponse, status_code=201)
async def add_brand(body: BrandCreate):
    return await create_brand(app.state.db, body.model_dump())


@app.get("/brands", response_model=list[BrandResponse])
async def get_brands():
    return await list_brands(app.state.db)


@app.get("/brands/{brand_id}", response_model=BrandResponse)
async def get_brand_detail(brand_id: int):
    b = await get_brand(app.state.db, brand_id)
    if not b:
        raise HTTPException(404, "Brand not found")
    return b


@app.patch("/brands/{brand_id}", response_model=BrandResponse)
async def patch_brand(brand_id: int, body: BrandUpdate):
    b = await update_brand(app.state.db, brand_id, body.model_dump(exclude_unset=True))
    if not b:
        raise HTTPException(404, "Brand not found")
    return b


@app.delete("/brands/{brand_id}", status_code=204)
async def remove_brand(brand_id: int):
    ok = await delete_brand(app.state.db, brand_id)
    if not ok:
        raise HTTPException(404, "Brand not found")


# ── Reels ────────────────────────────────────────────────────────────────

@app.post("/reels", response_model=ReelJob, status_code=201)
async def create_reel(body: CreateReelRequest, background_tasks: BackgroundTasks):
    if body.brand_id:
        b = await get_brand(app.state.db, body.brand_id)
        if not b:
            raise HTTPException(404, "Brand not found")
    job = await create_job(app.state.db, body.model_dump())
    background_tasks.add_task(process_job, app.state.db, job["id"])
    return job


@app.post("/reels/batch", response_model=list[ReelJob], status_code=201)
async def create_reels_batch(body: BatchCreateRequest, background_tasks: BackgroundTasks):
    """Create multiple reels from the same photo set with different styles — ideal for A/B testing."""
    invalid = [s for s in body.styles if s not in STYLE_CATALOG]
    if invalid:
        raise HTTPException(422, f"Unknown styles: {', '.join(invalid)}")
    if body.brand_id:
        b = await get_brand(app.state.db, body.brand_id)
        if not b:
            raise HTTPException(404, "Brand not found")
    jobs = await batch_create_jobs(app.state.db, body.model_dump())
    for job in jobs:
        background_tasks.add_task(process_job, app.state.db, job["id"])
    return jobs


@app.get("/reels/search", response_model=list[ReelListItem])
async def search_reels(
    q: str = Query(..., min_length=1, description="Search query (matches title)"),
    limit: int = Query(50, ge=1, le=200),
):
    """Search reel jobs by title."""
    return await search_jobs(app.state.db, q, limit)


@app.get("/reels", response_model=list[ReelListItem])
async def list_reels(
    status: str | None = Query(None, description="queued | processing | completed | failed"),
    limit: int = Query(50, ge=1, le=200),
):
    return await list_jobs(app.state.db, status, limit)


# render-log, retry, duplicate BEFORE /{job_id} to ensure correct routing
@app.get("/reels/{job_id}/render-log", response_model=RenderLogResponse)
async def render_log(job_id: int):
    log = await get_render_log(app.state.db, job_id)
    if not log:
        raise HTTPException(404, "Reel job not found")
    return log


@app.post("/reels/{job_id}/retry", response_model=ReelJob)
async def retry_reel(job_id: int, background_tasks: BackgroundTasks):
    job = await get_job(app.state.db, job_id)
    if not job:
        raise HTTPException(404, "Reel job not found")
    if job["status"] not in ("failed", "queued"):
        raise HTTPException(422, "Only failed or queued jobs can be retried")
    await app.state.db.execute("UPDATE reel_jobs SET status = 'queued' WHERE id = ?", (job_id,))
    await app.state.db.commit()
    background_tasks.add_task(process_job, app.state.db, job_id)
    return await get_job(app.state.db, job_id)


@app.post("/reels/{job_id}/duplicate", response_model=ReelJob, status_code=201)
async def duplicate_reel(
    job_id: int,
    body: DuplicateReelRequest,
    background_tasks: BackgroundTasks,
):
    """Clone an existing reel job with optional field overrides — perfect for A/B testing styles."""
    result = await duplicate_job(app.state.db, job_id, body.model_dump(exclude_none=True))
    if not result:
        raise HTTPException(404, "Reel job not found")
    background_tasks.add_task(process_job, app.state.db, result["id"])
    return result


@app.get("/reels/{job_id}", response_model=ReelJob)
async def get_reel(job_id: int):
    job = await get_job(app.state.db, job_id)
    if not job:
        raise HTTPException(404, "Reel job not found")
    return job


@app.delete("/reels/{job_id}", status_code=204)
async def delete_reel(job_id: int):
    ok = await delete_job(app.state.db, job_id)
    if not ok:
        raise HTTPException(404, "Reel job not found")


@app.get("/styles", response_model=list[StyleInfo])
async def list_styles():
    return [
        StyleInfo(name=k, description=v["description"],
                  best_for=v["best_for"], transitions=v["transitions"])
        for k, v in STYLE_CATALOG.items()
    ]


# ── Analytics ────────────────────────────────────────────────────────────

@app.get("/analytics/daily", response_model=list[DailyAnalyticsEntry])
async def daily_analytics(
    days: int = Query(30, ge=1, le=365, description="Number of days to look back"),
):
    """Daily breakdown: reels created, completed, failed, top style per day."""
    return await get_daily_analytics(app.state.db, days)


@app.get("/stats/by-style")
async def stats_by_style():
    """Per-style breakdown: total jobs, completed, failed, avg duration, success rate."""
    return await get_stats_by_style(app.state.db)


@app.get("/stats", response_model=StatsResponse)
async def stats():
    return await get_stats(app.state.db)

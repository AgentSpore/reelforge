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
    ReelPresetInfo, EngagementEventCreate, EngagementSummary,
    EngagementTopReel, BrandAnalyticsEntry,
    CollectionCreate, CollectionResponse, CollectionReelAdd, CollectionAnalytics,
    ABTestCreate, ABTestResponse,
    RenderQueueItem,
)
from engine import (
    init_db, create_job, batch_create_jobs, list_jobs, search_jobs,
    get_job, get_render_log, process_job,
    delete_job, get_stats, STYLE_CATALOG, REEL_PRESETS,
    duplicate_job, get_stats_by_style, get_daily_analytics,
    create_brand, list_brands, get_brand, update_brand, delete_brand,
    record_engagement, get_reel_engagement, get_engagement_analytics,
    get_brand_analytics, VALID_ENGAGEMENT_TYPES,
    create_collection, list_collections, get_collection, delete_collection,
    add_reel_to_collection, remove_reel_from_collection, get_collection_analytics,
    create_ab_test, get_ab_test, list_ab_tests, complete_ab_test,
    get_render_queue, VALID_PRIORITIES,
)

DB_PATH = os.getenv("DB_PATH", "reelforge.db")


@asynccontextmanager
async def lifespan(app: FastAPI):
    app.state.db = await init_db(DB_PATH)
    yield
    await app.state.db.close()


app = FastAPI(
    title="ReelForge",
    description=(
        "Product photo to marketing reel generator. Submit photos, choose style, "
        "get a ready-to-post reel. Supports brand profiles, collections, A/B testing, "
        "engagement tracking, render queue with priorities, and analytics."
    ),
    version="0.7.0",
    lifespan=lifespan,
)
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])


@app.get("/health")
async def health():
    return {"status": "ok", "version": "0.7.0"}


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


# ── Collections ──────────────────────────────────────────────────────────

@app.post("/collections", response_model=CollectionResponse, status_code=201)
async def add_collection(body: CollectionCreate):
    """Create a collection to group reels for campaign tracking."""
    return await create_collection(app.state.db, body.model_dump())


@app.get("/collections", response_model=list[CollectionResponse])
async def get_collections():
    return await list_collections(app.state.db)


@app.get("/collections/{collection_id}", response_model=CollectionResponse)
async def get_collection_detail(collection_id: int):
    c = await get_collection(app.state.db, collection_id)
    if not c:
        raise HTTPException(404, "Collection not found")
    return c


@app.delete("/collections/{collection_id}", status_code=204)
async def remove_collection(collection_id: int):
    ok = await delete_collection(app.state.db, collection_id)
    if not ok:
        raise HTTPException(404, "Collection not found")


@app.post("/collections/{collection_id}/reels", response_model=CollectionResponse)
async def collection_add_reel(collection_id: int, body: CollectionReelAdd):
    """Add a reel to a collection."""
    result = await add_reel_to_collection(app.state.db, collection_id, body.reel_id)
    if result is None:
        raise HTTPException(404, "Collection not found")
    if result == "reel_not_found":
        raise HTTPException(404, "Reel not found")
    return result


@app.delete("/collections/{collection_id}/reels/{reel_id}", response_model=CollectionResponse)
async def collection_remove_reel(collection_id: int, reel_id: int):
    """Remove a reel from a collection."""
    result = await remove_reel_from_collection(app.state.db, collection_id, reel_id)
    if not result:
        raise HTTPException(404, "Collection not found")
    return result


@app.get("/collections/{collection_id}/analytics", response_model=CollectionAnalytics)
async def collection_analytics(collection_id: int):
    """Aggregated analytics for all reels in a collection."""
    result = await get_collection_analytics(app.state.db, collection_id)
    if not result:
        raise HTTPException(404, "Collection not found")
    return result


# ── A/B Tests ────────────────────────────────────────────────────────────

@app.post("/ab-tests", response_model=ABTestResponse, status_code=201)
async def add_ab_test(body: ABTestCreate):
    """Create an A/B test to compare engagement between multiple reels."""
    result = await create_ab_test(app.state.db, body.model_dump())
    if isinstance(result, str) and result.startswith("reel_not_found"):
        rid = result.split(":")[1]
        raise HTTPException(404, f"Reel {rid} not found")
    return result


@app.get("/ab-tests", response_model=list[ABTestResponse])
async def get_ab_tests():
    return await list_ab_tests(app.state.db)


@app.get("/ab-tests/{test_id}", response_model=ABTestResponse)
async def get_ab_test_detail(test_id: int):
    result = await get_ab_test(app.state.db, test_id)
    if not result:
        raise HTTPException(404, "A/B test not found")
    return result


@app.post("/ab-tests/{test_id}/complete", response_model=ABTestResponse)
async def finalize_ab_test(test_id: int):
    """Mark an A/B test as completed and lock in the winner."""
    result = await complete_ab_test(app.state.db, test_id)
    if not result:
        raise HTTPException(404, "A/B test not found")
    return result


# ── Render Queue ─────────────────────────────────────────────────────────

@app.get("/queue", response_model=list[RenderQueueItem])
async def render_queue():
    """View the current render queue ordered by priority and creation time."""
    return await get_render_queue(app.state.db)


# ── Reels ────────────────────────────────────────────────────────────────

@app.post("/reels", response_model=ReelJob, status_code=201)
async def create_reel(body: CreateReelRequest, background_tasks: BackgroundTasks):
    if body.style not in STYLE_CATALOG:
        raise HTTPException(422, f"Unknown style. Available: {', '.join(STYLE_CATALOG.keys())}")
    if body.priority not in VALID_PRIORITIES:
        raise HTTPException(422, f"Invalid priority. Must be one of: {', '.join(sorted(VALID_PRIORITIES))}")
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
    return await search_jobs(app.state.db, q, limit)


@app.get("/reels", response_model=list[ReelListItem])
async def list_reels(
    status: str | None = Query(None, description="queued | processing | completed | failed"),
    limit: int = Query(50, ge=1, le=200),
):
    return await list_jobs(app.state.db, status, limit)


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


@app.post("/reels/{job_id}/engagement", response_model=EngagementSummary)
async def log_engagement(job_id: int, body: EngagementEventCreate):
    """Record an engagement event (view, like, share, click, save) for a reel."""
    if body.event_type not in VALID_ENGAGEMENT_TYPES:
        raise HTTPException(422, f"Invalid event_type. Must be one of: {', '.join(sorted(VALID_ENGAGEMENT_TYPES))}")
    result = await record_engagement(app.state.db, job_id, body.event_type, body.source)
    if not result:
        raise HTTPException(404, "Reel job not found")
    return result


@app.get("/reels/{job_id}/engagement", response_model=EngagementSummary)
async def reel_engagement(job_id: int):
    result = await get_reel_engagement(app.state.db, job_id)
    if not result:
        raise HTTPException(404, "Reel job not found")
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


# ── Presets ──────────────────────────────────────────────────────────────

@app.get("/presets", response_model=list[ReelPresetInfo])
async def list_presets():
    return [
        ReelPresetInfo(name=k, **v)
        for k, v in REEL_PRESETS.items()
    ]


@app.get("/presets/{preset_name}", response_model=ReelPresetInfo)
async def get_preset(preset_name: str):
    if preset_name not in REEL_PRESETS:
        raise HTTPException(404, f"Preset not found. Available: {', '.join(REEL_PRESETS.keys())}")
    return ReelPresetInfo(name=preset_name, **REEL_PRESETS[preset_name])


# ── Analytics ────────────────────────────────────────────────────────────

@app.get("/analytics/daily", response_model=list[DailyAnalyticsEntry])
async def daily_analytics(
    days: int = Query(30, ge=1, le=365, description="Number of days to look back"),
):
    return await get_daily_analytics(app.state.db, days)


@app.get("/analytics/engagement", response_model=list[EngagementTopReel])
async def engagement_analytics(
    limit: int = Query(20, ge=1, le=100),
    sort_by: str = Query("engagement_rate", description="Sort by: engagement_rate | views | likes | shares | clicks | saves"),
):
    return await get_engagement_analytics(app.state.db, limit, sort_by)


@app.get("/analytics/brands", response_model=list[BrandAnalyticsEntry])
async def brand_analytics():
    return await get_brand_analytics(app.state.db)


@app.get("/stats/by-style")
async def stats_by_style():
    return await get_stats_by_style(app.state.db)


@app.get("/stats", response_model=StatsResponse)
async def stats():
    return await get_stats(app.state.db)

from __future__ import annotations
import os
from contextlib import asynccontextmanager

import asyncio
from fastapi import FastAPI, HTTPException, Query, BackgroundTasks
from fastapi.middleware.cors import CORSMiddleware

from models import CreateReelRequest, ReelJob, ReelListItem, StyleInfo, StatsResponse
from engine import init_db, create_job, list_jobs, get_job, process_job, delete_job, get_stats, STYLE_CATALOG

DB_PATH = os.getenv("DB_PATH", "reelforge.db")


@asynccontextmanager
async def lifespan(app: FastAPI):
    app.state.db = await init_db(DB_PATH)
    yield
    await app.state.db.close()


app = FastAPI(
    title="ReelForge",
    description="Product photo to marketing reel generator. Submit photos, choose style, get a ready-to-post reel.",
    version="0.1.0",
    lifespan=lifespan,
)
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])


@app.get("/health")
async def health():
    return {"status": "ok", "version": "0.1.0"}


@app.post("/reels", response_model=ReelJob, status_code=201)
async def create_reel(body: CreateReelRequest, background_tasks: BackgroundTasks):
    """
    Submit product photos and style config to create a marketing reel.
    Processing runs in background — poll GET /reels/{id} for status.
    """
    job = await create_job(app.state.db, body.model_dump())
    background_tasks.add_task(process_job, app.state.db, job["id"])
    return job


@app.get("/reels", response_model=list[ReelListItem])
async def list_reels(
    status: str | None = Query(None, description="Filter: queued | processing | completed | failed"),
    limit: int = Query(50, ge=1, le=200),
):
    """List all reel jobs, optionally filtered by status."""
    return await list_jobs(app.state.db, status, limit)


@app.get("/reels/{job_id}", response_model=ReelJob)
async def get_reel(job_id: int):
    """Get reel job details including output URL when completed."""
    job = await get_job(app.state.db, job_id)
    if not job:
        raise HTTPException(404, "Reel job not found")
    return job


@app.post("/reels/{job_id}/retry", response_model=ReelJob)
async def retry_reel(job_id: int, background_tasks: BackgroundTasks):
    """Retry a failed reel job."""
    from engine import db as _
    import aiosqlite
    job = await get_job(app.state.db, job_id)
    if not job:
        raise HTTPException(404, "Reel job not found")
    if job["status"] not in ("failed", "queued"):
        raise HTTPException(422, "Only failed or queued jobs can be retried")
    await app.state.db.execute("UPDATE reel_jobs SET status = 'queued' WHERE id = ?", (job_id,))
    await app.state.db.commit()
    background_tasks.add_task(process_job, app.state.db, job_id)
    return await get_job(app.state.db, job_id)


@app.delete("/reels/{job_id}", status_code=204)
async def delete_reel(job_id: int):
    """Delete a reel job and its associated data."""
    ok = await delete_job(app.state.db, job_id)
    if not ok:
        raise HTTPException(404, "Reel job not found")


@app.get("/styles", response_model=list[StyleInfo])
async def list_styles():
    """Available reel styles with description and best-use cases."""
    return [
        StyleInfo(name=k, description=v["description"],
                  best_for=v["best_for"], transitions=v["transitions"])
        for k, v in STYLE_CATALOG.items()
    ]


@app.get("/stats", response_model=StatsResponse)
async def stats():
    """Aggregate stats: job counts by status, avg duration, most-used style."""
    return await get_stats(app.state.db)

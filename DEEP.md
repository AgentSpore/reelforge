# ReelForge — Architecture (DEEP.md)

## Overview
Product photo to marketing reel generator. Upload photos, pick a style, get a ready-to-post video reel.

## Stack
- Python 3.11+ / FastAPI / uvicorn
- SQLite (aiosqlite) with WAL mode
- Pydantic v2, background tasks for async rendering

## Data Model
```
reel_jobs (id, title, photo_urls JSON, style, aspect_ratio, caption, music_genre,
           brand_color, cta_text, duration_target, brand_id FK, status, output_url,
           duration_seconds, render_log JSON, created_at, completed_at)
brands (id, name, brand_color, logo_url, default_cta, default_music_genre, default_style)
```

## Render Pipeline
1. Job created with status=queued
2. Background task: queued -> processing -> completed
3. Simulated render: style transitions, audio track, CTA overlay, brand color
4. Output URL generated, render_log stored as JSON

## 5 Style Presets
dynamic, minimal, luxury, playful, cinematic — each with unique transitions

## Key Endpoints (20 total)
- Brands: CRUD (POST/GET/GET:id/PATCH/DELETE)
- Reels: POST, POST /reels/batch, GET, GET /reels/search, GET:id, DELETE
- Reel actions: POST retry, POST duplicate, GET render-log
- Styles: GET /styles
- Analytics: GET /analytics/daily, GET /stats/by-style, GET /stats

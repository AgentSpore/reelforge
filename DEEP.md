# ReelForge — Architecture

## Stack
- Python 3.11+ / FastAPI / aiosqlite / Pydantic v2

## Database
- SQLite WAL mode, tables: reel_jobs, brands, engagement_events, collections, collection_reels, ab_tests, ab_test_reels
- Migrations run on startup (brand_id column, engagement table, priority column, collections, ab_tests)

## Features (v0.7.0)
1. Brands CRUD with default style/color/CTA inheritance
2. Reels CRUD + batch create + duplicate + retry + search
3. Render pipeline (simulated) with render log
4. 5 reel styles: dynamic, minimal, luxury, playful, cinematic
5. 5 reel presets: product_showcase, unboxing, before_after, tutorial, testimonial
6. Engagement tracking: view/like/share/click/save per reel with engagement rate
7. Brand analytics: per-brand performance, completion rate, avg render time
8. Daily analytics + per-style stats
9. **Reel Collections** — organize reels into campaigns, grouped analytics (completion, engagement, top style)
10. **A/B Test Comparison** — create tests comparing 2-5 reels, auto-determine winner by engagement rate
11. **Render Queue Priority** — low/normal/high/urgent priorities, ordered queue view (GET /queue)

## Endpoints (33)
- GET /health
- POST/GET /brands, GET/PATCH/DELETE /brands/{id}
- POST/GET /collections, GET/DELETE /collections/{id}
- POST /collections/{id}/reels, DELETE /collections/{id}/reels/{reel_id}
- GET /collections/{id}/analytics
- POST/GET /ab-tests, GET /ab-tests/{id}, POST /ab-tests/{id}/complete
- GET /queue
- POST /reels, POST /reels/batch, GET /reels, GET /reels/search
- GET /reels/{id}, DELETE /reels/{id}
- GET /reels/{id}/render-log, POST /reels/{id}/retry, POST /reels/{id}/duplicate
- POST /reels/{id}/engagement, GET /reels/{id}/engagement
- GET /styles, GET /presets, GET /presets/{name}
- GET /analytics/daily, GET /analytics/engagement, GET /analytics/brands
- GET /stats/by-style, GET /stats

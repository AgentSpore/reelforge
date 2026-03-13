# ReelForge — Development Log (MEMORY.md)

## v0.1.0 — Initial MVP
- 5 reel styles with simulated render pipeline
- Background async processing
- Stats dashboard

## v0.2.0 — Render Log
- GET /reels/{id}/render-log — detailed render audit
- render_log included in job response

## v0.3.0 — Duplicate & Style Stats
- POST /reels/{id}/duplicate — A/B test with overrides
- GET /stats/by-style — per-style success rate, avg duration

## v0.4.0 — Brand Profiles
- Brand CRUD with auto-fill defaults on reel creation
- brand_id field, total_brands in stats

## v0.5.0 — Batch, Analytics, Search
- POST /reels/batch — create up to 5 reels with different styles from same photos
- GET /analytics/daily?days=30 — daily creation/completion/failure trend with top style
- GET /reels/search?q= — title search for fast reel lookup
- 20 endpoints total

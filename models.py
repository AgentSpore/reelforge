from __future__ import annotations
from pydantic import BaseModel, Field
from typing import Optional


class ReelJob(BaseModel):
    id: int
    title: str
    photo_urls: list[str]
    style: str
    aspect_ratio: str
    status: str
    priority: str = "normal"
    brand_id: Optional[int] = None
    tags: list[str] = Field(default_factory=list)
    output_url: Optional[str] = None
    duration_seconds: Optional[float] = None
    render_log: Optional[dict] = None
    created_at: str
    completed_at: Optional[str] = None


class CreateReelRequest(BaseModel):
    title: str = Field(..., description="Product name or reel title")
    photo_urls: list[str] = Field(..., min_length=1, max_length=10)
    style: str = Field("dynamic", description="dynamic | minimal | luxury | playful | cinematic")
    aspect_ratio: str = Field("9:16", description="9:16 | 1:1 | 16:9")
    brand_id: Optional[int] = Field(None, description="Brand profile ID to auto-fill defaults")
    caption: Optional[str] = None
    music_genre: Optional[str] = None
    brand_color: Optional[str] = Field(None, description="Hex colour, e.g. #FF6B35")
    cta_text: Optional[str] = None
    duration_target: int = Field(15, ge=5, le=60)
    priority: str = Field("normal", description="low | normal | high | urgent")
    tags: list[str] = Field(default_factory=list, description="Tags for organizing reels")


class BatchCreateRequest(BaseModel):
    title: str = Field(..., description="Base title for all reels")
    photo_urls: list[str] = Field(..., min_length=1, max_length=10)
    styles: list[str] = Field(..., min_length=1, max_length=5, description="List of styles to generate")
    aspect_ratio: str = Field("9:16", description="9:16 | 1:1 | 16:9")
    brand_id: Optional[int] = None
    caption: Optional[str] = None
    music_genre: Optional[str] = None
    brand_color: Optional[str] = None
    cta_text: Optional[str] = None
    duration_target: int = Field(15, ge=5, le=60)
    priority: str = Field("normal", description="low | normal | high | urgent")


class DuplicateReelRequest(BaseModel):
    title: Optional[str] = Field(None, description="Override title (default: original + ' (copy)')")
    style: Optional[str] = Field(None, description="Override style for A/B testing")
    aspect_ratio: Optional[str] = None
    caption: Optional[str] = None
    music_genre: Optional[str] = None
    brand_color: Optional[str] = None
    cta_text: Optional[str] = None
    duration_target: Optional[int] = Field(None, ge=5, le=60)


class ReelListItem(BaseModel):
    id: int
    title: str
    style: str
    aspect_ratio: str
    status: str
    priority: str
    photo_count: int
    tags: list[str] = Field(default_factory=list)
    created_at: str


class BrandCreate(BaseModel):
    name: str = Field(..., description="Brand name, e.g. 'Acme Corp'")
    brand_color: Optional[str] = Field(None, description="Hex colour, e.g. #FF6B35")
    logo_url: Optional[str] = None
    default_cta: Optional[str] = None
    default_music_genre: Optional[str] = None
    default_style: str = Field("dynamic", description="Default reel style for this brand")


class BrandUpdate(BaseModel):
    name: Optional[str] = None
    brand_color: Optional[str] = None
    logo_url: Optional[str] = None
    default_cta: Optional[str] = None
    default_music_genre: Optional[str] = None
    default_style: Optional[str] = None


class BrandResponse(BaseModel):
    id: int
    name: str
    brand_color: Optional[str]
    logo_url: Optional[str]
    default_cta: Optional[str]
    default_music_genre: Optional[str]
    default_style: str
    reels_count: int
    created_at: str


class StyleInfo(BaseModel):
    name: str
    description: str
    best_for: str
    transitions: list[str]


class StatsResponse(BaseModel):
    total_jobs: int
    completed: int
    processing: int
    failed: int
    avg_duration_seconds: float
    most_used_style: Optional[str]
    most_used_ratio: Optional[str]
    total_brands: int
    total_collections: int
    total_tags: int
    total_webhooks: int
    scheduled_count: int


class RenderLogResponse(BaseModel):
    job_id: int
    status: str
    render_log: Optional[dict]
    output_url: Optional[str]
    duration_seconds: Optional[float]
    completed_at: Optional[str]


class DailyAnalyticsEntry(BaseModel):
    date: str
    created: int
    completed: int
    failed: int
    top_style: Optional[str]


# ── Reel Presets ────────────────────────────────────────────────────────────

class ReelPresetInfo(BaseModel):
    name: str
    description: str
    recommended_photos: int
    recommended_style: str
    recommended_aspect: str
    recommended_duration: int
    recommended_music: str
    scene_flow: list[str]


# ── Engagement ──────────────────────────────────────────────────────────────

class EngagementEventCreate(BaseModel):
    event_type: str = Field(..., description="view | like | share | click | save")
    source: Optional[str] = Field(None, description="Traffic source: instagram | tiktok | facebook | organic | ads")


class EngagementSummary(BaseModel):
    reel_id: int
    views: int
    likes: int
    shares: int
    clicks: int
    saves: int
    total_events: int
    engagement_rate: float = Field(..., description="(likes+shares+saves+clicks)/views * 100")


class EngagementTopReel(BaseModel):
    reel_id: int
    title: str
    style: str
    status: str
    views: int
    likes: int
    shares: int
    clicks: int
    saves: int
    total_events: int
    engagement_rate: float


# ── Brand Analytics ─────────────────────────────────────────────────────────

class BrandAnalyticsEntry(BaseModel):
    brand_id: int
    brand_name: str
    total_reels: int
    completed: int
    failed: int
    completion_rate: float
    avg_duration_seconds: float
    top_style: Optional[str]
    total_views: int
    engagement_rate: float


# ── Collections ─────────────────────────────────────────────────────────────

class CollectionCreate(BaseModel):
    name: str = Field(..., min_length=1, max_length=120)
    description: Optional[str] = Field(None, max_length=500)


class CollectionResponse(BaseModel):
    id: int
    name: str
    description: Optional[str]
    reel_count: int
    created_at: str


class CollectionReelAdd(BaseModel):
    reel_id: int


class CollectionAnalytics(BaseModel):
    collection_id: int
    collection_name: str
    total_reels: int
    completed: int
    failed: int
    processing: int
    completion_rate: float
    total_views: int
    total_likes: int
    total_shares: int
    engagement_rate: float
    top_style: Optional[str]


# ── A/B Test ────────────────────────────────────────────────────────────────

class ABTestCreate(BaseModel):
    name: str = Field(..., min_length=1, max_length=120)
    reel_ids: list[int] = Field(..., min_length=2, max_length=5, description="Reel IDs to compare")


class ABTestReelResult(BaseModel):
    reel_id: int
    title: str
    style: str
    views: int
    likes: int
    shares: int
    clicks: int
    saves: int
    engagement_rate: float
    is_winner: bool


class ABTestResponse(BaseModel):
    id: int
    name: str
    status: str  # active | completed
    reels: list[ABTestReelResult]
    winner_id: Optional[int]
    created_at: str


# ── Render Queue ────────────────────────────────────────────────────────────

class RenderQueueItem(BaseModel):
    id: int
    title: str
    style: str
    priority: str
    status: str
    created_at: str
    position: int


# ── Tags ────────────────────────────────────────────────────────────────────

class TagRequest(BaseModel):
    tag: str = Field(..., min_length=1, max_length=50)


class TagStats(BaseModel):
    tag: str
    count: int


class TagAnalytics(BaseModel):
    tag: str
    total_reels: int
    completed: int
    failed: int
    avg_engagement_rate: float
    top_style: Optional[str]


# ── Webhooks ────────────────────────────────────────────────────────────────

VALID_WEBHOOK_EVENTS = {"render_complete", "render_failed", "all"}


class WebhookCreate(BaseModel):
    url: str = Field(..., min_length=10, max_length=500, description="Callback URL")
    events: list[str] = Field(..., min_length=1, description="Events: render_complete | render_failed | all")
    secret: Optional[str] = Field(None, max_length=200, description="HMAC secret for payload signing")


class WebhookResponse(BaseModel):
    id: int
    url: str
    events: list[str]
    is_active: bool
    deliveries: int
    last_delivery_at: Optional[str]
    created_at: str


class WebhookUpdate(BaseModel):
    url: Optional[str] = Field(None, min_length=10, max_length=500)
    events: Optional[list[str]] = None
    is_active: Optional[bool] = None


# ── Scheduled Publishing ───────────────────────────────────────────────────

class ScheduleCreate(BaseModel):
    publish_at: str = Field(..., description="ISO datetime for scheduled publishing (YYYY-MM-DDTHH:MM:SS)")
    platform: str = Field("instagram", description="Target: instagram | tiktok | youtube | facebook")
    caption: Optional[str] = Field(None, max_length=2200)


class ScheduleResponse(BaseModel):
    id: int
    reel_id: int
    reel_title: str
    publish_at: str
    platform: str
    caption: Optional[str]
    status: str  # scheduled | published | cancelled
    created_at: str

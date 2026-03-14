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
    render_profile_id: Optional[int] = None
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
    render_profile_id: Optional[int] = Field(None, description="Render profile ID for quality settings")
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
    render_profile_id: Optional[int] = None
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


# -- Reel Presets ------------------------------------------------------------

class ReelPresetInfo(BaseModel):
    name: str
    description: str
    recommended_photos: int
    recommended_style: str
    recommended_aspect: str
    recommended_duration: int
    recommended_music: str
    scene_flow: list[str]


# -- Engagement --------------------------------------------------------------

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


# -- Brand Analytics ---------------------------------------------------------

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


# -- Collections -------------------------------------------------------------

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


# -- A/B Test ----------------------------------------------------------------

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


# -- Render Queue ------------------------------------------------------------

class RenderQueueItem(BaseModel):
    id: int
    title: str
    style: str
    priority: str
    status: str
    created_at: str
    position: int


# -- Tags --------------------------------------------------------------------

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


# -- Webhooks ----------------------------------------------------------------

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


# -- Scheduled Publishing ---------------------------------------------------

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


# -- Reel Templates (v0.9.0) ------------------------------------------------

VALID_TEMPLATE_CATEGORIES = {
    "product", "lifestyle", "tutorial", "testimonial",
    "announcement", "seasonal", "promotion", "general",
}


class TemplateCreate(BaseModel):
    name: str = Field(..., min_length=1, max_length=120, description="Template name")
    description: Optional[str] = Field(None, max_length=500)
    source_reel_id: Optional[int] = Field(None, description="Copy settings from an existing reel")
    category: Optional[str] = Field("general", description="Template category")
    style: Optional[str] = Field(None, description="dynamic | minimal | luxury | playful | cinematic")
    aspect_ratio: Optional[str] = Field(None, description="9:16 | 1:1 | 16:9")
    music_genre: Optional[str] = None
    brand_color: Optional[str] = Field(None, description="Hex colour, e.g. #FF6B35")
    cta_text: Optional[str] = None
    duration_target: Optional[int] = Field(None, ge=5, le=60)
    brand_id: Optional[int] = None


class TemplateUpdate(BaseModel):
    name: Optional[str] = Field(None, min_length=1, max_length=120)
    description: Optional[str] = Field(None, max_length=500)
    category: Optional[str] = None


class TemplateResponse(BaseModel):
    id: int
    name: str
    description: Optional[str]
    source_reel_id: Optional[int]
    style: str
    aspect_ratio: str
    music_genre: Optional[str]
    brand_color: Optional[str]
    cta_text: Optional[str]
    duration_target: int
    brand_id: Optional[int]
    category: str
    times_used: int
    created_at: str


class CreateFromTemplateRequest(BaseModel):
    title: str = Field(..., min_length=1, max_length=200, description="Reel title")
    photo_urls: list[str] = Field(..., min_length=1, max_length=10)
    caption: Optional[str] = None
    priority: Optional[str] = Field("normal", description="low | normal | high | urgent")
    tags: list[str] = Field(default_factory=list, description="Tags for the new reel")


# -- Reel Comments / Collaboration (v0.9.0) ---------------------------------

class CommentCreate(BaseModel):
    author: str = Field(..., min_length=1, max_length=100, description="Comment author name")
    content: str = Field(..., min_length=1, max_length=2000, description="Comment text")
    parent_id: Optional[int] = Field(None, description="Parent comment ID for threaded replies")


class CommentUpdate(BaseModel):
    content: Optional[str] = Field(None, min_length=1, max_length=2000)
    is_resolved: Optional[bool] = None


class CommentResponse(BaseModel):
    id: int
    reel_id: int
    author: str
    content: str
    is_resolved: bool
    parent_id: Optional[int]
    replies_count: int
    created_at: str
    updated_at: Optional[str]


class CommentThread(BaseModel):
    comment: CommentResponse
    replies: list[CommentResponse]


class CommentStats(BaseModel):
    total_comments: int
    resolved: int
    unresolved: int
    top_commenters: list[dict]


# -- Export & Share Links (v0.9.0) -------------------------------------------

class ShareLinkCreate(BaseModel):
    expires_in_hours: Optional[int] = Field(72, ge=1, le=8760, description="Hours until link expires (default 72)")
    password: Optional[str] = Field(None, min_length=4, max_length=100, description="Optional password protection")
    allow_download: Optional[bool] = Field(True, description="Allow downloading the reel")


class ShareLinkResponse(BaseModel):
    id: int
    reel_id: int
    token: str
    expires_at: Optional[str]
    allow_download: bool
    view_count: int
    download_count: int
    is_expired: bool
    share_url: str
    created_at: str
    last_accessed_at: Optional[str]


class ShareLinkAccess(BaseModel):
    password: Optional[str] = Field(None, description="Password if link is protected")


class ShareLinkStats(BaseModel):
    total_links: int
    total_views: int
    total_downloads: int
    most_shared_reels: list[dict]


# -- Render Profiles (v1.0.0) -----------------------------------------------

VALID_RESOLUTIONS = {"720p", "1080p", "1440p", "4k"}
VALID_FPS = {24, 30, 60}
VALID_CODECS = {"h264", "h265", "vp9", "av1"}
VALID_QUALITY_PRESETS = {"draft", "balanced", "high", "ultra"}


class RenderProfileCreate(BaseModel):
    name: str = Field(..., min_length=1, max_length=100)
    resolution: str = Field("1080p", description="720p | 1080p | 1440p | 4k")
    fps: int = Field(30, description="24 | 30 | 60")
    codec: str = Field("h264", description="h264 | h265 | vp9 | av1")
    bitrate_kbps: int = Field(5000, ge=1000, le=50000)
    quality_preset: str = Field("balanced", description="draft | balanced | high | ultra")
    description: Optional[str] = None


class RenderProfileUpdate(BaseModel):
    name: Optional[str] = Field(None, min_length=1, max_length=100)
    resolution: Optional[str] = None
    fps: Optional[int] = None
    codec: Optional[str] = None
    bitrate_kbps: Optional[int] = Field(None, ge=1000, le=50000)
    quality_preset: Optional[str] = None
    description: Optional[str] = None


class RenderProfileResponse(BaseModel):
    id: int
    name: str
    resolution: str
    fps: int
    codec: str
    bitrate_kbps: int
    quality_preset: str
    description: Optional[str]
    created_at: str


# -- Reel Versioning (v1.0.0) -----------------------------------------------

class ReelVersionResponse(BaseModel):
    id: int
    reel_id: int
    version_number: int
    title: str
    style: str
    photo_urls: str  # JSON
    render_settings: str  # JSON (resolution, fps, etc.)
    output_url: Optional[str]
    created_at: str
    note: Optional[str]


# -- Content Calendar (v1.0.0) ----------------------------------------------

VALID_CALENDAR_STATUSES = {"planned", "assigned", "published", "skipped"}


class CalendarSlotCreate(BaseModel):
    title: str = Field(..., min_length=1, max_length=200)
    planned_date: str = Field(..., description="ISO date YYYY-MM-DD")
    platform: str = Field(..., description="instagram | tiktok | youtube | facebook")
    reel_id: Optional[int] = None
    notes: Optional[str] = None
    status: str = Field("planned", description="planned | assigned | published | skipped")


class CalendarSlotUpdate(BaseModel):
    title: Optional[str] = Field(None, min_length=1, max_length=200)
    planned_date: Optional[str] = None
    platform: Optional[str] = None
    reel_id: Optional[int] = None
    notes: Optional[str] = None
    status: Optional[str] = None


class CalendarSlotResponse(BaseModel):
    id: int
    title: str
    planned_date: str
    platform: str
    reel_id: Optional[int]
    notes: Optional[str]
    status: str
    created_at: str
    updated_at: Optional[str]


class CalendarOverview(BaseModel):
    month: str
    total_slots: int
    slots_per_platform: dict
    slots_per_day: dict
    gap_days: list[str]
    total_days: int
    coverage_pct: float


class CalendarGap(BaseModel):
    date: str
    missing_platforms: list[str]


# -- Watermark Management (v1.1.0) ------------------------------------------

VALID_WATERMARK_TYPES = {"text", "image"}
VALID_WATERMARK_POSITIONS = {"top_left", "top_right", "bottom_left", "bottom_right", "center"}


class WatermarkCreate(BaseModel):
    name: str
    watermark_type: str  # text or image
    content: str  # text string or image URL
    position: str = "bottom_right"
    opacity: float = Field(default=0.7, ge=0.1, le=1.0)
    scale: float = Field(default=1.0, ge=0.1, le=3.0)
    brand_id: Optional[int] = None


class WatermarkUpdate(BaseModel):
    name: Optional[str] = None
    content: Optional[str] = None
    position: Optional[str] = None
    opacity: Optional[float] = Field(default=None, ge=0.1, le=1.0)
    scale: Optional[float] = Field(default=None, ge=0.1, le=3.0)
    brand_id: Optional[int] = None


class WatermarkResponse(BaseModel):
    id: int
    name: str
    watermark_type: str
    content: str
    position: str
    opacity: float
    scale: float
    brand_id: Optional[int]
    times_applied: int
    created_at: str
    updated_at: str


# -- Reel Analytics Funnels (v1.1.0) ----------------------------------------

class FunnelCreate(BaseModel):
    name: str
    steps: list[str] = Field(default=["view", "like", "share", "click"])  # ordered event types
    description: Optional[str] = None


class FunnelResponse(BaseModel):
    id: int
    name: str
    steps: list[str]
    description: Optional[str]
    created_at: str


class FunnelStepMetric(BaseModel):
    step: str
    count: int
    drop_off_pct: float


class FunnelAnalysis(BaseModel):
    funnel_id: int
    funnel_name: str
    steps: list[FunnelStepMetric]
    overall_conversion_pct: float
    total_entry: int
    total_exit: int


class FunnelComparison(BaseModel):
    funnels: list[FunnelAnalysis]


# -- Asset Library (v1.1.0) -------------------------------------------------

VALID_ASSET_TYPES = {"photo", "music", "overlay", "background", "logo", "font"}


class AssetCreate(BaseModel):
    name: str
    asset_type: str  # photo, music, overlay, background, logo, font
    url: str
    thumbnail_url: Optional[str] = None
    file_size_kb: Optional[int] = None
    tags: Optional[list[str]] = None
    brand_id: Optional[int] = None


class AssetUpdate(BaseModel):
    name: Optional[str] = None
    url: Optional[str] = None
    thumbnail_url: Optional[str] = None
    tags: Optional[list[str]] = None


class AssetResponse(BaseModel):
    id: int
    name: str
    asset_type: str
    url: str
    thumbnail_url: Optional[str]
    file_size_kb: Optional[int]
    tags: list[str]
    brand_id: Optional[int]
    times_used: int
    created_at: str
    updated_at: str


class AssetUsageResponse(BaseModel):
    asset_id: int
    reel_id: int
    used_at: str


class AssetStats(BaseModel):
    total_assets: int
    by_type: dict
    total_usage: int
    top_assets: list[dict]

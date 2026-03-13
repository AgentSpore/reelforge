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
    brand_id: Optional[int] = None
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
    photo_count: int
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

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
    caption: Optional[str] = None
    music_genre: Optional[str] = None
    brand_color: Optional[str] = Field(None, description="Hex colour, e.g. #FF6B35")
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


class RenderLogResponse(BaseModel):
    job_id: int
    status: str
    render_log: Optional[dict]
    output_url: Optional[str]
    duration_seconds: Optional[float]
    completed_at: Optional[str]

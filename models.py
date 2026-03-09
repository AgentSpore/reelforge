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
    created_at: str
    completed_at: Optional[str] = None


class CreateReelRequest(BaseModel):
    title: str = Field(..., description="Product name or reel title")
    photo_urls: list[str] = Field(..., min_length=1, max_length=10, description="URLs of product photos to include")
    style: str = Field("dynamic", description="Style: dynamic | minimal | luxury | playful | cinematic")
    aspect_ratio: str = Field("9:16", description="Output ratio: 9:16 (TikTok/Reels) | 1:1 (Instagram) | 16:9 (YouTube)")
    caption: Optional[str] = Field(None, description="Optional caption text to overlay")
    music_genre: Optional[str] = Field(None, description="Background music genre: upbeat | ambient | dramatic | none")
    brand_color: Optional[str] = Field(None, description="Hex colour for brand overlay (e.g. #FF6B35)")
    cta_text: Optional[str] = Field(None, description="Call-to-action text (e.g. Shop Now)")
    duration_target: int = Field(15, ge=5, le=60, description="Target reel duration in seconds")


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

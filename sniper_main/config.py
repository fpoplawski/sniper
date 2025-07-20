from __future__ import annotations

from functools import lru_cache
from typing import List

from dotenv import load_dotenv
from pydantic import Field, field_validator
from pydantic_settings import BaseSettings

load_dotenv()


class Settings(BaseSettings):
    """Application settings loaded from environment variables."""

    telegram_token: str = Field(..., alias="TELEGRAM_BOT_TOKEN")
    poll_interval_h: int = Field(..., alias="POLL_INTERVAL_H")
    airports: List[str] = Field(default_factory=list, alias="AIRPORTS")

    @field_validator("telegram_token")
    @classmethod
    def _token_non_empty(cls, v: str) -> str:
        if not v or not v.strip():
            raise ValueError("TELEGRAM_BOT_TOKEN must be a non-empty string")
        return v

    @field_validator("poll_interval_h")
    @classmethod
    def _poll_positive(cls, v: int) -> int:
        if v <= 0:
            raise ValueError("POLL_INTERVAL_H must be greater than 0")
        return v

    @field_validator("airports", mode="before")
    @classmethod
    def _split_airports(cls, v):
        if isinstance(v, str):
            return [a.strip() for a in v.split(",") if a.strip()]
        return v


@lru_cache()
def get_settings() -> Settings:
    """Return application settings loaded from the environment."""
    return Settings()  # type: ignore[call-arg]


__all__ = ["Settings", "get_settings"]

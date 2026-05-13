"""Theme configuration — schema, YAML loader, and content-addressable hash.

A theme is the unit of narrative tracking. One YAML file per theme,
loaded once and validated up front. The brief specifies a fixed shape
(see themes/us_iran_escalation.yaml for the canonical example).

Two layers of validation live here:

  - Pydantic shape: required fields, enums, numeric ranges.
  - Domain checks: every keyword pattern must compile as a regex; the
    file stem must match `theme_id`; tracked-entity buckets must not be
    all-empty (a theme that tracks no entities can never produce a
    grounded narrative).

The `config_hash()` method produces a stable digest of the validated
model — not the raw YAML bytes — so line-ending differences between
Windows dev and the macOS deploy target do not register as config drift.
"""

from __future__ import annotations

import hashlib
import json
import re
from datetime import date
from pathlib import Path
from typing import Literal

import yaml
from pydantic import BaseModel, ConfigDict, Field, HttpUrl, field_validator, model_validator


# ---------- enums / constants ----------

ThemeStatus = Literal["active", "paused", "archived"]
AlertChannel = Literal["telegram", "abelard_queue"]

THEME_ID_RE = re.compile(r"^[a-z][a-z0-9_]*$")
FEED_ID_RE = re.compile(r"^[a-z0-9_-]{1,64}$")
# Telegram channel usernames: 5–32 chars, must start with a letter,
# alphanumerics + underscores. Mirrors Telegram's own constraint.
TELEGRAM_USERNAME_RE = re.compile(r"^[a-zA-Z][a-zA-Z0-9_]{4,31}$")


# ---------- nested models ----------


class Keywords(BaseModel):
    model_config = ConfigDict(extra="forbid")

    primary: list[str] = Field(default_factory=list)
    secondary: list[str] = Field(default_factory=list)
    exclusions: list[str] = Field(default_factory=list)

    @field_validator("primary", "secondary", "exclusions")
    @classmethod
    def _patterns_compile(cls, patterns: list[str]) -> list[str]:
        for p in patterns:
            if not isinstance(p, str) or not p.strip():
                raise ValueError("keyword pattern must be a non-empty string")
            try:
                re.compile(p)
            except re.error as exc:
                raise ValueError(f"invalid regex pattern {p!r}: {exc}") from exc
        return patterns

    @model_validator(mode="after")
    def _at_least_one_primary(self) -> "Keywords":
        if not self.primary:
            raise ValueError(
                "keywords.primary must contain at least one pattern — a theme "
                "with no primary matchers can never tag a headline"
            )
        return self


class TrackedEntities(BaseModel):
    model_config = ConfigDict(extra="forbid")

    tickers: list[str] = Field(default_factory=list)
    companies: list[str] = Field(default_factory=list)
    countries: list[str] = Field(default_factory=list)
    commodities: list[str] = Field(default_factory=list)
    people: list[str] = Field(default_factory=list)

    @model_validator(mode="after")
    def _at_least_one_bucket_populated(self) -> "TrackedEntities":
        if not any([self.tickers, self.companies, self.countries, self.commodities, self.people]):
            raise ValueError(
                "tracked_entities must populate at least one of "
                "tickers/companies/countries/commodities/people"
            )
        return self


class Synthesis(BaseModel):
    model_config = ConfigDict(extra="forbid")

    cadence_hours: int = Field(default=4, gt=0)
    min_headlines_to_synthesize: int = Field(default=5, gt=0)
    model: str = Field(default="claude-sonnet-4-7", min_length=1)
    max_tokens_output: int = Field(default=1000, gt=0)


class RssFeedConfig(BaseModel):
    """One RSS / Atom feed declared by a theme.

    The scrape orchestrator deduplicates `RssSource` instances across
    themes by `feed_id` (or the derived slug when `feed_id` is None).
    """

    model_config = ConfigDict(extra="forbid")

    url: HttpUrl
    feed_id: str | None = None
    enabled: bool = True

    @field_validator("feed_id")
    @classmethod
    def _feed_id_format(cls, v: str | None) -> str | None:
        if v is None:
            return v
        if not FEED_ID_RE.match(v):
            raise ValueError(
                f"feed_id must match {FEED_ID_RE.pattern} (lowercase letters, "
                f"digits, underscores, hyphens; 1–64 chars); got {v!r}"
            )
        return v


class TelegramChannelConfig(BaseModel):
    """One Telegram channel monitored by a theme.

    `username` is stored without the `@` prefix. `cadence_minutes` is
    the minimum delay between fetches for *this theme's* registration;
    if the same channel is referenced by multiple themes, the scrape
    factory deduplicates and uses the lowest cadence across all
    referencing themes (so a high-priority theme can override a
    low-priority one's polite cadence).
    """

    model_config = ConfigDict(extra="forbid")

    username: str
    cadence_minutes: int = Field(default=15, gt=0)
    enabled: bool = True

    @field_validator("username")
    @classmethod
    def _username_format(cls, v: str) -> str:
        if not TELEGRAM_USERNAME_RE.match(v):
            raise ValueError(
                f"username must match {TELEGRAM_USERNAME_RE.pattern} "
                f"(Telegram's 5–32 char username constraint); got {v!r}"
            )
        return v


class Alerts(BaseModel):
    model_config = ConfigDict(extra="forbid")

    velocity_baseline_headlines_per_day: float = Field(gt=0)
    velocity_spike_multiplier: float = Field(default=3.0, gt=0)
    on_narrative_shift: bool = True
    on_new_counter_evidence: bool = True
    alert_channels: list[AlertChannel] = Field(default_factory=list)

    @field_validator("alert_channels")
    @classmethod
    def _channels_unique(cls, channels: list[str]) -> list[str]:
        if len(set(channels)) != len(channels):
            raise ValueError("alert_channels must not contain duplicates")
        return channels


# ---------- top-level model ----------


class ThemeConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    theme_id: str
    display_name: str = Field(min_length=1)
    status: ThemeStatus
    created_at: date

    brief: str = Field(min_length=1)

    keywords: Keywords
    tracked_entities: TrackedEntities
    synthesis: Synthesis = Field(default_factory=Synthesis)
    alerts: Alerts
    rss_feeds: list[RssFeedConfig] = Field(default_factory=list)
    telegram_channels: list[TelegramChannelConfig] = Field(default_factory=list)

    @field_validator("theme_id")
    @classmethod
    def _theme_id_format(cls, v: str) -> str:
        if not THEME_ID_RE.match(v):
            raise ValueError(
                f"theme_id must be snake_case (lowercase, digits, underscores; "
                f"starts with a letter); got {v!r}"
            )
        return v

    @field_validator("rss_feeds")
    @classmethod
    def _rss_urls_unique(cls, feeds: list[RssFeedConfig]) -> list[RssFeedConfig]:
        urls = [str(f.url) for f in feeds]
        if len(set(urls)) != len(urls):
            raise ValueError("rss_feeds must not contain duplicate URLs within a single theme")
        return feeds

    @field_validator("telegram_channels")
    @classmethod
    def _telegram_usernames_unique(
        cls, channels: list[TelegramChannelConfig]
    ) -> list[TelegramChannelConfig]:
        usernames = [c.username for c in channels]
        if len(set(usernames)) != len(usernames):
            raise ValueError(
                "telegram_channels must not contain duplicate usernames within a single theme"
            )
        return channels

    def config_hash(self) -> str:
        """SHA256 of canonical JSON dump. Stable across OSes / line endings."""
        canonical = json.dumps(
            self.model_dump(mode="json"),
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=False,
        )
        return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


# ---------- YAML loader ----------


class ThemeLoadError(RuntimeError):
    """Raised when a theme YAML cannot be parsed or validated."""


def load_theme(path: Path) -> ThemeConfig:
    """Load and validate a single theme YAML file.

    Enforces:
      - file exists and is readable as YAML
      - YAML root is a mapping
      - shape passes Pydantic validation
      - `theme_id` matches the file stem (filename without `.yaml`)
    """
    if not path.is_file():
        raise ThemeLoadError(f"theme file not found: {path}")
    try:
        raw = yaml.safe_load(path.read_text(encoding="utf-8"))
    except yaml.YAMLError as exc:
        raise ThemeLoadError(f"invalid YAML in {path}: {exc}") from exc
    if not isinstance(raw, dict):
        raise ThemeLoadError(f"theme YAML root must be a mapping in {path}")

    try:
        theme = ThemeConfig.model_validate(raw)
    except Exception as exc:
        raise ThemeLoadError(f"theme validation failed for {path}: {exc}") from exc

    if theme.theme_id != path.stem:
        raise ThemeLoadError(
            f"theme_id {theme.theme_id!r} does not match filename stem "
            f"{path.stem!r} (file: {path})"
        )
    return theme


def load_all_themes(themes_dir: Path) -> list[ThemeConfig]:
    """Load every `*.yaml` file in `themes_dir`, sorted by theme_id."""
    if not themes_dir.is_dir():
        raise ThemeLoadError(f"themes directory not found: {themes_dir}")
    themes = [load_theme(p) for p in sorted(themes_dir.glob("*.yaml"))]
    ids = [t.theme_id for t in themes]
    if len(set(ids)) != len(ids):
        raise ThemeLoadError(f"duplicate theme_ids in {themes_dir}: {ids}")
    return themes

"""Synthesis-layer configuration loaded from a YAML file.

Distinct from `news_watch_daemon.config.Config` (which holds env-var
loaded daemon-level paths/keys). This file is the operator-tunable
behavior surface: thresholds, model choices, phrase lists.

Mando edits the file in the monorepo or on the deploy target; the
orchestrator reads it once at synthesis time.

Sections land incrementally across Pass C steps. To stay forward-
compatible with the bundled YAML across the build sequence, the
top-level model uses `extra="ignore"` — sections not yet modeled
don't break validation. Sub-models enforce `extra="forbid"` internally
so typos within a known section are caught.
"""

from __future__ import annotations

from pathlib import Path
from typing import Optional

import yaml
from pydantic import BaseModel, ConfigDict, Field, field_validator


# ---------- trigger_gate (Pass C Step 4) ----------


class TriggerGateConfig(BaseModel):
    """Trigger-gate thresholds and phrase list.

    Three signals (in evaluation order):
      1. cross-theme — a single headline tagged to ≥2 themes
      2. high-signal phrase — headline contains any phrase from the list
      3. delta threshold — N or more new tagged headlines for one theme
    """

    model_config = ConfigDict(extra="forbid")

    delta_threshold_default: int = Field(default=3, gt=0)
    delta_threshold_overrides: dict[str, int] = Field(default_factory=dict)
    high_signal_phrases: list[str] = Field(default_factory=list)
    cross_theme_always_triggers: bool = True

    @field_validator("delta_threshold_overrides")
    @classmethod
    def _positive_overrides(cls, v: dict[str, int]) -> dict[str, int]:
        for theme_id, threshold in v.items():
            if not isinstance(threshold, int) or isinstance(threshold, bool) or threshold <= 0:
                raise ValueError(
                    f"delta_threshold_overrides[{theme_id!r}] must be a positive int; got {threshold!r}"
                )
        return v

    @field_validator("high_signal_phrases")
    @classmethod
    def _phrases_non_empty_strings(cls, v: list[str]) -> list[str]:
        for phrase in v:
            if not isinstance(phrase, str) or not phrase.strip():
                raise ValueError(f"high_signal_phrases entries must be non-empty strings; got {phrase!r}")
        return v


# ---------- alert_sink (Pass C Step 6) ----------


class SignalSinkConfig(BaseModel):
    """SignalSink-specific configuration.

    `destination` is the literal string the destination-validation gate
    (`_assert_destination_allowed` in alert/signal_sink.py) compares
    against. Currently only "note_to_self" is supported; any other
    value will be refused by the gate.

    `cli_path` is the signal-cli executable. Absolute path on the
    Mac mini deploy target; bare "signal-cli" works if it's on PATH.

    `timeout_s` caps each signal-cli subprocess invocation.
    """

    model_config = ConfigDict(extra="forbid")

    destination: str = "note_to_self"
    cli_path: str = "signal-cli"
    timeout_s: float = Field(default=30.0, gt=0)


class TelegramBotSinkConfig(BaseModel):
    """TelegramBotSink configuration.

    `bot_token_env` and `chat_id_env` are env-var NAMES, not values.
    The factory resolves them at construction time. Storing names in
    config (rather than values) keeps the actual token/id out of the
    YAML and lets the env-var redaction filter scrub them from logs.

    `timeout_s` caps each HTTPS request to the Bot API.
    """

    model_config = ConfigDict(extra="forbid")

    bot_token_env: str = "NWD_TG_BOT_TOKEN"
    chat_id_env: str = "NWD_TG_BOT_CHAT_ID"
    timeout_s: float = Field(default=30.0, gt=0)


class AlertSinkConfig(BaseModel):
    """Top-level alert-sink configuration.

    `type` selects which sink the factory instantiates: "signal" or
    "telegram_bot".
    """

    model_config = ConfigDict(extra="forbid")

    type: str = "signal"
    signal: SignalSinkConfig = Field(default_factory=SignalSinkConfig)
    telegram_bot: TelegramBotSinkConfig = Field(default_factory=TelegramBotSinkConfig)


# ---------- synthesis (Pass C Step 8 — materiality + Step 9 — model) ----------


class SynthesisConfig(BaseModel):
    """Synthesis orchestrator settings.

    `materiality_threshold` is the gate's pass/fail line: events whose
    score is below this are filtered out. PLACEHOLDER 0.55 — §14
    calibration will tune.

    `dedup_window_hours` is how far back the materiality gate scans the
    Brief archive for duplicate-event fingerprints. Per §9 default 6.

    `max_events_per_brief` caps the events the synthesis prompt is
    allowed to emit; prevents Sonnet from over-listing.

    `default_model` is the model ID for the synthesis call (Step 9).
    Per-theme override via each theme YAML's `synthesis.model` field.
    """

    model_config = ConfigDict(extra="forbid")

    default_model: str = "claude-sonnet-4-6"
    materiality_threshold: float = Field(default=0.55, ge=0.0, le=1.0)
    dedup_window_hours: int = Field(default=6, gt=0)
    max_events_per_brief: int = Field(default=8, gt=0)


# ---------- drift_watcher (Pass C Step 10) ----------


class DriftWatcherConfig(BaseModel):
    """Drift-watcher (Haiku) settings.

    `default_model` is the Anthropic model ID for the drift call. Haiku
    is cheap + fast — drift detection is lower-judgment than synthesis
    but still benefits from adaptive thinking on individual proposals.

    `max_proposals_per_batch` caps what Haiku may emit per run.
    Prevents flood-of-proposals failure mode and keeps the
    `proposals approve|reject` review queue manageable.

    `min_evidence_count` is a defensive floor — the prompt also asks
    for `>=3`; this is the orchestrator-side filter so a misbehaving
    Haiku response can't slip a singleton proposal through.
    """

    model_config = ConfigDict(extra="forbid")

    default_model: str = "claude-haiku-4-5"
    max_proposals_per_batch: int = Field(default=8, gt=0)
    min_evidence_count: int = Field(default=3, gt=0)


# ---------- top-level synthesis config ----------


class SynthesisDaemonConfig(BaseModel):
    """Top-level shape of synthesis_config.yaml.

    `extra="ignore"` allows the bundled YAML to declare sections that
    later Pass C steps will model. Once a section is modeled, its
    sub-model enforces `extra="forbid"` to catch typos.

    All known sections at Pass C close:
      - trigger_gate     (Step 4)
      - alert_sink       (Step 6/7)
      - synthesis        (Step 8/9)
      - drift_watcher    (Step 10)
    """

    model_config = ConfigDict(extra="ignore")

    trigger_gate: TriggerGateConfig = Field(default_factory=TriggerGateConfig)
    alert_sink: AlertSinkConfig = Field(default_factory=AlertSinkConfig)
    synthesis: SynthesisConfig = Field(default_factory=SynthesisConfig)
    drift_watcher: DriftWatcherConfig = Field(default_factory=DriftWatcherConfig)


class SynthesisConfigError(RuntimeError):
    """Raised when synthesis_config.yaml cannot be loaded or validated."""


def load_synthesis_config(path: Path) -> SynthesisDaemonConfig:
    """Load and validate the synthesis config YAML. Fail loud on errors."""
    if not isinstance(path, Path):
        path = Path(path)
    if not path.is_file():
        raise SynthesisConfigError(f"synthesis_config not found: {path}")
    try:
        raw = yaml.safe_load(path.read_text(encoding="utf-8"))
    except yaml.YAMLError as exc:
        raise SynthesisConfigError(f"invalid YAML in {path}: {exc}") from exc
    if raw is None:
        raw = {}
    if not isinstance(raw, dict):
        raise SynthesisConfigError(
            f"synthesis_config root must be a mapping in {path}; got {type(raw).__name__}"
        )
    try:
        return SynthesisDaemonConfig.model_validate(raw)
    except Exception as exc:  # noqa: BLE001 — pydantic validation surface
        raise SynthesisConfigError(
            f"synthesis_config validation failed for {path}: {exc}"
        ) from exc


__all__ = [
    "AlertSinkConfig",
    "DriftWatcherConfig",
    "SignalSinkConfig",
    "SynthesisConfig",
    "SynthesisConfigError",
    "SynthesisDaemonConfig",
    "TelegramBotSinkConfig",
    "TriggerGateConfig",
    "load_synthesis_config",
]

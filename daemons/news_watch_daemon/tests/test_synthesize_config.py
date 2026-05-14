"""SynthesisDaemonConfig + TriggerGateConfig loader tests."""

from __future__ import annotations

from pathlib import Path

import pytest
import yaml

from news_watch_daemon.synthesize.config import (
    DriftWatcherConfig,
    SynthesisConfigError,
    SynthesisDaemonConfig,
    TriggerGateConfig,
    load_synthesis_config,
)


def _write_yaml(tmp_path: Path, payload: dict, name: str = "synth.yaml") -> Path:
    p = tmp_path / name
    p.write_text(yaml.safe_dump(payload), encoding="utf-8")
    return p


# ---------- TriggerGateConfig validation ----------


def test_trigger_gate_defaults():
    cfg = TriggerGateConfig()
    assert cfg.delta_threshold_default == 3
    assert cfg.delta_threshold_overrides == {}
    assert cfg.high_signal_phrases == []
    assert cfg.cross_theme_always_triggers is True


def test_trigger_gate_zero_threshold_rejected():
    with pytest.raises(Exception):
        TriggerGateConfig(delta_threshold_default=0)


def test_trigger_gate_negative_override_rejected():
    with pytest.raises(Exception, match="positive"):
        TriggerGateConfig(delta_threshold_overrides={"theme_a": -1})


def test_trigger_gate_empty_phrase_rejected():
    with pytest.raises(Exception, match="non-empty"):
        TriggerGateConfig(high_signal_phrases=["", "valid"])


def test_trigger_gate_extra_field_forbidden():
    with pytest.raises(Exception):
        TriggerGateConfig.model_validate({"surprise": 1})


# ---------- DriftWatcherConfig validation (Step 10) ----------


def test_drift_watcher_defaults():
    cfg = DriftWatcherConfig()
    assert cfg.default_model == "claude-haiku-4-5"
    assert cfg.max_proposals_per_batch == 8
    assert cfg.min_evidence_count == 3


def test_drift_watcher_zero_max_proposals_rejected():
    with pytest.raises(Exception):
        DriftWatcherConfig(max_proposals_per_batch=0)


def test_drift_watcher_zero_min_evidence_rejected():
    with pytest.raises(Exception):
        DriftWatcherConfig(min_evidence_count=0)


def test_drift_watcher_extra_field_forbidden():
    with pytest.raises(Exception):
        DriftWatcherConfig.model_validate({"surprise": 1})


def test_drift_watcher_typo_in_yaml_fails(tmp_path):
    """Sub-models keep extra='forbid' — typos within drift_watcher fail loud."""
    p = _write_yaml(tmp_path, {
        "drift_watcher": {"default_modle": "claude-haiku-4-5"},  # typo
    })
    with pytest.raises(SynthesisConfigError):
        load_synthesis_config(p)


# ---------- load_synthesis_config ----------


def test_load_full_synthesis_yaml(tmp_path):
    p = _write_yaml(tmp_path, {
        "trigger_gate": {
            "delta_threshold_default": 5,
            "delta_threshold_overrides": {"theme_a": 7},
            "high_signal_phrases": ["new sanctions"],
            "cross_theme_always_triggers": False,
        },
    })
    cfg = load_synthesis_config(p)
    assert cfg.trigger_gate.delta_threshold_default == 5
    assert cfg.trigger_gate.delta_threshold_overrides == {"theme_a": 7}
    assert cfg.trigger_gate.high_signal_phrases == ["new sanctions"]
    assert cfg.trigger_gate.cross_theme_always_triggers is False


def test_load_missing_file_fails_loud(tmp_path):
    with pytest.raises(SynthesisConfigError, match="not found"):
        load_synthesis_config(tmp_path / "missing.yaml")


def test_load_invalid_yaml_fails_loud(tmp_path):
    bad = tmp_path / "bad.yaml"
    bad.write_text("not: valid: yaml: :", encoding="utf-8")
    with pytest.raises(SynthesisConfigError, match="invalid YAML"):
        load_synthesis_config(bad)


def test_load_root_not_mapping_fails_loud(tmp_path):
    bad = tmp_path / "list.yaml"
    bad.write_text("- a\n- b\n", encoding="utf-8")
    with pytest.raises(SynthesisConfigError, match="must be a mapping"):
        load_synthesis_config(bad)


def test_load_empty_yaml_uses_defaults(tmp_path):
    empty = tmp_path / "empty.yaml"
    empty.write_text("", encoding="utf-8")
    cfg = load_synthesis_config(empty)
    assert cfg.trigger_gate.delta_threshold_default == 3


def test_unknown_top_level_section_ignored(tmp_path):
    """Forward-compat: synthesis_config.yaml can declare sections that
    later Pass C steps (or future post-Pass-C calibration steps) will
    model. Top-level extra='ignore' tolerates them; once a section is
    modeled, sub-models tighten to 'forbid'.

    drift_watcher modeled at Step 10 — use a synthetic future section
    here so the forward-compat property keeps a probe even after every
    current Pass C section has been modeled.
    """
    p = _write_yaml(tmp_path, {
        "trigger_gate": {"delta_threshold_default": 4},
        "future_calibration_section": {"some_field": 1},
    })
    cfg = load_synthesis_config(p)
    assert cfg.trigger_gate.delta_threshold_default == 4


def test_unknown_field_in_known_section_fails(tmp_path):
    """Sub-models keep extra='forbid' — typos within trigger_gate fail loud."""
    p = _write_yaml(tmp_path, {
        "trigger_gate": {"delta_threshhold_default": 3},  # typo
    })
    with pytest.raises(SynthesisConfigError):
        load_synthesis_config(p)


def test_bundled_synthesis_config_loads_cleanly():
    """The bundled config/synthesis_config.yaml must load + validate."""
    path = Path(__file__).resolve().parent.parent / "config" / "synthesis_config.yaml"
    cfg = load_synthesis_config(path)
    assert cfg.trigger_gate.delta_threshold_default == 3
    assert cfg.trigger_gate.delta_threshold_overrides == {"us_iran_escalation": 5}
    assert "ceasefire" in cfg.trigger_gate.high_signal_phrases
    assert cfg.trigger_gate.cross_theme_always_triggers is True
    # drift_watcher (Step 10): bundled defaults should match the
    # Pydantic class defaults.
    assert cfg.drift_watcher.default_model == "claude-haiku-4-5"
    assert cfg.drift_watcher.max_proposals_per_batch == 8
    assert cfg.drift_watcher.min_evidence_count == 3

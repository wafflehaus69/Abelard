"""Config loading/validation: fail loudly, reject typos, overlay env secrets."""

from __future__ import annotations

import pytest
import yaml

from consensus.config import Config, load_config
from consensus.errors import ConfigError
from tests.conftest import BASE_CONFIG


def test_valid_config_validates():
    cfg = Config.model_validate(BASE_CONFIG)
    assert cfg.categories.targets == ["geopolitics"]
    assert cfg.data_layer.http.max_retries == 2


def test_m10_tier_thresholds_requires_all_keys():
    """Review 2026-07-20: a partial tier_thresholds override must fail at LOAD,
    not KeyError mid-scan (config doctrine: a bad knob is a startup error)."""
    from consensus.config import M10Config
    from pydantic import ValidationError

    M10Config(tier_thresholds={"WATCH": 0.3, "ELEVATED": 0.5, "CRITICAL": 0.7})  # ok
    M10Config()  # defaults are complete
    with pytest.raises(ValidationError):
        M10Config(tier_thresholds={"WATCH": 0.3, "ELEVATED": 0.5})  # missing CRITICAL


def test_unknown_key_rejected():
    import copy
    bad = copy.deepcopy(BASE_CONFIG)
    bad["data_layer"]["typo_knob"] = 1
    with pytest.raises(Exception):
        Config.model_validate(bad)


def test_load_missing_file_raises(tmp_path):
    with pytest.raises(ConfigError):
        load_config(tmp_path / "does_not_exist.yaml")


def test_load_malformed_yaml_raises(tmp_path):
    p = tmp_path / "bad.yaml"
    p.write_text("meta: [unclosed", encoding="utf-8")
    with pytest.raises(ConfigError):
        load_config(p)


def test_load_valid_resolves_cache_path(config_file):
    loaded = load_config(config_file)
    # cache_path "cache.db" resolves relative to the config file's dir.
    assert loaded.cache_path == (config_file.parent / "cache.db").resolve()
    assert loaded.config.meta.regime_floor_date == "2026-06-01"


def test_env_secret_overlay(config_file, monkeypatch):
    monkeypatch.setenv("ETHERSCAN_API_KEY", "sekret123")
    monkeypatch.setenv("LOG_LEVEL", "DEBUG")
    loaded = load_config(config_file)
    assert loaded.secrets.etherscan_api_key == "sekret123"
    assert loaded.log_level == "DEBUG"  # env overrides yaml logging.level
    assert "sekret123" in loaded.secrets.secret_values()


def test_redacting_filter_scrubs_secret_from_records():
    import logging
    from consensus.config import _RedactingFilter, REDACTED

    filt = _RedactingFilter(("sekret123",))
    record = logging.LogRecord(
        name="consensus.http", level=logging.WARNING, pathname=__file__, lineno=1,
        msg="http attempt failed for https://x/api?apikey=%s", args=("sekret123",),
        exc_info=None,
    )
    assert filt.filter(record) is True
    assert "sekret123" not in record.getMessage()
    assert REDACTED in record.getMessage()


def test_redacting_filter_no_secrets_passthrough():
    import logging
    from consensus.config import _RedactingFilter

    filt = _RedactingFilter(())
    record = logging.LogRecord(
        name="n", level=logging.INFO, pathname=__file__, lineno=1,
        msg="plain %d", args=(7,), exc_info=None,
    )
    assert filt.filter(record) is True
    assert record.getMessage() == "plain 7"


def test_configure_logging_attaches_redaction_and_is_idempotent(tmp_path, monkeypatch):
    import logging
    from consensus.config import _RedactingFilter, configure_logging
    from tests.conftest import make_loaded

    logger = logging.getLogger("consensus")
    saved_handlers = logger.handlers[:]
    logger.handlers = []
    try:
        loaded = make_loaded(tmp_path, etherscan_key="sekret123")
        configure_logging(loaded)
        configure_logging(loaded)  # idempotent: no duplicate handlers
        assert len(logger.handlers) == 1
        filters = [f for h in logger.handlers for f in h.filters]
        assert any(isinstance(f, _RedactingFilter) for f in filters)
        assert logger.propagate is False
    finally:
        logger.handlers = saved_handlers


def test_absolute_cache_path_respected(tmp_path, monkeypatch):
    import copy
    data = copy.deepcopy(BASE_CONFIG)
    abs_path = tmp_path / "sub" / "abs.db"
    data["data_layer"]["cache_path"] = str(abs_path)
    p = tmp_path / "config.yaml"
    p.write_text(yaml.safe_dump(data), encoding="utf-8")
    loaded = load_config(p)
    assert loaded.cache_path == abs_path.resolve()

"""Phase 0 scaffold assertions.

These are invariant tests, not coverage theatre: each one encodes a ruling from
the SC-1 order that a future edit could quietly violate.
"""

from __future__ import annotations

import inspect
from pathlib import Path

import pytest

from scout_daemon import config
from scout_daemon.errors import ScoutError


def test_roster_is_the_post_ruling_fourteen() -> None:
    """SC-R1 §3.1's twelve, plus Sherlock and YesWeHack.

    The recon roster was correct when written and stale the instant the
    white-hat carve-out moved security research out of RED. A rubric change
    expires the roster it produced -- this count is the assertion that the
    re-derivation actually happened rather than the old list being inherited.
    """
    assert len(config.WIRE_SOURCES) == 14
    names = {s.name for s in config.WIRE_SOURCES}
    assert {"sherlock", "yeswehack"} <= names


def test_whitehat_lane_is_identifiable() -> None:
    """White-hat sources enter YELLOW-per-program, never as ordinary work."""
    whitehat = {s.name for s in config.WIRE_SOURCES if s.lane == "whitehat"}
    assert whitehat == {"sherlock", "yeswehack"}


def test_immunefi_is_not_in_the_roster() -> None:
    """REJECT-on-ToS outranks legitimacy.

    Immunefi is green work with hostile terms: its ToS prohibits automated
    monitoring or copying while its robots.txt is permissive. It stays out on
    the ToS gate, which is separate from and higher-priority than any RED
    legitimacy class -- so a future rubric change must not quietly readmit it.
    """
    urls = " ".join(s.base_url for s in config.WIRE_SOURCES)
    assert "immunefi" not in urls.lower()


def test_source_names_unique() -> None:
    names = [s.name for s in config.WIRE_SOURCES]
    assert len(names) == len(set(names))
    assert len(config.SOURCES_BY_NAME) == len(names)


def test_every_source_clears_the_recon_gate() -> None:
    """The pre-registered gate was >=60% field fit. Nothing below it is WIRE."""
    for source in config.WIRE_SOURCES:
        assert source.recon_field_fit >= 60.0, source.name


def test_source_taxonomy_is_closed() -> None:
    for source in config.WIRE_SOURCES:
        assert source.access in {"json_api", "graphql", "ssr_html"}, source.name
        assert source.lane in {
            "work", "grant", "affiliate", "agent_native", "whitehat"
        }, source.name


def test_affiliate_lane_is_identifiable() -> None:
    """The affiliate lane is YELLOW-conditional and gated on Mando's Q2 ruling.

    It must stay greppable rather than being inferred at classification time.
    """
    affiliate = {s.name for s in config.WIRE_SOURCES if s.lane == "affiliate"}
    assert affiliate == {"affiliate_watch", "affpaying"}


def test_all_sources_are_https() -> None:
    for source in config.WIRE_SOURCES:
        assert source.base_url.startswith("https://"), source.name


def test_model_pin_is_a_bare_alias() -> None:
    """Date-suffixed model IDs are a documented 404 source."""
    model = config.CLASSIFIER_MODEL_ID
    assert model == "claude-sonnet-4-6"
    tail = model.rsplit("-", 1)[-1]
    assert not (tail.isdigit() and len(tail) == 8), f"date suffix in {model}"


def test_client_is_bounded() -> None:
    """The SDK default (600s x 3) can hang an unattended scan for ~30 minutes.

    Bound raised 120 -> 300 on 2026-08-11: one batched call over ~211 items
    generated 16k output tokens and needed more than 60s even streamed. The
    point of the assertion is that the client stays BOUNDED, not that it is
    fast -- 300s x 2 retries is a few minutes, not half an hour.
    """
    assert config.ANTHROPIC_TIMEOUT_S <= 300
    assert config.ANTHROPIC_MAX_RETRIES <= 3
    # The real guard: worst-case wall clock on one call stays under 15 minutes.
    worst_case_s = config.ANTHROPIC_TIMEOUT_S * (config.ANTHROPIC_MAX_RETRIES + 1)
    assert worst_case_s <= 900, f"worst case {worst_case_s}s is not bounded"


def test_state_home_is_outside_any_cloud_sync_tree() -> None:
    """A SQLite file inside a sync root corrupts mid-write, intermittently.

    Checks the real resolved path on the running host rather than asserting a
    hardcoded layout, so this stays meaningful after a migration to Basilic.
    """
    parts = {p.lower() for p in config.STATE_HOME.resolve().parts}
    for hazard in ("onedrive", "dropbox", "icloud", "icloud drive", "google drive"):
        assert hazard not in parts, f"state home sits inside {hazard}: {config.STATE_HOME}"


def test_env_example_carries_no_source_credentials() -> None:
    """Invariant 5: no account creation, ever -- so no source keys, ever."""
    text = (Path(config.__file__).resolve().parent.parent / ".env.example").read_text(
        encoding="utf-8"
    )
    for banned in ("SUPERTEAM", "QUESTBOOK", "DEWORK", "OPIRE", "ZINDI",
                   "GITHUB_TOKEN", "PASSWORD", "SECRET"):
        assert banned not in text.upper(), f"{banned} appears in .env.example"


def test_config_does_not_import_openclaw() -> None:
    """Zero OpenClaw coupling is what keeps the Orban/Basilic drift non-blocking."""
    src = inspect.getsource(config)
    assert "import openclaw" not in src
    assert "from openclaw" not in src


def test_halt_is_two_independent_channels(monkeypatch, tmp_path) -> None:
    monkeypatch.delenv(config.HALT_ENV_VAR, raising=False)
    monkeypatch.setattr(config, "HALT_FILE", tmp_path / "HALT")
    assert config.fetching_halted() is False

    monkeypatch.setenv(config.HALT_ENV_VAR, "1")
    assert config.fetching_halted() is True

    # Env values that must NOT be read as a halt.
    for falsey in ("", "0", "false", "False"):
        monkeypatch.setenv(config.HALT_ENV_VAR, falsey)
        assert config.fetching_halted() is False, falsey

    # File channel works with the env channel clear.
    monkeypatch.delenv(config.HALT_ENV_VAR, raising=False)
    (tmp_path / "HALT").touch()
    assert config.fetching_halted() is True


def test_error_taxonomy_roots_at_scout_error() -> None:
    from scout_daemon import errors

    for name in ("ConfigError", "FetchError", "ClassificationError", "QuarantineError"):
        assert issubclass(getattr(errors, name), ScoutError), name


def test_api_key_absence_fails_loud(monkeypatch) -> None:
    """Fail-loud: a missing key raises rather than returning a plausible None."""
    monkeypatch.setattr(config, "_load_dotenv", lambda path=None: None)
    monkeypatch.delenv("ANTHROPIC_API_KEY", raising=False)
    with pytest.raises(config.ConfigError):
        config.anthropic_api_key(required=True)
    assert config.anthropic_api_key(required=False) is None

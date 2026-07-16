"""Raw-scrape history dump (Order 19) — section grouping + timestamped write."""

from __future__ import annotations

from chatter_daemon.history import render_history, write_history


def test_render_groups_by_source_and_ticker():
    raw = [
        "finnhub_news\tAAPL\tApple beats on earnings",
        "stocktwits\tAAPL\t$AAPL to the moon",
        "twitter\tNVDA\tsolid data-center thesis",
        "twitter\tNVDA\tanother nvda take",
    ]
    out = render_history(raw, scan_id="cd-x", stamp="07-09-2026 09:42 EDT")
    assert "HEADLINES" in out and "Apple beats on earnings" in out
    assert "STOCKTWITS" in out and "$AAPL to the moon" in out
    assert "TWITTER" in out and "solid data-center thesis" in out and "another nvda take" in out
    assert "[AAPL]" in out and "[NVDA]" in out
    assert "cd-x" in out and "07-09-2026 09:42 EDT" in out


def test_render_skips_malformed_lines():
    out = render_history(["bad-line-no-tabs", "twitter\tNVDA\tgood"], scan_id="s", stamp="t")
    assert "good" in out and "bad-line-no-tabs" not in out


def test_headlines_section_merges_finnhub_and_yahoo():
    # CH-SRC-1: Finnhub + Yahoo heads share ONE HEADLINES section, grouped together per ticker.
    raw = [
        "finnhub_news\tNVDA\tNvidia earnings beat",
        "yahoo_rss\tNVDA\tNvidia fresh scoop",
        "finnhub_news\tMU\tMicron guidance",
    ]
    out = render_history(raw, scan_id="s", stamp="t")
    assert out.count("### HEADLINES") == 1  # one merged headline section, not two
    nvda = out.split("[NVDA]")[1].split("[MU]")[0]
    assert "Nvidia earnings beat" in nvda and "Nvidia fresh scoop" in nvda  # both sources together


def test_write_timestamped_txt(tmp_path):
    p = write_history(
        tmp_path, ["twitter\tNVDA\thello"], scan_id="cd-1", canonical_ts="2026-07-09T13:42:00Z"
    )
    assert p.exists() and p.parent == tmp_path
    assert p.name.startswith("chatter-raw_") and p.name.endswith(".txt")
    assert "_EDT.txt" in p.name  # 13:42 UTC in July -> 09:42 EDT
    assert "hello" in p.read_text(encoding="utf-8")


def test_write_creates_history_dir(tmp_path):
    root = tmp_path / "history"
    assert not root.exists()
    write_history(root, ["twitter\tX\ty"], scan_id="s", canonical_ts="2026-07-09T13:42:00Z")
    assert root.is_dir()

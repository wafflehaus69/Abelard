"""Brief archive writer/reader tests — hermetic, tmp-dir scoped."""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path

import pytest

from news_watch_daemon.synthesize.archive import (
    ArchiveError,
    list_brief_ids,
    read_brief,
    write_brief,
)
from news_watch_daemon.synthesize.archive import _month_partition  # internal
from news_watch_daemon.synthesize.brief import (
    Brief,
    Dispatch,
    SynthesisMetadata,
    Trigger,
    TriggerWindow,
)


def _make_brief(brief_id: str = "nwd-2026-05-13T14-32-08Z-a1b2c3d4", narrative: str = "x") -> Brief:
    return Brief(
        brief_id=brief_id,
        generated_at="2026-05-13T14:32:08Z",
        trigger=Trigger(
            type="event",
            reason="t",
            window=TriggerWindow(since="a", until="b"),
        ),
        themes_covered=["us_iran_escalation"],
        narrative=narrative,
        dispatch=Dispatch(alerted=False),
        synthesis_metadata=SynthesisMetadata(
            model_used="claude-sonnet-4-7",
            theses_doc_available=True,
        ),
    )


# ---------- _month_partition ----------


def test_month_partition_extracts_yyyy_mm():
    assert _month_partition("nwd-2026-05-13T14-32-08Z-a1b2c3d4") == "2026-05"
    assert _month_partition("nwd-2099-12-31T23-59-59Z-deadbeef") == "2099-12"


def test_month_partition_rejects_non_nwd_prefix():
    with pytest.raises(ArchiveError, match="nwd"):
        _month_partition("foo-2026-05-13T14-32-08Z-abc")


def test_month_partition_rejects_short_id():
    with pytest.raises(ArchiveError):
        _month_partition("nwd-2026")


def test_month_partition_rejects_non_numeric_year_month():
    with pytest.raises(ArchiveError):
        _month_partition("nwd-XXXX-05-13T14-32-08Z-abc")


# ---------- write + read round-trip ----------


def test_write_and_read_roundtrip(tmp_path):
    archive = tmp_path / "archive"
    brief = _make_brief()
    path = write_brief(archive, brief)
    assert path.is_file()
    assert path.parent.name == "2026-05"
    assert path.name == f"{brief.brief_id}.json"
    restored = read_brief(archive, brief.brief_id)
    assert restored == brief


def test_write_creates_archive_root_if_missing(tmp_path):
    archive = tmp_path / "deep" / "nested" / "archive"
    write_brief(archive, _make_brief())
    assert archive.is_dir()


def test_write_creates_partition_dir(tmp_path):
    archive = tmp_path / "archive"
    write_brief(archive, _make_brief())
    assert (archive / "2026-05").is_dir()


def test_write_two_briefs_same_month_share_partition(tmp_path):
    archive = tmp_path / "archive"
    write_brief(archive, _make_brief("nwd-2026-05-13T14-32-08Z-aaaaaaaa", narrative="one"))
    write_brief(archive, _make_brief("nwd-2026-05-15T09-00-00Z-bbbbbbbb", narrative="two"))
    files = list((archive / "2026-05").iterdir())
    assert len([f for f in files if f.suffix == ".json"]) == 2


def test_write_briefs_different_months_separate_partitions(tmp_path):
    archive = tmp_path / "archive"
    write_brief(archive, _make_brief("nwd-2026-05-13T14-32-08Z-aaaaaaaa"))
    write_brief(archive, _make_brief("nwd-2026-06-01T00-00-00Z-bbbbbbbb"))
    assert (archive / "2026-05").is_dir()
    assert (archive / "2026-06").is_dir()


# ---------- atomic write semantics ----------


def test_no_tmp_files_after_successful_write(tmp_path):
    archive = tmp_path / "archive"
    write_brief(archive, _make_brief())
    partition = archive / "2026-05"
    tmp_files = [f for f in partition.iterdir() if f.name.startswith(".") and f.name.endswith(".tmp")]
    assert tmp_files == [], "atomic write should leave no .tmp residue"


def test_write_tmp_files_cleaned_on_failure(tmp_path, monkeypatch):
    """If serialization fails after open, the tmp file is removed."""
    archive = tmp_path / "archive"

    def _boom(*args, **kwargs):
        raise RuntimeError("induced serialization failure")

    monkeypatch.setattr("json.dump", _boom)
    with pytest.raises(RuntimeError, match="induced"):
        write_brief(archive, _make_brief())

    # Partition should exist (it was mkdir-ed before the failure) but
    # contain no tmp files left behind.
    partition = archive / "2026-05"
    if partition.exists():
        residue = list(partition.iterdir())
        assert residue == [], f"tmp file leaked on failure: {residue}"


# ---------- read errors ----------


def test_read_missing_brief_raises_archive_error(tmp_path):
    archive = tmp_path / "archive"
    archive.mkdir()
    with pytest.raises(ArchiveError, match="not found"):
        read_brief(archive, "nwd-2026-05-13T14-32-08Z-aaaaaaaa")


def test_read_corrupt_brief_raises_archive_error(tmp_path):
    archive = tmp_path / "archive"
    partition = archive / "2026-05"
    partition.mkdir(parents=True)
    bid = "nwd-2026-05-13T14-32-08Z-aaaaaaaa"
    (partition / f"{bid}.json").write_text("{not valid json", encoding="utf-8")
    with pytest.raises(ArchiveError, match="corrupt"):
        read_brief(archive, bid)


def test_read_schema_mismatch_raises_archive_error(tmp_path):
    archive = tmp_path / "archive"
    partition = archive / "2026-05"
    partition.mkdir(parents=True)
    bid = "nwd-2026-05-13T14-32-08Z-aaaaaaaa"
    # Valid JSON but missing required fields
    (partition / f"{bid}.json").write_text('{"hello": "world"}', encoding="utf-8")
    with pytest.raises(ArchiveError, match="schema mismatch"):
        read_brief(archive, bid)


# ---------- list_brief_ids ----------


def test_list_empty_archive(tmp_path):
    assert list_brief_ids(tmp_path / "does-not-exist") == []


def test_list_returns_brief_ids_newest_first(tmp_path):
    archive = tmp_path / "archive"
    ids = [
        "nwd-2026-04-01T00-00-00Z-aaaaaaaa",  # older month
        "nwd-2026-05-13T14-32-08Z-bbbbbbbb",  # mid
        "nwd-2026-05-15T09-00-00Z-cccccccc",  # newest in May
    ]
    for bid in ids:
        write_brief(archive, _make_brief(bid))
    listed = list_brief_ids(archive)
    # Newest first: 05/15 > 05/13 > 04/01
    assert listed == [
        "nwd-2026-05-15T09-00-00Z-cccccccc",
        "nwd-2026-05-13T14-32-08Z-bbbbbbbb",
        "nwd-2026-04-01T00-00-00Z-aaaaaaaa",
    ]


def test_list_with_limit_short_circuits(tmp_path):
    archive = tmp_path / "archive"
    for i in range(5):
        bid = f"nwd-2026-05-13T14-32-{i:02d}Z-aaaaaaaa"
        write_brief(archive, _make_brief(bid))
    assert len(list_brief_ids(archive, limit=2)) == 2


def test_list_ignores_non_partition_dirs(tmp_path):
    archive = tmp_path / "archive"
    archive.mkdir()
    (archive / "junk-dir").mkdir()  # not YYYY-MM format
    (archive / "2026").mkdir()      # missing MM
    write_brief(archive, _make_brief())  # legit
    # Only the legitimate brief should be listed
    listed = list_brief_ids(archive)
    assert len(listed) == 1


def test_list_ignores_hidden_tmp_files(tmp_path):
    archive = tmp_path / "archive"
    partition = archive / "2026-05"
    partition.mkdir(parents=True)
    bid = "nwd-2026-05-13T14-32-08Z-aaaaaaaa"
    # Real brief
    (partition / f"{bid}.json").write_text(
        json.dumps(_make_brief(bid).model_dump(mode="json")),
        encoding="utf-8",
    )
    # Decoy hidden tmp file
    (partition / f".{bid}.xyz.tmp").write_text("partial", encoding="utf-8")
    listed = list_brief_ids(archive)
    assert listed == [bid]

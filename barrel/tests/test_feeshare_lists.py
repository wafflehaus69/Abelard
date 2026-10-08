"""recon/feeshare_lists.py: from the first fee-share query's rows to the lists the member query and the
organic query are given. Synthetic 32-byte keys only (the writer is test_feeshare's own).
Run: python -m pytest barrel/tests/test_feeshare_lists.py"""
import json
import pathlib
import sys

import pytest

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1] / "recon"))
sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent))
import feeshare_lists as fl  # noqa: E402
from test_feeshare import MINT, OTHER_MINT, OWNER, addr, create, row, token_row, update  # noqa: E402

ROWS = [row(create([(10, 10_000)]), -10),                                  # in force from creation
        row(update([(20, 5000), (OWNER, 5000)]), 30),                      # between the 15 and 60 minute lags
        row(update([(10, 2500), (30, 7500)]), 100),                        # between 60 and 240
        row(update([(40, 10_000)]), 500),                                  # after the last lag: outside any entry
        row(create([(10, 10_000)], mint=OTHER_MINT), -10, mint=OTHER_MINT),
        token_row(mint=3)]                                                 # a token with no config at all


def test_codes_say_in_force_at_entry_or_only_before_and_nothing_about_later():
    out, pairs, codes = fl.summary(ROWS, owner={addr(OWNER)})
    got = {(c["mint"], c["w"]): c["code"] for c in codes}
    assert got == {(addr(MINT), addr(10)): "E", (addr(MINT), addr(30)): "E",     # the config of minute 100 is the one at + 240
                   (addr(MINT), addr(20)): "P",                                  # a recipient at minute 30, replaced by minute 100
                   (addr(OTHER_MINT), addr(10)): "E"}
    assert addr(40) not in {c["w"] for c in codes}                               # became one after + 240 minutes: no code
    assert addr(OWNER) not in json.dumps(codes) + json.dumps(pairs)
    assert out["owner_codes_removed"] == 1 and out["owner_pairs_removed"] == 1
    assert out["codes"] == {"E": 3, "P": 1}


def test_the_pairs_are_the_member_querys_candidates_and_the_summary_holds_counts_only():
    out, pairs, _ = fl.summary(ROWS, owner={addr(OWNER)})
    assert {(p["mint"], p["w"]) for p in pairs} == {(addr(MINT), addr(10)), (addr(MINT), addr(20)), (addr(MINT), addr(30)),
                                                    (addr(OTHER_MINT), addr(10))}
    assert out["tokens"] == 3 and out["tokens_with_a_pair"] == 2 and out["pairs"] == 4
    assert out["state_by_lag"]["240"] == {"measured": 2, "not_applicable": 1}
    assert out["tokens_by_recipient_count_at_entry"] == {"0": 1, "1": 1, "2": 1}
    assert not any(addr(i) in json.dumps(out) for i in (MINT, OTHER_MINT, 10, 20, 30, 40, OWNER))


def test_a_token_whose_state_at_entry_cannot_be_read_names_no_recipient_at_entry():
    rows = [row(create([(10, 10_000)]), -10), row(update([(20, 10_000)]), 30, owner_in_payload=True, data_hex=None)]
    _, pairs, codes = fl.summary(rows)
    assert {c["code"] for c in codes} == {"P"} and [c["w"] for c in codes] == [addr(10)]     # known before; unknown since


@pytest.fixture
def tree(tmp_path, monkeypatch):
    for d in ("private/out", "data", "recon/out"):
        (tmp_path / d).mkdir(parents=True)
    monkeypatch.setattr(fl, "ROOT", tmp_path); monkeypatch.setattr(fl, "PRIV", tmp_path / "private" / "out"); monkeypatch.setattr(fl, "DATA", tmp_path / "data")
    monkeypatch.setattr(fl.owner_wallets, "owner_wallets", lambda: [addr(OWNER)])
    return tmp_path


def test_a_chunk_reads_its_own_export_and_writes_lists_the_runner_will_accept_for_that_chunk(tree):
    (tree / "data" / "2026-08_fs_20261101T120000.json").write_text(json.dumps(ROWS), encoding="utf-8")
    (tree / "data" / "2026-07_fs_20261102T120000.json").write_text(json.dumps([token_row()]), encoding="utf-8")   # another chunk, newer
    (tree / "private" / "out" / "rows_2026-08_fs_20261103T120000.json").write_text("null", encoding="utf-8")      # a cancelled run left this
    fl.main("2026-08")
    pairs = json.loads((tree / "private" / "out" / "fs_pairs_2026-08.json").read_text(encoding="utf-8"))
    codes = json.loads((tree / "private" / "out" / "fs_codes_2026-08.json").read_text(encoding="utf-8"))
    assert len(pairs) == 4 and len(codes) == 4 and set(codes[0]) == {"mint", "w", "code"}
    tracked = json.loads((tree / "recon" / "out" / "feeshare_lists_2026-08.json").read_text(encoding="utf-8"))
    assert tracked["source"] == "2026-08_fs_20261101T120000.json" and tracked["pairs"] == 4
    with pytest.raises(SystemExit, match="has not been run for 2026-09"):
        fl.main("2026-09")


def test_a_proving_tag_reads_the_proving_files_rows(tree):
    (tree / "private" / "out" / "rows_feeshare_fs_pbday_20261101T120000.json").write_text(json.dumps(ROWS), encoding="utf-8")
    fl.main("pbday")
    assert (tree / "private" / "out" / "fs_pairs_pbday.json").exists()

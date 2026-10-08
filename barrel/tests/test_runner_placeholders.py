"""What the runner substitutes into a query at run time, with Dune stubbed. No network.

Lists of addresses (funders, fee-share pairs) and the owner filter exist only in the text that is sent; the file in
the repo keeps a placeholder. These tests read the text that would have been sent.
Run: python -m pytest barrel/tests/test_runner_placeholders.py"""
import json
import pathlib
import sys

import pytest

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1] / "recon"))
import dune_run_sql as r  # noqa: E402
import owner_wallets  # noqa: E402

LEDGER = "| n | file | id | credits | total |\n|---|---|---|---|---|\n| 0 | x | `x` | 0.000 | 0.00 |\n\n" \
         "**Total consumed: 0.00 credits. Remaining: 25000. Usable after 15% reserve: 21250.**\n"
OWNER = "1" * 31 + "2"                      # synthetic 32-byte key: thirty-one zero bytes and 0x01
A, B, MINT = "1" * 31 + "3", "1" * 31 + "4", "1" * 31 + "9"


class Dune:
    def __init__(self, rows=None): self.sent, self.rows = [], rows or []
    def __call__(self, method, path, key, body=None):
        if path == "/sql/execute": self.sent.append(body["sql"]); return {"execution_id": "EXEC1"}
        if path.endswith("/status"): return {"state": "QUERY_STATE_COMPLETED", "is_execution_finished": True, "execution_cost_credits": 1.0}
        if "/results" in path: return {"result": {"rows": list(self.rows), "metadata": {"total_row_count": len(self.rows)}}}
        raise AssertionError(path)


@pytest.fixture
def env(tmp_path, monkeypatch):
    (tmp_path / "recon" / "out").mkdir(parents=True); (tmp_path / "docs").mkdir()
    (tmp_path / "docs" / "credits.md").write_text(LEDGER, encoding="utf-8")
    monkeypatch.setattr(r, "ROOT", tmp_path); monkeypatch.setattr(r, "OUT", tmp_path / "recon" / "out")
    monkeypatch.setattr(r, "LEDGER", tmp_path / "docs" / "credits.md")
    monkeypatch.setattr(r.rt, "api_key", lambda: "k")
    monkeypatch.setattr(r.dune_usage, "usage", lambda: {"credits_included": 25000, "credits_used": 100})
    monkeypatch.setattr(owner_wallets, "owner_wallets", lambda: [OWNER])

    def run(name, sql, *flags, rows=None):
        f = tmp_path / name; f.write_text(sql, encoding="utf-8")
        d = Dune(rows); monkeypatch.setattr(r.rt, "dune", d)
        monkeypatch.setattr(sys, "argv", ["dune_run_sql.py", str(f), "--expect", "1", *flags])
        r.main()
        return d

    def pairs(name, rows):
        f = tmp_path / name; f.write_text(json.dumps(rows), encoding="utf-8"); return str(f)
    return tmp_path, run, pairs


def test_a_key_as_hex_is_its_32_bytes():
    assert owner_wallets.hex_key("1" * 32) == "00" * 32
    assert owner_wallets.hex_key(OWNER) == "00" * 31 + "01"
    with pytest.raises(OverflowError):
        owner_wallets.hex_key("z" * 44)                      # more than 32 bytes is not a key


def test_the_hex_owner_filter_looks_for_the_key_anywhere_in_the_payload(env):
    tmp, run, _ = env
    d = run("fs.sql", "SELECT CASE WHEN __NOT_OWNER_HEX(e.data_hex)__ THEN e.data_hex END AS data_hex FROM e")
    assert d.sent == ["SELECT CASE WHEN (strpos(e.data_hex, '" + "00" * 31 + "01') = 0) THEN e.data_hex END AS data_hex FROM e"]


def test_fee_share_pairs_go_into_the_text_sent_without_the_owner_and_are_recorded_by_count(env):
    tmp, run, pairs = env
    p = pairs("pairs_2026-08.json", [{"mint": MINT, "w": B}, {"mint": MINT, "w": A}, {"mint": MINT, "w": OWNER}, {"mint": MINT, "w": A}])
    d = run("2026-08_fsm.sql", "SELECT mint, w FROM (VALUES __FS_PAIRS__) AS t(mint, w) WHERE mint IS NOT NULL AND __NOT_OWNER(w)__", "--pairs-from", p)
    assert d.sent[0].startswith(f"SELECT mint, w FROM (VALUES ('{MINT}', '{A}'), ('{MINT}', '{B}')) AS t(mint, w)")
    assert OWNER not in d.sent[0].split("NOT IN")[0]                       # only inside the owner filter itself
    rec = json.loads(next((tmp / "recon" / "out").glob("run_2026-08_fsm_*.json")).read_text(encoding="utf-8"))
    assert rec["asked"]["n_pairs"] == 2 and rec["asked"]["n_owner_pairs_removed"] == 1 and A not in json.dumps(rec)


def test_the_member_query_is_refused_without_its_list_or_with_another_chunks(env):
    tmp, run, pairs = env
    sql = "SELECT 1 FROM (VALUES __FS_PAIRS__) AS t(mint, w)"
    with pytest.raises(SystemExit, match="needs --pairs-from"):
        run("2026-08_fsm.sql", sql)
    with pytest.raises(SystemExit, match="does not name that chunk"):
        run("2026-08_fsm.sql", sql, "--pairs-from", pairs("pairs_2026-07.json", [{"mint": MINT, "w": A}]))
    with pytest.raises(SystemExit, match="nothing for this query to do"):
        run("2026-08_fsm.sql", sql, "--pairs-from", pairs("pairs_2026-08.json", [{"mint": MINT, "w": OWNER}]))
    with pytest.raises(SystemExit, match="not two base58 addresses"):
        run("2026-08_fsm.sql", sql, "--pairs-from", pairs("pairs_2026-08.json", [{"mint": MINT, "w": "0x' OR 1=1 --"}]))


def test_the_organic_query_runs_without_a_list_only_when_told_and_says_so_in_its_output(env):
    tmp, run, pairs = env
    sql = "SELECT __FS_SUPPLIED__ AS fs_list FROM (VALUES __FS_CODES__) AS t(mint, w, code) WHERE mint IS NOT NULL"
    with pytest.raises(SystemExit, match="--no-pairs said out loud"):
        run("2025-06_org2.sql", sql)
    d = run("2025-06_org2.sql", sql, "--no-pairs")
    assert d.sent == ["SELECT FALSE AS fs_list FROM (VALUES (CAST(NULL AS varchar), CAST(NULL AS varchar), CAST(NULL AS varchar))) AS t(mint, w, code) WHERE mint IS NOT NULL"]
    d = run("2026-08_org2.sql", sql, "--pairs-from", pairs("codes_2026-08.json", [{"mint": MINT, "w": A, "code": "E"}]))
    assert d.sent == [f"SELECT TRUE AS fs_list FROM (VALUES ('{MINT}', '{A}', 'E')) AS t(mint, w, code) WHERE mint IS NOT NULL"]
    with pytest.raises(SystemExit, match="not one upper-case letter"):
        run("2026-08_org2.sql", sql, "--pairs-from", pairs("codes_2026-08.json", [{"mint": MINT, "w": A, "code": "E'"}]))


def test_a_placeholder_left_in_the_text_is_never_submitted(env):
    tmp, run, _ = env
    with pytest.raises(SystemExit, match="placeholder\\(s\\) not substituted: __NOT_OWNER\\(b.\"user\"\\)__"):
        run("x.sql", 'SELECT 1 FROM b WHERE __NOT_OWNER(b."user")__')      # the argument form the macro does not accept
    with pytest.raises(SystemExit, match="__SOMETHING_NEW__"):
        run("y.sql", "SELECT __SOMETHING_NEW__")


def test_a_fetched_row_with_an_owner_key_inside_a_hex_payload_is_dropped(env):
    tmp, run, _ = env
    payload = "E445A52E51CB9A1D" + "AB" * 40 + owner_wallets.hex_key(OWNER).lower() + "CD" * 8
    d = run("fs.sql", "SELECT 1", rows=[{"mint": MINT, "data_hex": payload}, {"mint": MINT, "data_hex": "AB" * 64}, {"mint": MINT, "data_hex": None}])
    rec = json.loads(next((tmp / "recon" / "out").glob("run_fs_*.json")).read_text(encoding="utf-8"))
    assert rec["rows_dropped_owner"] == 1 and len(rec["rows"]) == 2

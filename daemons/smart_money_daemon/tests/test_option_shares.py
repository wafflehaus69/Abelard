"""Option rows state a share count and it must survive to the DB.

Form 13F reports sshPrnamt and sshPrnamtType on option rows exactly as on long
rows. The ingest emitted a hardcoded 0 for every put and call, and the comment
above it asserted the filing "never said" — so 450 option rows across the corpus
carried shares=0 while the filings carried real counts. Fixtures below are
verbatim from Duquesne's 2026-06-30 filing (0001536411-26-000006) and Elliott's
(0001013594-26-000915).

There is no strike and no expiry in Form 13F. These tests cover SIZE only.
"""
from smart_money import db as dbmod, queries as q
from smart_money.thirteenf import parse_holdings
from smart_money.thirteenf_ingest import _holding_rows
from smart_money import reparse_option_shares as ros


def _xml(*blocks):
    return ('<?xml version="1.0"?><informationTable '
            'xmlns="http://www.sec.gov/edgar/document/thirteenf/informationtable">'
            + "".join(blocks) + "</informationTable>")


def _row(issuer, cusip, value, shares, pc=None, stype="SH", title="COM"):
    return (
        "<infoTable><nameOfIssuer>{}</nameOfIssuer>"
        "<titleOfClass>{}</titleOfClass><cusip>{}</cusip><value>{}</value>"
        "<shrsOrPrnAmt><sshPrnamt>{}</sshPrnamt>"
        "<sshPrnamtType>{}</sshPrnamtType></shrsOrPrnAmt>{}"
        "<investmentDiscretion>DFND</investmentDiscretion>"
        "<votingAuthority><Sole>{}</Sole><Shared>0</Shared><None>0</None>"
        "</votingAuthority></infoTable>"
    ).format(issuer, title, cusip, value, shares, stype,
             "<putCall>{}</putCall>".format(pc) if pc else "", shares)


# Duquesne Q2, verbatim: value 52996 (thousands), sshPrnamt 126000, type SH.
DRUCK_TSLA = _row("Tesla Inc", "88160R101", 52996, 126000, "Call")


def test_the_call_share_count_reaches_the_parser():
    h = parse_holdings(_xml(DRUCK_TSLA))["88160R101"]
    assert h["call_val"] == 52996
    assert h["call_sh"] == 126000, "THE defect: this was discarded"
    assert h["call_type"] == "SH"
    assert h["value"] == 0 and h["shares"] == 0, "no long bucket in this filing"


def test_the_call_share_count_reaches_the_durable_row():
    rows = list(_holding_rows(parse_holdings(_xml(DRUCK_TSLA))))
    assert len(rows) == 1
    cusip, _issuer, pc, val, sh, stype, title = rows[0]
    assert (cusip, pc, val, sh, stype, title) == (
        "88160R101", "call", 52996, 126000, "SH", "COM")


def test_implied_underlying_price_is_now_derivable():
    """value / shares is the underlying's mark — the arithmetic that was
    impossible while shares was zero. 52,996,000 / 126,000 = $420.60."""
    h = parse_holdings(_xml(DRUCK_TSLA))["88160R101"]
    assert round((h["call_val"] * 1000) / h["call_sh"], 2) == 420.60
    assert h["call_sh"] % 100 == 0, "share counts are whole contracts"
    assert h["call_sh"] // 100 == 1260


def test_puts_and_calls_and_longs_on_one_cusip_stay_separate():
    """One cusip carrying all three buckets must not pool its share counts."""
    x = _xml(_row("Ishares Tr", "464287655", 99149, 330000, "Call"),
             _row("Ishares Tr", "464287655", 75000, 250000, "Put"),
             _row("Ishares Tr", "464287655", 12000, 40000))
    h = parse_holdings(x)["464287655"]
    assert (h["call_sh"], h["put_sh"], h["shares"]) == (330000, 250000, 40000)
    assert (h["call_val"], h["put_val"], h["value"]) == (99149, 75000, 12000)
    by = {pc: (val, sh) for _c, _i, pc, val, sh, _st, _t in _holding_rows(parse_holdings(x))}
    assert by == {"long": (12000, 40000), "call": (99149, 330000),
                  "put": (75000, 250000)}


def test_repeated_option_rows_on_one_cusip_sum():
    x = _xml(_row("Elliott QQQ", "73935A104", 2000000, 3000000, "Put"),
             _row("Elliott QQQ", "73935A104", 562672, 480000, "Put"))
    h = parse_holdings(x)["73935A104"]
    assert (h["put_val"], h["put_sh"]) == (2562672, 3480000)


def test_mixed_option_type_is_marked_not_resolved():
    """PRN on an option row is unusual, not forbidden. If one cusip reports both
    types the summed count is meaningless and must say so."""
    x = _xml(_row("Odd Co", "111111111", 100, 5, "Call", stype="SH"),
             _row("Odd Co", "111111111", 200, 7, "Call", stype="PRN"))
    assert parse_holdings(x)["111111111"]["call_type"] == "MIXED"


def test_a_zero_share_option_row_is_still_emitted():
    """Marked, never dropped: a filer stating value with no share count keeps its
    row rather than vanishing from the book."""
    x = _xml(_row("Sparse Co", "222222222", 4242, 0, "Put"))
    rows = list(_holding_rows(parse_holdings(x)))
    assert [(r[2], r[3], r[4]) for r in rows] == [("put", 4242, 0)]


# ---- direction: shares are a TRADE, value is a trade plus a MARK -------------

def _seed(con, cik, period, pc, value, shares):
    con.execute(
        "INSERT OR REPLACE INTO thirteenf_holdings(cik, accession, period, "
        "filed_date, cusip, ticker, issuer, put_call, value, shares, "
        "ingested_at_unix, value_scale) VALUES (?,?,?,?,?,?,?,?,?,?,0,1)",
        (cik, "acc" + cik + period + pc, period, period, "cTSLA", "TSLA",
         "Tesla Inc", pc, value, shares))


def test_an_untouched_call_through_a_rally_is_not_added(tmp_path, monkeypatch):
    """THE regression. Same 126,000 shares in both periods, underlying up 40%.
    Measured on value that reads 'added' — a trade the filer never made."""
    path = str(tmp_path / "d.db")
    con = dbmod.connect(path)
    _seed(con, "1536411", "2026-03-31", "call", 37_854_000, 126000)
    _seed(con, "1536411", "2026-06-30", "call", 52_996_000, 126000)
    con.commit()
    con.close()
    ro = q.connect_ro(path)
    cur = q._scaled_holdings(ro, "1536411", "2026-06-30", 1)
    pri = q._scaled_holdings(ro, "1536411", "2026-03-31", 1)
    k = list(cur)[0]
    cm, pm, measure = q._flow_measure(cur[k], pri[k])
    assert measure == "shares" and cm == pm, (
        "unchanged contracts must express no direction, got %r vs %r" % (cm, pm))
    ro.close()


def test_a_genuinely_enlarged_call_still_reads_added(tmp_path):
    path = str(tmp_path / "e.db")
    con = dbmod.connect(path)
    _seed(con, "1536411", "2026-03-31", "call", 37_854_000, 90000)
    _seed(con, "1536411", "2026-06-30", "call", 52_996_000, 126000)
    con.commit()
    con.close()
    ro = q.connect_ro(path)
    cur = q._scaled_holdings(ro, "1536411", "2026-06-30", 1)
    pri = q._scaled_holdings(ro, "1536411", "2026-03-31", 1)
    k = list(cur)[0]
    cm, pm, measure = q._flow_measure(cur[k], pri[k])
    assert measure == "shares" and cm > pm
    ro.close()


def test_value_fallback_survives_for_unbackfilled_rows(tmp_path):
    """Rows ingested before the fix carry shares=0 on both sides. Those must keep
    reading direction off value rather than silently going flat."""
    path = str(tmp_path / "f.db")
    con = dbmod.connect(path)
    _seed(con, "1536411", "2026-03-31", "call", 37_854_000, 0)
    _seed(con, "1536411", "2026-06-30", "call", 52_996_000, 0)
    con.commit()
    con.close()
    ro = q.connect_ro(path)
    cur = q._scaled_holdings(ro, "1536411", "2026-06-30", 1)
    pri = q._scaled_holdings(ro, "1536411", "2026-03-31", 1)
    k = list(cur)[0]
    cm, pm, measure = q._flow_measure(cur[k], pri[k])
    assert measure == "value" and cm > pm
    ro.close()


def test_long_row_direction_is_unchanged_by_the_new_rule(tmp_path):
    """Long rows always had shares and always used them. The unified rule must not
    move them: 0 long rows in the corpus have value>0 with shares=0."""
    path = str(tmp_path / "g.db")
    con = dbmod.connect(path)
    _seed(con, "1536411", "2026-03-31", "long", 15_215_000, 37675)
    _seed(con, "1536411", "2026-06-30", "long", 4_882_000, 18838)
    con.commit()
    con.close()
    ro = q.connect_ro(path)
    cur = q._scaled_holdings(ro, "1536411", "2026-06-30", 1)
    pri = q._scaled_holdings(ro, "1536411", "2026-03-31", 1)
    k = list(cur)[0]
    cm, pm, measure = q._flow_measure(cur[k], pri[k])
    assert measure == "shares" and cm < pm, "shares halved -> trimmed"
    ro.close()


# ---- the backfill refuses anything it cannot prove ---------------------------

def _db_with_option_row(path, value=52996, shares=0):
    con = dbmod.connect(path)
    con.execute(
        "INSERT INTO thirteenf_holdings(cik, accession, period, filed_date, "
        "cusip, ticker, issuer, put_call, value, shares, ingested_at_unix, "
        "value_scale) VALUES ('1536411','0001536411-26-000006','2026-06-30',"
        "'2026-08-14','88160R101','TSLA','Tesla Inc','call',?,?,0,1000)",
        (value, shares))
    con.commit()
    return con


def _patch_fetch(monkeypatch, xml):
    monkeypatch.setattr(ros.thirteenf, "fetch_info_table",
                        lambda cik, acc, contact: parse_holdings(xml))


def test_backfill_plans_and_applies_the_recovered_count(tmp_path, monkeypatch):
    con = _db_with_option_row(str(tmp_path / "b.db"))
    _patch_fetch(monkeypatch, _xml(DRUCK_TSLA))
    updates, stats = ros.plan(con, "contact")
    assert stats["filings_usable"] == 1 and stats["rows_to_update"] == 1
    assert stats["shares_recovered"] == 126000
    assert ros.apply(con, updates) == 1
    got = con.execute("SELECT shares, shares_type FROM thirteenf_holdings").fetchone()
    assert got == (126000, "SH")
    con.close()


def test_backfill_refuses_a_filing_whose_value_moved(tmp_path, monkeypatch):
    """value was never touched by the defect, so it is the control proving this is
    the same document. If it drifted, the share counts may belong elsewhere."""
    con = _db_with_option_row(str(tmp_path / "v.db"), value=99999)
    _patch_fetch(monkeypatch, _xml(DRUCK_TSLA))
    updates, stats = ros.plan(con, "contact")
    assert updates == [] and stats["filings_value_mismatch"] == 1
    assert stats["filings_usable"] == 0
    con.close()


def test_backfill_refuses_a_filing_whose_option_rows_moved(tmp_path, monkeypatch):
    con = _db_with_option_row(str(tmp_path / "k.db"))
    _patch_fetch(monkeypatch, _xml(
        _row("Meta Platforms Inc", "30303M102", 28165, 50000, "Call")))
    updates, stats = ros.plan(con, "contact")
    assert updates == [] and stats["filings_key_mismatch"] == 1
    con.close()


def test_backfill_never_touches_a_long_row(tmp_path, monkeypatch):
    path = str(tmp_path / "l.db")
    con = _db_with_option_row(path)
    con.execute(
        "INSERT INTO thirteenf_holdings(cik, accession, period, filed_date, "
        "cusip, ticker, issuer, put_call, value, shares, ingested_at_unix, "
        "value_scale) VALUES ('1536411','0001536411-26-000006','2026-06-30',"
        "'2026-08-14','88160R101','TSLA','Tesla Inc','long',1000,55,0,1000)")
    con.commit()
    _patch_fetch(monkeypatch, _xml(DRUCK_TSLA, _row("Tesla Inc", "88160R101", 1000, 55)))
    updates, _stats = ros.plan(con, "contact")
    ros.apply(con, updates)
    assert con.execute(
        "SELECT shares FROM thirteenf_holdings WHERE put_call='long'"
    ).fetchone()[0] == 55, "the long row is not this tool's business"
    con.close()


def test_backfill_is_a_noop_on_second_run(tmp_path, monkeypatch):
    con = _db_with_option_row(str(tmp_path / "i.db"))
    _patch_fetch(monkeypatch, _xml(DRUCK_TSLA))
    ros.apply(con, ros.plan(con, "contact")[0])
    updates, stats = ros.plan(con, "contact")
    assert updates == [] and stats["rows_already_correct"] == 1
    con.close()


def test_backfill_records_a_real_zero_rather_than_repairing_it(tmp_path, monkeypatch):
    con = _db_with_option_row(str(tmp_path / "z.db"), value=4242)
    _patch_fetch(monkeypatch, _xml(_row("Tesla Inc", "88160R101", 4242, 0, "Call")))
    updates, stats = ros.plan(con, "contact")
    assert updates == [] and stats["rows_zero_in_filing"] == 1
    con.close()

"""Mojibake counter, synthetic fixture (ORDER SM-R1 P1). Detect-and-count only,
never fix. Names built from code points so this test file stays pure-ASCII."""

from smart_money import db as dbmod
from smart_money.mojibake import scan_mojibake


def _person(con, name):
    con.execute("INSERT INTO persons(name, type) VALUES (?, 'test')", (name,))


def test_mojibake_detected_counted_not_fixed():
    con = dbmod.connect(":memory:")
    moji = "Jos" + chr(0xC3) + chr(0xA9)   # mis-decoded 'Jose' (C3 A9) -> mojibake
    clean = "Jos" + chr(0xE9)              # legit e-acute -> non-ascii, NOT mojibake
    for nm in (moji, clean, "Smith"):
        _person(con, nm)
    con.commit()
    col = scan_mojibake(con, [("persons", "name")])["per_column"][0]
    assert col["nonnull"] == 3
    assert col["suspected_mojibake"] == 1     # only the mis-decoded value
    assert col["non_ascii_total"] == 2        # mojibake + the legitimate accent
    # detect-not-fix: the corpus is unchanged
    names = [r[0] for r in con.execute("SELECT name FROM persons").fetchall()]
    assert moji in names


def test_replacement_char_flagged():
    con = dbmod.connect(":memory:")
    _person(con, "Bad" + chr(0xFFFD) + "name")
    con.commit()
    assert scan_mojibake(con, [("persons", "name")])["total_suspected_mojibake"] == 1


def test_euro_dash_prefix_flagged():
    con = dbmod.connect(":memory:")
    # mis-decoded curly apostrophe: U+00E2 U+0080 U+0099
    _person(con, "O" + chr(0xE2) + chr(0x80) + chr(0x99) + "Brien")
    con.commit()
    assert scan_mojibake(con, [("persons", "name")])["total_suspected_mojibake"] == 1


def test_pure_ascii_is_clean():
    con = dbmod.connect(":memory:")
    _person(con, "John Smith")
    con.commit()
    r = scan_mojibake(con, [("persons", "name")])
    assert r["total_suspected_mojibake"] == 0
    assert r["total_non_ascii"] == 0

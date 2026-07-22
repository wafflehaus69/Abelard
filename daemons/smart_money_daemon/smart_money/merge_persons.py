"""Idempotent migration: merge person rows that canonicalize to the same
politician (honorific/whitespace splits). Remaps congress_trades to the
survivor, sets a clean display name, deletes the dupes. Running twice is a
no-op. Deterministic: survivor = lowest person_id in each group."""
import argparse
import sys
from collections import defaultdict

from . import db as dbmod
from .names import canonical_key, display_name


def merge(con) -> dict:
    rows = con.execute("SELECT person_id, name FROM persons").fetchall()
    groups = defaultdict(list)
    for pid, name in rows:
        groups[canonical_key(name)].append((pid, name))

    merged_groups = 0
    remapped = 0
    for key, members in groups.items():
        if len(members) < 2:
            continue
        members.sort(key=lambda m: m[0])
        survivor = members[0][0]
        # cleanest display = fewest tokens among members, then lowest id
        best = min(members, key=lambda m: (len(display_name(m[1])), m[0]))
        # Remap + delete dupes FIRST so the survivor rename cannot collide with a
        # not-yet-deleted member holding the target name.
        for pid, _ in members[1:]:
            cur = con.execute(
                "UPDATE congress_trades SET person_id=? WHERE person_id=?",
                (survivor, pid),
            )
            remapped += cur.rowcount
            con.execute("DELETE FROM persons WHERE person_id=?", (pid,))
        con.execute(
            "UPDATE persons SET name=? WHERE person_id=?",
            (display_name(best[1]), survivor),
        )
        merged_groups += 1
    con.commit()
    return {"merged_groups": merged_groups, "trades_remapped": remapped}


def main(argv=None):
    ap = argparse.ArgumentParser(description="Merge duplicate person rows")
    ap.add_argument("--db", default=dbmod.DB_PATH_DEFAULT)
    args = ap.parse_args(argv)
    con = dbmod.connect(args.db)
    before = con.execute("SELECT COUNT(*) FROM persons").fetchone()[0]
    stats = merge(con)
    after = con.execute("SELECT COUNT(*) FROM persons").fetchone()[0]
    print(
        "[merge] persons {} -> {} | merged_groups={} trades_remapped={}".format(
            before, after, stats["merged_groups"], stats["trades_remapped"]
        )
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())

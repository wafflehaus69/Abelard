"""One-off: extract electronic PTR uuids from the browser-harvest tool-result
dump into data/raw/efd/senate_ptr_index_20260720.json. Regex over the
unescaped stream to sidestep the double-JSON encoding."""
import json
import pathlib
import re
import sys

SRC = pathlib.Path(sys.argv[1])
raw = SRC.read_text(encoding="utf-8")
# Raw text is triple-escaped: office\\\":\\\"...\\\",\\\"kind\\\":\\\"ptr\\\",
# \\\"uuid\\\":\\\"<uuid>\\\",\\\"date\\\":\\\"MM/DD/YYYY\\\"
esc = r'\\+"'  # one-or-more backslashes then a quote
pat = re.compile(
    r'office' + esc + r':' + esc + r'(.*?)' + esc + r','
    + esc + r'kind' + esc + r':' + esc + r'ptr' + esc + r','
    + esc + r'uuid' + esc + r':' + esc + r'([0-9a-f-]{36})' + esc + r','
    + esc + r'date' + esc + r':' + esc + r'(\d\d/\d\d/\d\d\d\d)'
)
objs = pat.findall(raw)
seen = set()
elec = []
for office, uuid, date in objs:
    if uuid and uuid not in seen:
        seen.add(uuid)
        elec.append({"uuid": uuid, "office": office, "filed": date})
out = pathlib.Path("data/raw/efd/senate_ptr_index_20260720.json")
out.parent.mkdir(parents=True, exist_ok=True)
out.write_text(
    json.dumps(
        {
            "harvested_via": "browser_datatables_api",
            "harvested_date": "2026-07-20",
            "matched_ptr_rows": len(objs),
            "unique_electronic_ptr": len(elec),
            "rows": elec,
        }
    )
)
print("objects_parsed=", len(objs), "unique_electronic_ptr=", len(elec), "wrote", out)

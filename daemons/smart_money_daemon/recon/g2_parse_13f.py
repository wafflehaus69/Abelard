"""G2 recon. Parse cached 13F information table XML, verify putCall column,
print per-issuer net directionality demo. Reads data/raw/recon/g2_infotable.xml."""
import collections
import re
import sys
import xml.etree.ElementTree as ET

PATH = "data/raw/recon/g2_infotable.xml"


def main() -> int:
    raw = open(PATH).read()
    raw = re.sub(r'xmlns="[^"]+"', "", raw, count=1)
    root = ET.fromstring(raw)

    rows = []
    for it in root.iter():
        if not it.tag.split("}")[-1] == "infoTable":
            continue

        def g(tag):
            for e in it.iter():
                if e.tag.split("}")[-1] == tag:
                    return (e.text or "").strip()
            return ""

        rows.append(
            {
                "issuer": g("nameOfIssuer"),
                "cusip": g("cusip"),
                "value": int(g("value") or 0),
                "shares": int(g("sshPrnamt") or 0),
                "type": g("sshPrnamtType"),
                "putCall": g("putCall"),
            }
        )

    if not rows:
        print("FAIL: zero infoTable rows parsed", file=sys.stderr)
        return 1

    print("rows:", len(rows))
    pc = collections.Counter(r["putCall"] or "(none)" for r in rows)
    print("putCall distribution:", dict(pc))

    net = collections.defaultdict(
        lambda: {"long_sh": 0, "long_v": 0, "call_v": 0, "put_v": 0}
    )
    for r in rows:
        k = r["issuer"]
        if r["putCall"] == "Put":
            net[k]["put_v"] += r["value"]
        elif r["putCall"] == "Call":
            net[k]["call_v"] += r["value"]
        else:
            net[k]["long_sh"] += r["shares"]
            net[k]["long_v"] += r["value"]

    print("net directionality demo (values USD):")
    for k, v in sorted(net.items()):
        net_opt = v["call_v"] - v["put_v"]
        print(
            "  {}: long_shares={} long_val={} call_val={} put_val={} net_opt={}".format(
                k, v["long_sh"], v["long_v"], v["call_v"], v["put_v"], net_opt
            )
        )
    return 0


if __name__ == "__main__":
    sys.exit(main())

"""Tiny markdown table formatter. Avoids the tabulate dependency."""


def md_table(df, floatfmt=".4f"):
    cols = list(df.columns)

    def cell(v):
        if v is None:
            return ""
        if isinstance(v, float):
            if v != v:  # NaN
                return ""
            return format(v, floatfmt)
        return str(v)

    lines = ["| " + " | ".join(str(c) for c in cols) + " |"]
    lines.append("|" + "|".join("---" for _ in cols) + "|")
    for _, row in df.iterrows():
        lines.append("| " + " | ".join(cell(row[c]) for c in cols) + " |")
    return "\n".join(lines)

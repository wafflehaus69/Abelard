"""Render a saved Full Brief envelope to human-readable text.

Loads a persisted FullBriefEnvelope JSON artifact and prints it through
`render_full_brief()` — the same render path the `news-watch-daemon
full-brief` CLI uses on a live run. Stdout is reconfigured to UTF-8 to
match the CLI fix-forward (commit 283ceb3) so Unicode characters in
the render (→, Δ) don't crash on Windows cp1252.

USAGE:
    python tools/render_brief.py PATH

    # Example: replay Run 1 of the Full Brief v1 live smoke
    python tools/render_brief.py ~/.openclaw/news_watch/briefs/2026-06/nwd-fullbrief-2026-06-02T03-36-41Z-a819cb69.json

EXIT CODES:
    0  — render succeeded
    1  — file not found, JSON parse error, or envelope validation error
    2  — argparse usage error (missing or extra arguments)

This is a stopgap operator tool until a proper `news-watch-daemon
read-brief` CLI subcommand lands (follow-up). It is NOT a daemon path
— assembly logic is not invoked, only render. The artifact must
already exist on disk.
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

# Add src/ to path so we can run as `python tools/render_brief.py`
# without installing first. Editable install (`pip install -e .`) also
# works.
_REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(_REPO_ROOT / "src"))

# UTF-8 reconfigure mirrors the CLI handler fix-forward (commit 283ceb3)
# so → (U+2192) and Δ (U+0394) in the render don't crash on Windows
# cp1252 default stdout encoding. errors="replace" matches CLI behavior.
if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
if hasattr(sys.stderr, "reconfigure"):
    sys.stderr.reconfigure(encoding="utf-8", errors="replace")

from news_watch_daemon.fullbrief.brief import FullBriefEnvelope  # noqa: E402
from news_watch_daemon.fullbrief.pdf import (  # noqa: E402
    PdfRenderError,
    render_full_brief_pdf,
)
from news_watch_daemon.fullbrief.render import render_full_brief  # noqa: E402


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Render a saved Full Brief envelope to human-readable text.",
    )
    parser.add_argument(
        "path",
        type=str,
        help="Path to a Full Brief envelope JSON artifact. ~ is expanded.",
    )
    parser.add_argument(
        "--pdf",
        metavar="OUT.pdf",
        help="Render to a PDF at this path (ReportLab, Unicode-safe) instead "
             "of text to stdout. Fails loud; never a zero-byte PDF.",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(argv)
    path = Path(args.path).expanduser()

    if not path.exists():
        print(f"error: file not found: {path}", file=sys.stderr)
        return 1

    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        print(f"error: not valid JSON ({path}): {exc}", file=sys.stderr)
        return 1

    try:
        envelope = FullBriefEnvelope.model_validate(data)
    except Exception as exc:
        # Pydantic ValidationError or any other parse-shape failure.
        # The message itself is the diagnostic.
        print(f"error: not a FullBriefEnvelope ({path}): {exc}", file=sys.stderr)
        return 1

    if args.pdf:
        try:
            written = render_full_brief_pdf(envelope, args.pdf)
        except PdfRenderError as exc:
            print(f"error: PDF render failed: {exc}", file=sys.stderr)
            return 1
        print(f"Wrote PDF: {written} ({written.stat().st_size} bytes)")
        return 0

    print(render_full_brief(envelope))
    return 0


if __name__ == "__main__":
    sys.exit(main())

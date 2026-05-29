"""Build manifest.json + release_notes.md summarising a collected Lighter dataset.

Scans the staged OHLCV feathers (any timeframe) and funding parquets, computes
per-asset file counts and date coverage, and emits:

* ``manifest.json`` — machine-readable coverage, attached to the release.
* ``release_notes.md`` — a human-readable timerange table used as the release body.

Usage::

    python collector/make_manifest.py --futures _staging/futures --funding _staging/funding \
        --out dist/manifest.json --notes dist/release_notes.md

:author: lighter-historical-data
"""

from __future__ import annotations

import argparse
import json
import logging
import re
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

logger = logging.getLogger(__name__)

#: Match ``{TOKEN}_{QUOTE}_{SETTLE}-{tf}-futures.feather`` and capture the timeframe.
_TF_RE = re.compile(r"-(?P<tf>\d+[mhdw])-futures\.feather$")

#: Coarse-to-fine ordering for stable display.
_TF_ORDER = ["1w", "1d", "12h", "4h", "1h", "30m", "15m", "5m", "1m"]


def _coverage(paths: list[Path], date_col: str = "date") -> dict[str, object]:
    """Summarise file count + date span across feather/parquet files.

    :param paths: Files to scan.
    :param date_col: Timestamp column name; falls back to the index if absent.
    :returns: ``{files, min_date, max_date}`` (ISO strings or None).
    """
    lo: pd.Timestamp | None = None
    hi: pd.Timestamp | None = None
    for p in paths:
        try:
            df = pd.read_feather(p) if p.suffix == ".feather" else pd.read_parquet(p)
        except (OSError, ValueError):
            continue
        col = pd.to_datetime(df[date_col] if date_col in df.columns else df.index, utc=True)
        if len(col):
            lo = col.min() if lo is None else min(lo, col.min())
            hi = col.max() if hi is None else max(hi, col.max())
    return {
        "files": len(paths),
        "min_date": lo.isoformat() if lo is not None else None,
        "max_date": hi.isoformat() if hi is not None else None,
    }


def main() -> None:
    """Write manifest.json + release_notes.md for the publish step."""
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    parser = argparse.ArgumentParser(description="Build dataset manifest + release notes.")
    parser.add_argument("--futures", type=Path, required=True, help="Staged OHLCV feather dir.")
    parser.add_argument("--funding", type=Path, required=True, help="Staged funding parquet dir.")
    parser.add_argument("--out", type=Path, required=True, help="manifest.json output path.")
    parser.add_argument("--notes", type=Path, required=True, help="release_notes.md output path.")
    args = parser.parse_args()

    # Group OHLCV feathers by timeframe.
    by_tf: dict[str, list[Path]] = {}
    if args.futures.exists():
        for p in sorted(args.futures.glob("*-futures.feather")):
            m = _TF_RE.search(p.name)
            if m:
                by_tf.setdefault(m.group("tf"), []).append(p)
    funding = sorted(args.funding.glob("*.parquet")) if args.funding.exists() else []

    ordered_tfs = [tf for tf in _TF_ORDER if tf in by_tf] + sorted(set(by_tf) - set(_TF_ORDER))
    assets: dict[str, dict[str, object]] = {f"lighter-{tf}.zip": _coverage(by_tf[tf]) for tf in ordered_tfs}
    assets["lighter-funding.zip"] = _coverage(funding)

    manifest = {
        "exchange": "lighter",
        "venue_host": "mainnet.zklighter.elliot.ai",
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "history": "full — collected from Lighter genesis to run time (no truncation)",
        "format": {
            "ohlcv": "Freqtrade feather: {TOKEN}_{QUOTE}_{SETTLE}-{tf}-futures.feather, cols date,open,high,low,close,volume",
            "funding": "parquet: lighter_funding_rate_lighter_{TOKEN}_1h_{start}_{end}.parquet, DatetimeIndex 'time' + funding_rate/value/direction",
        },
        "assets": assets,
    }
    args.out.parent.mkdir(parents=True, exist_ok=True)
    args.out.write_text(json.dumps(manifest, indent=2))

    # Human-readable release notes with the timerange of every asset.
    def _d(iso: object) -> str:
        return str(iso)[:10] if iso else "—"

    lines = [
        "**Full historical Lighter dataset** — regenerated automatically (daily).",
        "",
        f"Generated: `{manifest['generated_at'][:19]}Z` · venue `mainnet.zklighter.elliot.ai`",
        "",
        "| Asset | Markets | From | To |",
        "|---|---|---|---|",
    ]
    for name, cov in assets.items():
        lines.append(f"| `{name}` | {cov['files']} | {_d(cov['min_date'])} | {_d(cov['max_date'])} |")
    lines += [
        "",
        "Download: `gh release download latest -R Ankvik-Tech-Labs/lighter-historical-data`",
        "or a single asset at "
        "`https://github.com/Ankvik-Tech-Labs/lighter-historical-data/releases/latest/download/<asset>`.",
        "",
        "OHLCV = Freqtrade feathers (`date,open,high,low,close,volume`); funding = hourly parquet. "
        "See `manifest.json` for exact ISO timestamps.",
    ]
    args.notes.parent.mkdir(parents=True, exist_ok=True)
    args.notes.write_text("\n".join(lines))

    logger.info("Wrote %s and %s", args.out, args.notes)
    logger.info(json.dumps(assets, indent=2))


if __name__ == "__main__":
    main()

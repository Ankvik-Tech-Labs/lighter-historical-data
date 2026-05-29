"""Build a manifest.json summarising a collected Lighter dataset.

Scans the staged OHLCV feathers and funding parquets and emits per-category
file counts plus the overall date coverage, so consumers of the rolling
``latest`` release can see what they're getting without downloading everything.

Usage::

    python collector/make_manifest.py --futures _staging/futures --funding _staging/funding --out dist/manifest.json

:author: lighter-historical-data
"""

from __future__ import annotations

import argparse
import json
import logging
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

logger = logging.getLogger(__name__)


def _coverage(paths: list[Path], date_col: str = "date") -> dict[str, object]:
    """Summarise row count and date span across a set of feather/parquet files.

    :param paths: Files to scan (feather or parquet).
    :param date_col: Name of the timestamp column / index to derive the span from.
    :returns: Dict with ``files``, ``min_date``, ``max_date`` (ISO strings or None).
    """
    lo: pd.Timestamp | None = None
    hi: pd.Timestamp | None = None
    for p in paths:
        try:
            df = pd.read_feather(p) if p.suffix == ".feather" else pd.read_parquet(p)
        except (OSError, ValueError):
            continue
        if date_col in df.columns:
            col = pd.to_datetime(df[date_col], utc=True)
        else:
            col = pd.to_datetime(df.index, utc=True)
        if len(col):
            lo = col.min() if lo is None else min(lo, col.min())
            hi = col.max() if hi is None else max(hi, col.max())
    return {
        "files": len(paths),
        "min_date": lo.isoformat() if lo is not None else None,
        "max_date": hi.isoformat() if hi is not None else None,
    }


def main() -> None:
    """Write the manifest JSON for the publish step."""
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    parser = argparse.ArgumentParser(description="Build the dataset manifest.json.")
    parser.add_argument("--futures", type=Path, required=True, help="Staged OHLCV feather dir.")
    parser.add_argument("--funding", type=Path, required=True, help="Staged funding parquet dir.")
    parser.add_argument("--out", type=Path, required=True, help="Output manifest path.")
    args = parser.parse_args()

    fut = sorted(args.futures.glob("*.feather")) if args.futures.exists() else []
    d1 = [p for p in fut if p.name.endswith("-1d-futures.feather")]
    m15 = [p for p in fut if p.name.endswith("-15m-futures.feather")]
    fund = sorted(args.funding.glob("*.parquet")) if args.funding.exists() else []

    manifest = {
        "exchange": "lighter",
        "venue_host": "mainnet.zklighter.elliot.ai",
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "format": {
            "ohlcv": "Freqtrade feather: {TOKEN}_{QUOTE}_{SETTLE}-{tf}-futures.feather, cols date,open,high,low,close,volume",
            "funding": "parquet: lighter_funding_rate_lighter_{TOKEN}_1h_{start}_{end}.parquet, DatetimeIndex 'time' + funding_rate/value/direction",
        },
        "assets": {
            "lighter-1d.zip": _coverage(d1),
            "lighter-15m.zip": _coverage(m15),
            "lighter-funding.zip": _coverage(fund),
        },
    }
    args.out.parent.mkdir(parents=True, exist_ok=True)
    args.out.write_text(json.dumps(manifest, indent=2))
    logger.info("Wrote %s", args.out)
    logger.info(json.dumps(manifest["assets"], indent=2))


if __name__ == "__main__":
    main()

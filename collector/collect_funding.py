#!/usr/bin/env python3
"""Standalone Lighter DEX funding-rate collector.

This module is a self-contained backfill tool for Lighter perpetual funding
rates, designed to run unattended inside GitHub Actions matrix jobs (one shard
per runner / IP).  It depends only on the Python standard library plus
``ccxt``, ``pandas``, ``pyarrow``, ``rich`` and ``requests``.

Data source
-----------
The collector reads the public Lighter mainnet REST endpoint::

    GET https://mainnet.zklighter.elliot.ai/api/v1/fundings

Query parameters:

* ``market_id`` (int)        — numeric Lighter market identifier.
* ``resolution`` (str)       — funding resolution; Lighter funding is hourly,
                               so ``"1h"`` is the only sensible value.
* ``start_timestamp`` (ms)   — window start, Unix milliseconds.
* ``end_timestamp`` (ms)     — window end, Unix milliseconds.
* ``count_back`` (int ≤ 500) — maximum records to return.

Response payload::

    {
      "code": 200,
      "resolution": "1h",
      "fundings": [
        {"timestamp": 1778850000, "value": "0.55124860",
         "rate": "0.0007", "direction": "long"},
        ...
      ]
    }

.. warning::
   The response ``timestamp`` is expressed in **seconds**, not milliseconds;
   it is multiplied by 1000 before being handed to pandas.

Because each call returns at most 500 records, the collector paginates with
explicit ``(start, end)`` windows — the same shape as a candle backfill — and
de-duplicates by timestamp.  Earliest available data is roughly 2025-01.

Sign convention
----------------
The Lighter ``/fundings`` REST endpoint returns ``rate`` as an **unsigned
magnitude** together with a ``direction`` discriminator (``"long"`` /
``"short"``).  The NautilusTrader Lighter adapter
(``adapters/lighter/parsers.py::parse_market_stats_to_funding_rate``) consumes
the *WebSocket* ``market_stats`` feed instead, where the field is
``current_funding_rate`` — already a **signed** decimal string — so the adapter
does no direction-based sign reconstruction::

    rate = Decimal(str(stats.get("current_funding_rate", "0")))
    ...
    return FundingRateUpdate(instrument_id=instrument_id, rate=rate, ...)

Since the adapter does not disambiguate the REST ``direction`` field, this
collector adopts the documented default convention:

    ``funding_rate = float(rate) * (+1 if direction == "long" else -1)``

i.e. ``direction == "long"`` (longs pay shorts) yields a **positive** funding
rate; any other direction yields a **negative** funding rate.  This matches the
sign meaning of the adapter's signed ``current_funding_rate`` (positive ⇒ longs
pay) and the downstream loader's ``df["funding_rate"].resample("4h").sum()``
expectation.

Output
------
One parquet file per market is written to ``--out``::

    lighter_funding_rate_lighter_{TOKEN}_1h_{start}_{end}.parquet

where ``TOKEN`` is the upper-cased base symbol (e.g. ``BTC``) and ``start`` /
``end`` are ``YYYYMMDD`` stamps of the first / last observation.  The parquet
holds a pandas ``DataFrame`` with a tz-aware ``DatetimeIndex`` named ``time``
(UTC) and three columns: ``funding_rate`` (signed float), ``value`` (float) and
``direction`` (str).  This schema is what
``nautilus-strategies/common/data/funding_loader.py`` expects.

Usage
-----
.. code-block:: bash

    python collector/collect_funding.py --shard 0/8
"""
from __future__ import annotations

import argparse
import logging
import threading
import time
from collections import deque
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import ccxt
import pandas as pd
import requests
from rich.logging import RichHandler
from rich.progress import (
    BarColumn,
    MofNCompleteColumn,
    Progress,
    TextColumn,
    TimeElapsedColumn,
)

#: Public Lighter mainnet funding endpoint.
FUNDINGS_URL = "https://mainnet.zklighter.elliot.ai/api/v1/fundings"

#: Maximum records returned by a single ``/fundings`` call.
MAX_COUNT_BACK = 500

#: Window size (hours) requested per HTTP call. At 1h resolution this maps
#: 1:1 to records, so 500h stays at or below ``MAX_COUNT_BACK``.
WINDOW_HOURS = MAX_COUNT_BACK

#: Hours in milliseconds — pagination step helper.
_HOUR_MS = 3_600_000

#: Earliest plausible Lighter funding data (2025-01-01 UTC, ms).
EARLIEST_START_MS = int(
    datetime(2025, 1, 1, tzinfo=timezone.utc).timestamp() * 1000
)

#: HTTP request timeout (seconds).
HTTP_TIMEOUT = 30.0

#: Retry attempts per HTTP window.
MAX_RETRIES = 4

logging.basicConfig(
    level=logging.INFO,
    format="%(message)s",
    datefmt="[%X]",
    handlers=[RichHandler(rich_tracebacks=True, show_path=False)],
)
logger = logging.getLogger(__name__)


class RateLimiter:
    """Thread-safe sliding-window token-bucket rate limiter.

    Permits at most ``max_calls`` acquisitions within any rolling
    ``period_s``-second window.  ``acquire`` blocks the calling thread until a
    slot is free, so it is safe to share a single instance across all worker
    threads and to call it before *every* outbound HTTP request.

    :param max_calls: Maximum number of acquisitions allowed per window.
    :param period_s: Length of the sliding window, in seconds.
    """

    def __init__(self, max_calls: int, period_s: float) -> None:
        #: Maximum acquisitions per sliding window.
        self._max_calls = max(1, int(max_calls))
        #: Sliding-window length in seconds.
        self._period_s = float(period_s)
        #: Monotonic timestamps of recent acquisitions.
        self._calls: deque[float] = deque()
        #: Guards mutation of ``_calls``.
        self._lock = threading.Lock()

    def acquire(self) -> None:
        """Block until a request slot is available, then consume it.

        :returns: ``None``.  Returns once a slot has been reserved for the
            caller.
        """
        while True:
            with self._lock:
                now = time.monotonic()
                cutoff = now - self._period_s
                while self._calls and self._calls[0] <= cutoff:
                    self._calls.popleft()
                if len(self._calls) < self._max_calls:
                    self._calls.append(now)
                    return
                # Sleep until the oldest call ages out of the window.
                sleep_for = self._calls[0] + self._period_s - now
            if sleep_for > 0:
                time.sleep(sleep_for)


@dataclass(slots=True)
class PerpMarket:
    """A Lighter perpetual market to backfill.

    :param symbol: ccxt unified symbol, e.g. ``"BTC/USDC:USDC"``.
    :param market_id: Numeric Lighter market identifier.
    :param token: Upper-cased base token used in the output filename.
    """

    #: ccxt unified symbol (e.g. ``"BTC/USDC:USDC"``).
    symbol: str
    #: Numeric Lighter market identifier.
    market_id: int
    #: Upper-cased base token (e.g. ``"BTC"``).
    token: str


@dataclass(slots=True)
class FundingRow:
    """A single parsed funding observation.

    :param ts_ms: Observation time in Unix milliseconds (UTC).
    :param funding_rate: Signed funding rate (see module docstring).
    :param value: Raw ``value`` field from the API, as a float.
    :param direction: Raw ``direction`` discriminator (``"long"`` / ``"short"``).
    """

    #: Observation time in Unix milliseconds (UTC).
    ts_ms: int
    #: Signed funding rate.
    funding_rate: float
    #: Raw ``value`` field as float.
    value: float
    #: Raw ``direction`` discriminator.
    direction: str


@dataclass(slots=True)
class CollectorConfig:
    """Resolved CLI configuration for a collection run.

    :param out_dir: Output directory for parquet files.
    :param resolution: Funding resolution string (``"1h"``).
    :param workers: Thread-pool size.
    :param rate: Maximum requests per minute (shared across workers).
    :param shard: Optional ``(index, total)`` shard selector.
    :param symbols: Optional explicit symbol allowlist.
    """

    #: Output directory for parquet files.
    out_dir: Path
    #: Funding resolution string.
    resolution: str = "1h"
    #: Thread-pool size.
    workers: int = 8
    #: Maximum requests per minute (shared across workers).
    rate: int = 55
    #: Optional ``(index, total)`` shard selector.
    shard: tuple[int, int] | None = None
    #: Optional explicit symbol allowlist.
    symbols: list[str] | None = field(default=None)


def _signed_rate(rate: str, direction: str) -> float:
    """Apply the Lighter direction-to-sign convention.

    The REST ``rate`` is an unsigned magnitude; ``direction`` carries the sign.
    ``direction == "long"`` (longs pay shorts) maps to a positive funding rate;
    any other value maps to negative.  This mirrors the signed semantics of the
    adapter's WebSocket ``current_funding_rate`` (positive ⇒ longs pay).

    :param rate: Unsigned funding rate magnitude as a decimal string.
    :param direction: Direction discriminator (``"long"`` / ``"short"``).
    :returns: Signed funding rate as a float.
    """
    magnitude = float(rate)
    sign = 1.0 if str(direction).lower() == "long" else -1.0
    return magnitude * sign


def fetch_perp_markets(rate_limiter: RateLimiter) -> list[PerpMarket]:
    """Load Lighter perpetual markets via ccxt.

    Calls ``ccxt.lighter().load_markets()`` and keeps only swap (perp) markets
    whose symbol ends in ``":USDC"``.  The numeric market id is read from
    ``market["info"]["market_id"]`` with a fallback to ``market["id"]``.

    :param rate_limiter: Shared limiter; one slot is consumed for the market
        load (which performs network I/O).
    :returns: List of :class:`PerpMarket`, sorted by symbol for deterministic
        sharding.
    :raises RuntimeError: If no perpetual markets can be resolved.
    """
    rate_limiter.acquire()
    exchange = ccxt.lighter()
    markets = exchange.load_markets()

    perps: list[PerpMarket] = []
    for symbol, market in markets.items():
        if not symbol.endswith(":USDC"):
            continue
        if not (market.get("swap") or market.get("type") == "swap"):
            continue
        info = market.get("info", {}) or {}
        raw_id: Any = info.get("market_id", market.get("id"))
        if raw_id is None:
            logger.warning("Skipping %s: no market_id resolvable", symbol)
            continue
        try:
            market_id = int(raw_id)
        except (TypeError, ValueError):
            logger.warning("Skipping %s: market_id %r not an int", symbol, raw_id)
            continue
        base = market.get("base") or symbol.split("/")[0]
        perps.append(
            PerpMarket(symbol=symbol, market_id=market_id, token=str(base).upper())
        )

    if not perps:
        raise RuntimeError("No Lighter perpetual markets resolved from ccxt")

    perps.sort(key=lambda m: m.symbol)
    return perps


def select_markets(
    perps: list[PerpMarket],
    shard: tuple[int, int] | None,
    symbols: list[str] | None,
) -> list[PerpMarket]:
    """Filter markets by explicit symbol list and/or deterministic shard.

    The ``--symbols`` allowlist is applied first (matching against the unified
    symbol, the base token, or a base-token prefix), then the shard selector
    keeps every market whose position in the sorted list satisfies
    ``index % total == shard_index``.

    :param perps: Full sorted list of perpetual markets.
    :param shard: Optional ``(index, total)`` selector.
    :param symbols: Optional explicit symbol/token allowlist.
    :returns: The filtered, still-sorted subset of markets.
    """
    selected = perps
    if symbols:
        wanted = {s.upper() for s in symbols}
        selected = [
            m
            for m in selected
            if m.symbol.upper() in wanted
            or m.token in wanted
            or m.symbol.split("/")[0].upper() in wanted
        ]

    if shard is not None:
        shard_index, shard_total = shard
        selected = [
            m for i, m in enumerate(selected) if i % shard_total == shard_index
        ]

    return selected


def _request_window(
    session: requests.Session,
    rate_limiter: RateLimiter,
    market_id: int,
    resolution: str,
    start_ms: int,
    end_ms: int,
) -> list[dict[str, Any]]:
    """Fetch one ``(start, end)`` funding window with retries.

    A rate-limiter slot is consumed before every HTTP attempt, including
    retries, so the shared cap is honoured globally.

    :param session: Reusable HTTP session.
    :param rate_limiter: Shared sliding-window limiter.
    :param market_id: Numeric Lighter market identifier.
    :param resolution: Funding resolution string.
    :param start_ms: Window start, Unix milliseconds.
    :param end_ms: Window end, Unix milliseconds.
    :returns: Raw ``fundings`` list (possibly empty) for the window.
    :raises requests.RequestException: If all retry attempts fail.
    """
    params = {
        "market_id": market_id,
        "resolution": resolution,
        "start_timestamp": start_ms,
        "end_timestamp": end_ms,
        "count_back": MAX_COUNT_BACK,
    }
    last_exc: Exception | None = None
    for attempt in range(1, MAX_RETRIES + 1):
        rate_limiter.acquire()
        try:
            resp = session.get(FUNDINGS_URL, params=params, timeout=HTTP_TIMEOUT)
            if resp.status_code == 429:
                backoff = min(2.0 ** attempt, 30.0)
                logger.warning(
                    "429 for market_id=%s; backing off %.1fs (attempt %d/%d)",
                    market_id,
                    backoff,
                    attempt,
                    MAX_RETRIES,
                )
                time.sleep(backoff)
                continue
            resp.raise_for_status()
            payload = resp.json()
            return list(payload.get("fundings", []) or [])
        except requests.RequestException as exc:
            last_exc = exc
            backoff = min(2.0 ** attempt, 30.0)
            logger.warning(
                "HTTP error for market_id=%s (attempt %d/%d): %s; retry in %.1fs",
                market_id,
                attempt,
                MAX_RETRIES,
                exc,
                backoff,
            )
            time.sleep(backoff)

    raise requests.RequestException(
        f"Exhausted retries for market_id={market_id} "
        f"[{start_ms}, {end_ms}]: {last_exc}"
    )


def fetch_funding_history(
    session: requests.Session,
    rate_limiter: RateLimiter,
    market: PerpMarket,
    resolution: str,
    start_ms: int,
    end_ms: int,
) -> list[FundingRow]:
    """Paginate the full funding history for one market.

    Walks ``(start, end)`` windows of :data:`WINDOW_HOURS` hours from
    ``start_ms`` to ``end_ms``, parses and signs each record, and de-duplicates
    by timestamp.

    :param session: Reusable HTTP session.
    :param rate_limiter: Shared sliding-window limiter.
    :param market: Market to backfill.
    :param resolution: Funding resolution string.
    :param start_ms: Backfill start, Unix milliseconds.
    :param end_ms: Backfill end, Unix milliseconds.
    :returns: Chronologically-sorted, de-duplicated funding rows.
    """
    by_ts: dict[int, FundingRow] = {}
    window_ms = WINDOW_HOURS * _HOUR_MS
    cursor = start_ms
    while cursor < end_ms:
        win_end = min(cursor + window_ms, end_ms)
        records = _request_window(
            session, rate_limiter, market.market_id, resolution, cursor, win_end
        )
        for rec in records:
            try:
                ts_ms = int(rec["timestamp"]) * 1000  # API timestamp is SECONDS
                row = FundingRow(
                    ts_ms=ts_ms,
                    funding_rate=_signed_rate(rec["rate"], rec.get("direction", "")),
                    value=float(rec.get("value", "nan")),
                    direction=str(rec.get("direction", "")),
                )
            except (KeyError, TypeError, ValueError) as exc:
                logger.warning(
                    "Skipping malformed record for %s: %r (%s)",
                    market.symbol,
                    rec,
                    exc,
                )
                continue
            by_ts[row.ts_ms] = row
        cursor = win_end

    return [by_ts[ts] for ts in sorted(by_ts)]


def rows_to_frame(rows: list[FundingRow]) -> pd.DataFrame:
    """Convert parsed rows into the loader-compatible DataFrame.

    The result has a tz-aware UTC ``DatetimeIndex`` named ``time`` and the
    columns ``funding_rate`` (signed float), ``value`` (float) and
    ``direction`` (str).

    :param rows: Chronologically-sorted funding rows.
    :returns: DataFrame ready to be written as parquet.
    """
    index = pd.to_datetime(
        [r.ts_ms for r in rows], unit="ms", utc=True
    )
    index.name = "time"
    frame = pd.DataFrame(
        {
            "funding_rate": [r.funding_rate for r in rows],
            "value": [r.value for r in rows],
            "direction": [r.direction for r in rows],
        },
        index=index,
    )
    return frame


def write_parquet(out_dir: Path, market: PerpMarket, frame: pd.DataFrame) -> Path:
    """Write a market's funding frame to parquet with the canonical name.

    :param out_dir: Destination directory (created if absent).
    :param market: Market the frame belongs to.
    :param frame: Loader-compatible funding DataFrame.
    :returns: Path of the written parquet file.
    """
    out_dir.mkdir(parents=True, exist_ok=True)
    start_stamp = frame.index[0].strftime("%Y%m%d")
    end_stamp = frame.index[-1].strftime("%Y%m%d")
    name = (
        f"lighter_funding_rate_lighter_{market.token}_1h_"
        f"{start_stamp}_{end_stamp}.parquet"
    )
    path = out_dir / name
    frame.to_parquet(path, engine="pyarrow")
    return path


def collect_one(
    session: requests.Session,
    rate_limiter: RateLimiter,
    config: CollectorConfig,
    market: PerpMarket,
    start_ms: int,
    end_ms: int,
) -> Path | None:
    """Backfill and persist funding history for a single market.

    Wraps :func:`fetch_funding_history`, :func:`rows_to_frame` and
    :func:`write_parquet`.  Returns ``None`` when no data is available.

    :param session: Reusable HTTP session.
    :param rate_limiter: Shared sliding-window limiter.
    :param config: Resolved run configuration.
    :param market: Market to backfill.
    :param start_ms: Backfill start, Unix milliseconds.
    :param end_ms: Backfill end, Unix milliseconds.
    :returns: Written parquet path, or ``None`` if there was no data.
    """
    rows = fetch_funding_history(
        session, rate_limiter, market, config.resolution, start_ms, end_ms
    )
    if not rows:
        logger.info("%s: no funding records in range", market.symbol)
        return None
    frame = rows_to_frame(rows)
    path = write_parquet(config.out_dir, market, frame)
    logger.info("%s: wrote %d rows -> %s", market.symbol, len(frame), path.name)
    return path


def run(config: CollectorConfig) -> None:
    """Execute a full collection run.

    Resolves markets, applies symbol/shard filtering, then fans the per-market
    backfills out across a :class:`~concurrent.futures.ThreadPoolExecutor`
    bounded by a single shared :class:`RateLimiter`.  Each market is wrapped in
    its own try/except so one failure does not abort the run.

    :param config: Resolved run configuration.
    :returns: ``None``.
    """
    rate_limiter = RateLimiter(config.rate, 60.0)
    perps = fetch_perp_markets(rate_limiter)
    markets = select_markets(perps, config.shard, config.symbols)

    logger.info(
        "Resolved %d perp markets; collecting %d after shard/symbol filter",
        len(perps),
        len(markets),
    )
    if not markets:
        logger.warning("No markets selected; nothing to do")
        return

    start_ms = EARLIEST_START_MS
    end_ms = int(time.time() * 1000)

    session = requests.Session()
    session.headers.update({"User-Agent": "lighter-historical-data/collect_funding"})

    failures = 0
    progress = Progress(
        TextColumn("[progress.description]{task.description}"),
        BarColumn(),
        MofNCompleteColumn(),
        TextColumn("[progress.percentage]{task.percentage:>3.0f}%"),
        TimeElapsedColumn(),
    )
    with progress:
        task = progress.add_task("Funding backfill", total=len(markets))
        with ThreadPoolExecutor(max_workers=config.workers) as pool:
            future_to_market = {
                pool.submit(
                    collect_one,
                    session,
                    rate_limiter,
                    config,
                    market,
                    start_ms,
                    end_ms,
                ): market
                for market in markets
            }
            for future in as_completed(future_to_market):
                market = future_to_market[future]
                try:
                    future.result()
                except Exception as exc:  # noqa: BLE001 — per-pair isolation
                    failures += 1
                    logger.exception("Failed to collect %s: %s", market.symbol, exc)
                finally:
                    progress.advance(task)

    logger.info(
        "Done: %d markets attempted, %d failed", len(markets), failures
    )


def parse_shard(value: str) -> tuple[int, int]:
    """Parse a ``"I/N"`` shard specifier.

    :param value: Shard string, e.g. ``"0/8"``.
    :returns: ``(index, total)`` tuple.
    :raises argparse.ArgumentTypeError: If the format is invalid or out of range.
    """
    try:
        index_str, total_str = value.split("/", 1)
        index, total = int(index_str), int(total_str)
    except (ValueError, AttributeError) as exc:
        raise argparse.ArgumentTypeError(
            f"--shard must look like 'I/N', got {value!r}"
        ) from exc
    if total <= 0 or not (0 <= index < total):
        raise argparse.ArgumentTypeError(
            f"--shard out of range: need 0 <= I < N and N > 0, got {value!r}"
        )
    return index, total


def parse_args(argv: list[str] | None = None) -> CollectorConfig:
    """Parse command-line arguments into a :class:`CollectorConfig`.

    :param argv: Optional explicit argument vector (defaults to ``sys.argv``).
    :returns: Resolved configuration.
    """
    parser = argparse.ArgumentParser(
        description="Backfill Lighter DEX perpetual funding rates to parquet.",
    )
    parser.add_argument(
        "--out",
        type=Path,
        default=Path("data/funding"),
        help="Output directory for parquet files (default: data/funding).",
    )
    parser.add_argument(
        "--resolution",
        default="1h",
        help="Funding resolution (default: 1h; Lighter funding is hourly).",
    )
    parser.add_argument(
        "--workers",
        type=int,
        default=8,
        help="Thread-pool size (default: 8).",
    )
    parser.add_argument(
        "--rate",
        type=int,
        default=55,
        help="Max requests per minute, shared across workers (default: 55).",
    )
    parser.add_argument(
        "--shard",
        type=parse_shard,
        default=None,
        help="Deterministic shard 'I/N' of sorted perp symbols (e.g. 0/8).",
    )
    parser.add_argument(
        "--symbols",
        nargs="+",
        default=None,
        help="Explicit symbol/token allowlist (e.g. BTC ETH or BTC/USDC:USDC).",
    )
    args = parser.parse_args(argv)
    return CollectorConfig(
        out_dir=args.out,
        resolution=args.resolution,
        workers=args.workers,
        rate=args.rate,
        shard=args.shard,
        symbols=args.symbols,
    )


def main(argv: list[str] | None = None) -> None:
    """CLI entry point.

    :param argv: Optional explicit argument vector (defaults to ``sys.argv``).
    :returns: ``None``.
    """
    config = parse_args(argv)
    run(config)


if __name__ == "__main__":
    main()

"""Standalone Lighter DEX OHLCV collector for GitHub Actions.

Lighter is a zk-rollup order-book perpetuals DEX exposed as a first-class CCXT
exchange (``ccxt.lighter()``, CCXT >= 4.5). Its public ``GET /api/v1/candles``
endpoint requires no authentication. This script downloads candles for every
Lighter perpetual market (or an explicit subset / deterministic shard) and
writes them as Freqtrade-compatible feathers.

The script is fully self-contained: it depends only on the Python standard
library plus ``ccxt``, ``pandas``, ``pyarrow`` and ``rich``. It imports nothing
from any surrounding repository, so it can be dropped into a public demo repo
and run directly in a GitHub Actions matrix.

Throughput is bounded by Lighter's **60 requests/min per IP** limit. A shared
sliding-window token-bucket :class:`RateLimiter` gates every HTTP call so a
thread pool (``--workers``) can overlap network round-trips and approach — but
never exceed — that ceiling. The candles endpoint returns at most 500 candles
per call and anchors its window on ``count_back``/``end_timestamp`` rather than
honouring ``since``, so we page with explicit ``(since, until)`` windows and
de-duplicate by timestamp. Earliest data is ~2024-12-01.

Usage::

    python collector/collect_ohlcv.py --timeframe 1d
    python collector/collect_ohlcv.py --timeframe 1d --shard 0/8 --workers 8
    python collector/collect_ohlcv.py --timeframe 15m --symbols BTC/USDC:USDC ETH/USDC:USDC

GitHub Actions can fan out across runners (each with a distinct egress IP) by
giving every matrix job a different ``--shard I/N``.
"""

from __future__ import annotations

import argparse
import logging
import threading
import time
from collections import deque
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from pathlib import Path

import ccxt
import pandas as pd
from rich.logging import RichHandler
from rich.progress import (
    BarColumn,
    MofNCompleteColumn,
    Progress,
    SpinnerColumn,
    TextColumn,
    TimeElapsedColumn,
    TimeRemainingColumn,
)

logger = logging.getLogger(__name__)

#: Milliseconds per candle for each supported timeframe.
TIMEFRAME_MS: dict[str, int] = {
    "1m": 60 * 1000,
    "5m": 5 * 60 * 1000,
    "15m": 15 * 60 * 1000,
    "30m": 30 * 60 * 1000,
    "1h": 60 * 60 * 1000,
    "4h": 4 * 60 * 60 * 1000,
    "12h": 12 * 60 * 60 * 1000,
    "1d": 24 * 60 * 60 * 1000,
    "1w": 7 * 24 * 60 * 60 * 1000,
}

#: Lighter mainnet has no candles before this instant; used as the page anchor.
EARLIEST_MS: int = ccxt.lighter().parse8601("2024-12-01T00:00:00Z")

#: Maximum candles Lighter returns per ``fetch_ohlcv`` call.
PAGE_LIMIT: int = 500

#: Per-window fetch attempts before giving up (guards against transient 429/5xx/timeout
#: leaving data gaps); exponential backoff between attempts.
MAX_RETRIES: int = 4
#: Base backoff in seconds (attempt n waits ``RETRY_BACKOFF_S * 2**n``).
RETRY_BACKOFF_S: float = 1.0

#: Thread-local store for per-thread CCXT instances (ccxt is not thread-safe to share).
_TLS = threading.local()


@dataclass(slots=True)
class PairResult:
    """Outcome of collecting one symbol.

    :ivar pair: CCXT unified symbol, e.g. ``BTC/USDC:USDC``.
    :ivar rows: Number of candle rows written (0 if no data / failed).
    :ivar start: Timestamp of the first candle, or ``None``.
    :ivar end: Timestamp of the last candle, or ``None``.
    """

    #: CCXT unified symbol, e.g. ``BTC/USDC:USDC``.
    pair: str
    #: Number of candle rows written.
    rows: int
    #: Timestamp of the first candle, or ``None`` when empty.
    start: pd.Timestamp | None
    #: Timestamp of the last candle, or ``None`` when empty.
    end: pd.Timestamp | None


class RateLimiter:
    """Shared sliding-window token bucket: at most ``max_calls`` per ``period`` seconds.

    Thread-safe; :meth:`acquire` blocks until a slot frees. Enforces the global
    Lighter IP rate limit across all worker threads.

    :param max_calls: Maximum requests allowed within the window.
    :param period: Window length in seconds.
    """

    def __init__(self, max_calls: int, period: float = 60.0) -> None:
        self._max = max_calls
        self._period = period
        self._calls: deque[float] = deque()
        self._lock = threading.Lock()

    def acquire(self) -> None:
        """Block until a request slot is available, then consume it.

        :returns: ``None``; returns once a slot has been reserved.
        """
        while True:
            with self._lock:
                now = time.monotonic()
                while self._calls and self._calls[0] <= now - self._period:
                    self._calls.popleft()
                if len(self._calls) < self._max:
                    self._calls.append(now)
                    return
                wait = self._period - (now - self._calls[0])
            time.sleep(min(max(wait, 0.0), 1.0))


def get_exchange() -> ccxt.Exchange:
    """Return this thread's CCXT Lighter instance (created + markets loaded once).

    A fresh instance is built per thread and cached in thread-local storage
    because ccxt exchange objects are not safe to share across threads. CCXT's
    own rate limiting is disabled — the shared :class:`RateLimiter` governs
    throughput instead.

    :returns: A thread-local ``ccxt.lighter`` with markets loaded.
    """
    ex = getattr(_TLS, "exchange", None)
    if ex is None:
        ex = ccxt.lighter({"enableRateLimit": False})
        ex.load_markets()
        _TLS.exchange = ex
    return ex


def resolve_perp_symbols(exchange: ccxt.Exchange) -> list[str]:
    """Return all Lighter perpetual (swap) markets as sorted CCXT symbols.

    Spot markets are skipped; only swaps (CCXT symbols settled in USDC, i.e.
    ending in ``:USDC``) are kept.

    :param exchange: A loaded CCXT Lighter exchange instance.
    :returns: Sorted list of perpetual CCXT unified symbols.
    """
    perps = sorted(
        symbol
        for symbol, market in exchange.markets.items()
        if market.get("swap") and market.get("settle") == "USDC"
    )
    logger.info("Universe: %d Lighter perpetual markets", len(perps))
    return perps


def apply_shard(symbols: list[str], shard: str) -> list[str]:
    """Deterministically select shard ``I`` of ``N`` from a sorted symbol list.

    Uses a stable strided split: ``symbols[i::n]``. Because the input list is
    sorted and the stride is fixed, every runner computes an identical, disjoint
    partition with no coordination. Across all ``N`` shards the union is exactly
    the input list. Strided (rather than contiguous) slicing also spreads the
    alphabetical order across runners so no single shard gets a clustered subset.

    :param symbols: Sorted list of CCXT symbols (the full universe).
    :param shard: String ``"I/N"`` with ``0 <= I < N`` and ``N >= 1``.
    :returns: The subset of ``symbols`` belonging to shard ``I``.
    :raises ValueError: If ``shard`` is malformed or out of range.
    """
    try:
        i_str, n_str = shard.split("/")
        i, n = int(i_str), int(n_str)
    except (ValueError, AttributeError) as exc:
        raise ValueError(f"--shard must be 'I/N', got {shard!r}") from exc
    if n < 1 or not (0 <= i < n):
        raise ValueError(f"--shard out of range: need 0 <= I < N and N >= 1, got {shard!r}")
    subset = symbols[i::n]
    logger.info("Shard %d/%d: %d of %d symbols", i, n, len(subset), len(symbols))
    return subset


def fetch_pair(symbol: str, timeframe: str, limiter: RateLimiter) -> pd.DataFrame:
    """Fetch the full available candle history for one symbol.

    Pages deterministically with explicit ``(since, until)`` windows (the Lighter
    endpoint anchors on ``count_back`` rather than honouring ``since``). Each HTTP
    call passes through the shared :class:`RateLimiter`. Rows are de-duplicated by
    candle open timestamp.

    :param symbol: CCXT unified symbol, e.g. ``BTC/USDC:USDC``.
    :param timeframe: One of the keys in :data:`TIMEFRAME_MS`.
    :param limiter: Shared rate limiter gating every request.
    :returns: A Freqtrade-shaped frame (``date,open,high,low,close,volume``),
        UTC-dated, sorted ascending, de-duplicated. Empty if no data.
    """
    exchange = get_exchange()
    tf_ms = TIMEFRAME_MS[timeframe]
    window_ms = PAGE_LIMIT * tf_ms
    now_ms = exchange.milliseconds()
    rows: dict[int, list[float]] = {}

    cursor = EARLIEST_MS
    while cursor < now_ms:
        window_end = min(cursor + window_ms, now_ms)
        batch: list = []
        for attempt in range(MAX_RETRIES):
            limiter.acquire()
            try:
                batch = exchange.fetch_ohlcv(symbol, timeframe, since=cursor, limit=PAGE_LIMIT, params={"until": window_end})
                break
            except ccxt.BaseError as exc:
                if attempt == MAX_RETRIES - 1:
                    # Exhausted retries — log and skip this window (leaves a gap; rare).
                    logger.warning("%s: window %d-%d failed after %d tries: %s", symbol, cursor, window_end, MAX_RETRIES, exc)
                else:
                    # Transient (429 / 5xx / timeout) — exponential backoff, then retry.
                    time.sleep(RETRY_BACKOFF_S * (2 ** attempt))
        for r in batch:
            rows[int(r[0])] = r
        cursor = window_end + tf_ms

    columns = ["date", "open", "high", "low", "close", "volume"]
    if not rows:
        return pd.DataFrame(columns=columns)

    df = pd.DataFrame(sorted(rows.values()), columns=columns)
    df["date"] = pd.to_datetime(df["date"], unit="ms", utc=True)
    for col in ("open", "high", "low", "close", "volume"):
        df[col] = df[col].astype("float64")
    return df.sort_values("date").reset_index(drop=True)


def feather_name(symbol: str, timeframe: str) -> str:
    """Map a CCXT symbol to its Freqtrade feather filename.

    :param symbol: CCXT unified symbol, e.g. ``BTC/USDC:USDC``.
    :param timeframe: Candle timeframe string.
    :returns: e.g. ``BTC_USDC_USDC-1d-futures.feather``.
    """
    token = symbol.replace("/", "_").replace(":", "_")
    return f"{token}-{timeframe}-futures.feather"


def collect_pair(symbol: str, timeframe: str, out_dir: Path, limiter: RateLimiter) -> PairResult:
    """Worker: fetch one symbol, write its feather, return a summary record.

    :param symbol: CCXT unified symbol.
    :param timeframe: Candle timeframe.
    :param out_dir: Directory the feather is written into.
    :param limiter: Shared rate limiter.
    :returns: A :class:`PairResult` describing the outcome.
    """
    df = fetch_pair(symbol, timeframe, limiter)
    if df.empty:
        return PairResult(pair=symbol, rows=0, start=None, end=None)
    out_dir.mkdir(parents=True, exist_ok=True)
    df.to_feather(out_dir / feather_name(symbol, timeframe))
    return PairResult(pair=symbol, rows=len(df), start=df["date"].iloc[0], end=df["date"].iloc[-1])


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    """Parse command-line arguments.

    :param argv: Optional explicit argument list (defaults to ``sys.argv``).
    :returns: The parsed argument namespace.
    """
    parser = argparse.ArgumentParser(description="Standalone Lighter DEX OHLCV collector.")
    parser.add_argument("--timeframe", default="1d", choices=sorted(TIMEFRAME_MS), help="Candle timeframe (default: 1d).")
    parser.add_argument("--out", type=Path, default=Path("data/futures"), help="Output directory for feathers (default: data/futures).")
    parser.add_argument("--workers", type=int, default=8, help="Concurrent download threads (default: 8).")
    parser.add_argument("--rate", type=int, default=55, help="Max requests per minute; Lighter IP cap is 60 (default: 55).")
    parser.add_argument("--shard", default=None, help="Collect only shard I of N from the sorted perp universe, e.g. '0/8'. Strided split symbols[I::N].")
    parser.add_argument("--symbols", nargs="*", default=None, help="Explicit CCXT symbols; overrides universe and --shard.")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> None:
    """CLI entry point — collect the resolved universe (threaded) and write feathers.

    :param argv: Optional explicit argument list (defaults to ``sys.argv``).
    :returns: ``None``.
    """
    args = parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(message)s", handlers=[RichHandler(show_path=False, rich_tracebacks=True)])

    bootstrap = ccxt.lighter({"enableRateLimit": True})
    bootstrap.load_markets()

    if args.symbols:
        symbols = args.symbols
        logger.info("Explicit symbols: %d", len(symbols))
    else:
        symbols = resolve_perp_symbols(bootstrap)
        if args.shard:
            symbols = apply_shard(symbols, args.shard)

    if not symbols:
        logger.warning("No symbols resolved; nothing to do.")
        return

    out_dir: Path = args.out
    out_dir.mkdir(parents=True, exist_ok=True)
    #: Summary CSV lands in a sibling ``_summary`` dir under --out.
    summary_dir = out_dir / "_summary"
    summary_dir.mkdir(parents=True, exist_ok=True)

    limiter = RateLimiter(args.rate, 60.0)
    results: list[PairResult] = []

    columns = [
        SpinnerColumn(),
        TextColumn("[bold blue]{task.description}"),
        BarColumn(),
        MofNCompleteColumn(),
        TextColumn("•"),
        TimeElapsedColumn(),
        TextColumn("• eta"),
        TimeRemainingColumn(),
        TextColumn("[dim]{task.fields[last]}"),
    ]
    with Progress(*columns) as progress:
        task = progress.add_task(f"Lighter {args.timeframe}", total=len(symbols), last="")
        with ThreadPoolExecutor(max_workers=args.workers) as pool:
            futures = {pool.submit(collect_pair, sym, args.timeframe, out_dir, limiter): sym for sym in symbols}
            for fut in as_completed(futures):
                sym = futures[fut]
                try:
                    rec = fut.result()
                except Exception as exc:  # noqa: BLE001 — record and continue per-pair
                    logger.warning("%s: failed: %s", sym, exc)
                    rec = PairResult(pair=sym, rows=0, start=None, end=None)
                results.append(rec)
                token = sym.split("/")[0]
                last = f"{token} {rec.rows}r" if rec.rows else f"{token} EMPTY"
                progress.update(task, advance=1, last=last)

    summary_df = pd.DataFrame(
        [{"pair": r.pair, "rows": r.rows, "start": r.start, "end": r.end} for r in results]
    ).sort_values("pair").reset_index(drop=True)
    summary_path = summary_dir / f"download_summary_{args.timeframe}.csv"
    summary_df.to_csv(summary_path, index=False)
    logger.info("Wrote summary %s", summary_path)
    logger.info("DONE: %d/%d pairs had data", int((summary_df["rows"] > 0).sum()), len(summary_df))


if __name__ == "__main__":
    main()

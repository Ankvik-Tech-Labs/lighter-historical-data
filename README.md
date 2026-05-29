# lighter-historical-data

Automated collector for **Lighter DEX** ([`mainnet.zklighter.elliot.ai`](https://apidocs.lighter.xyz)) historical market data, published as a rolling GitHub **Release**. A GitHub Actions workflow refreshes the full perp universe daily and (re)uploads the dataset so anyone can pull it from one stable URL — no API calls, no rate limits, no auth.

## What's collected

**Full history** (Lighter genesis → now, no truncation) for **all perp markets**, at **every Lighter timeframe**, plus hourly funding. One release asset per timeframe:

| Asset | Contents |
|---|---|
| `lighter-1m.zip` `lighter-5m.zip` `lighter-15m.zip` `lighter-30m.zip` | intraday OHLCV |
| `lighter-1h.zip` `lighter-4h.zip` `lighter-12h.zip` `lighter-1d.zip` `lighter-1w.zip` | hourly→weekly OHLCV |
| `lighter-funding.zip` | hourly funding rates |
| `manifest.json` | file counts + **exact date coverage** per asset |

- OHLCV format: Freqtrade feather `{TOKEN}_USDC_USDC-{tf}-futures.feather` (`date,open,high,low,close,volume`).
- Funding format: parquet `lighter_funding_rate_lighter_{TOKEN}_1h_{start}_{end}.parquet` — `DatetimeIndex 'time'` + `funding_rate` (signed), `value`, `direction`.
- The **release notes show the data timerange** of every asset; `manifest.json` has exact ISO timestamps.

Lighter perps begin **2025-01-17**; markets list progressively, so newer tokens have shorter histories.

## Consume the data

```bash
# whole dataset, latest snapshot
gh release download latest -R Ankvik-Tech-Labs/lighter-historical-data

# or a single asset by stable URL
curl -L -o lighter-1d.zip \
  https://github.com/Ankvik-Tech-Labs/lighter-historical-data/releases/latest/download/lighter-1d.zip
unzip lighter-1d.zip -d data/futures
```

```python
import pandas as pd
df = pd.read_feather("data/futures/BTC_USDC_USDC-1d-futures.feather")  # date,open,high,low,close,volume
```

## How it works

Lighter rate-limits at **60 requests/min per IP**. GitHub Actions matrix jobs run on **separate runners (separate IPs)**, so the workflow shards the universe across `SHARDS` (default 8) parallel jobs per timeframe — roughly `SHARDS ×` the single-IP ceiling — then a `publish` job merges every shard's artifacts and updates the `latest` release.

```
.github/workflows/collect.yml   schedule (daily 06:00 UTC) + manual dispatch
  ├─ ohlcv   (matrix: tf ∈ {1d,15m} × shard ∈ 0..7)  → artifact per shard
  ├─ funding (matrix: shard ∈ 0..7)                  → artifact per shard
  └─ publish (needs all) → merge → zip + manifest → gh release upload latest --clobber
```

Each collector is self-contained (`ccxt` + `pandas` + `pyarrow` + `rich` [+ `requests`]) and uses a shared sliding-window token-bucket limiter so threads never trip 429s.

## Run locally

```bash
pip install -r requirements.txt

# one timeframe, full universe
python collector/collect_ohlcv.py --timeframe 1d --out data/futures

# a single shard (what each CI runner does)
python collector/collect_ohlcv.py --timeframe 15m --shard 0/8 --out data/futures

# funding
python collector/collect_funding.py --out data/funding

# specific markets
python collector/collect_ohlcv.py --timeframe 1d --symbols BTC/USDC:USDC ETH/USDC:USDC
```

Collector CLIs share `--out`, `--workers` (default 8), `--rate` (default 55, under the 60/min IP cap), `--shard I/N`, `--symbols`. `collect_ohlcv.py` adds `--timeframe`; `collect_funding.py` adds `--resolution` (default `1h`).

## Tuning

- **More throughput / faster full backfill** → raise `SHARDS` in `collect.yml` and extend the matrix `shard:` lists to match.
- **Skip 15m** (the slow one) → drop `"15m"` from the `ohlcv` matrix `tf:`.
- Data is never committed (`.gitignore` excludes `data/`); it lives only in Releases.

## Caveats

Exploratory dataset. Public candles are unauthenticated and best-effort; `value`/`direction`→signed `funding_rate` follows the convention `direction=="long" → positive` (longs pay). Not affiliated with Lighter.

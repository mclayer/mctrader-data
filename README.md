# mctrader-data

OHLCV collector + Parquet/DuckDB storage for the mctrader platform — ADR-009 v1 schema reference impl.

## Status

`v0.1.0` — first commit, MCT-15 Phase 2.

## Public API

```python
from mctrader_data import scan_candles, OhlcvSchema, BackfillRunner
```

## CLI

```bash
mctrader-data backfill --exchange bithumb --symbol KRW-BTC --tf 1h --days 7
mctrader-data backfill --exchange bithumb --symbol KRW-BTC --tf 1h --start 2026-04-25T00:00:00Z --end 2026-05-02T00:00:00Z --policy halt
mctrader-data backfill --exchange bithumb --symbol KRW-BTC --tf 1h --days 7 --dry-run
```

## Storage layout (ADR-009 D2)

```
{root}/market/ohlcv/schema_version=ohlcv.v1/exchange={ex}/symbol={sym}/timeframe={tf}/year={Y}/month={M}/date={D}/*.parquet
```

Root resolution priority: `--root` > `MCTRADER_DATA_ROOT` > repo-local `data/parquet/`.

## Related

- [mctrader-hub](https://github.com/mclayer/hub) — governance / ADR-009
- [mctrader-market](https://github.com/mclayer/mctrader-market) — Candle Protocol contract

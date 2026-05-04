# mctrader collector — Linux systemd deployment

24/7 forward-only WebSocket collector for ticks + orderbook (MCT-58).

## Prerequisites

- Linux host (Debian/Ubuntu/Fedora etc.) with `systemd`
- Python 3.11+ via `uv` (recommended) or `pip`
- Outbound HTTPS + WebSocket access to `pubwss.bithumb.com` and `api.bithumb.com`

## Install

```bash
# 1. Create dedicated user (no shell, no home)
sudo useradd --system --no-create-home --shell /usr/sbin/nologin mctrader

# 2. Install runtime as a venv under /opt/mctrader/
sudo mkdir -p /opt/mctrader
sudo chown -R mctrader:mctrader /opt/mctrader
sudo -u mctrader bash -c '
  cd /opt/mctrader
  uv venv .venv --python 3.12
  uv pip install --python .venv/bin/python \
    "mctrader-data @ git+https://github.com/mclayer/mctrader-data.git@main"
'

# 3. Verify CLI runs
/opt/mctrader/.venv/bin/mctrader-data --help

# 4. Install systemd unit
sudo cp deploy/mctrader-collector.service /etc/systemd/system/
sudo systemctl daemon-reload

# 5. Enable + start
sudo systemctl enable --now mctrader-collector

# 6. Watch logs
sudo journalctl -u mctrader-collector -f
```

## Configuration

The unit reads `Environment=` lines:

| Var | Default | Purpose |
|---|---|---|
| `MCTRADER_DATA_ROOT` | `/var/lib/mctrader/data` | Parquet output root (Hive partitions land below) |

Edit the `ExecStart` line to change CLI args:

```ini
# Run a specific symbol set instead of top-10:
ExecStart=/opt/mctrader/.venv/bin/mctrader-data collect \
  --symbols KRW-BTC,KRW-ETH,KRW-XRP \
  --include transactions,orderbook \
  --log-level INFO

# Tickers only (smaller storage):
ExecStart=... --top-n 10 --include transactions ...
```

After edits: `sudo systemctl daemon-reload && sudo systemctl restart mctrader-collector`.

## Storage layout (ADR-009 amendment)

```
/var/lib/mctrader/data/
└── market/
    ├── ohlcv/                                     # MCT-15 (existing)
    │   └── schema_version=ohlcv.v1/.../...parquet
    ├── ticks/                                     # MCT-58 (new)
    │   └── schema_version=tick.v1/exchange=bithumb/symbol=KRW-BTC/date=2026-05-04/part-{snapshot_id}.parquet
    └── orderbook/                                 # MCT-58 (new)
        └── schema_version=orderbook.v1/exchange=bithumb/symbol=KRW-BTC/date=2026-05-04/part-{snapshot_id}.parquet
```

Each partition has a `_lineage_{snapshot_id}.json` sidecar identifying provenance.

## Storage volume estimate (top-10 KRW, both channels)

| Component | Per day | Per month | Per year |
|---|---|---|---|
| Ticks (10 symbols) | ~2 MB | ~60 MB | ~720 MB |
| Orderbook L2 (10 symbols) | ~100 MB | ~3 GB | ~36 GB |
| **Total** | **~100 MB** | **~3 GB** | **~36 GB** |

Snappy compression already applied. For longer retention, add a parallel cold-archive process moving partitions older than N days to S3/MinIO/B2.

## Operations

| Action | Command |
|---|---|
| Status | `systemctl status mctrader-collector` |
| Tail logs | `journalctl -u mctrader-collector -f` |
| Stop (graceful) | `sudo systemctl stop mctrader-collector` (SIGTERM + 30s drain) |
| Restart | `sudo systemctl restart mctrader-collector` |
| Disable boot autostart | `sudo systemctl disable mctrader-collector` |

## Health check

The collector logs a startup line per symbol: `[collector] symbol=KRW-BTC channels=['transaction', 'orderbook'] root=/var/lib/mctrader/data`.

If logs stop within 5 minutes of startup, suspect upstream WebSocket reachability.
The Bithumb stream auto-reconnects with exponential backoff — transient drops are
non-fatal. Hard failures (DNS / TLS) cause systemd to restart the service after
10s (see `RestartSec=` in unit).

## Disaster recovery

- All data is forward-only. **There is no backfill for ticks/orderbook** — Bithumb
  public API does not expose historical tick data. A collector outage = permanent
  gap in the data window for that symbol.
- For multi-host redundancy, run the collector on 2+ hosts writing to separate
  roots; reconcile/merge offline. Out of scope for v1.

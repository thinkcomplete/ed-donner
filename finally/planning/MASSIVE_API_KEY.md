# Massive API (formerly Polygon.io) — Reference Guide

## Overview

Massive (rebranded from Polygon.io) provides REST and WebSocket APIs for US stock market data. The project uses the `massive` Python package (`massive>=1.0.0`) to poll real-time and end-of-day prices when `MASSIVE_API_KEY` is set in the environment.

**Key facts:**
- Base URL: `https://api.massive.com` (same infrastructure as the former `api.polygon.io`)
- Python package: `massive` (pip/uv installable)
- Authentication: API key passed to `RESTClient(api_key=...)`
- Free tier: 5 requests/minute, 15-minute delayed data
- Paid tiers: higher rate limits, real-time data (Advanced/Business plans)

## Getting an API Key

1. Sign up at [massive.com](https://massive.com)
2. Navigate to Dashboard → API Keys
3. Copy your key and add it to `.env`:
   ```
   MASSIVE_API_KEY=your_key_here
   ```

## Python Client Setup

```python
from massive import RESTClient

client = RESTClient(api_key="your_key_here")
# or, reading from env:
import os
client = RESTClient(api_key=os.environ["MASSIVE_API_KEY"])
```

The `RESTClient` is **synchronous**. In async code (FastAPI), wrap calls with `asyncio.to_thread()` to avoid blocking the event loop.

```python
import asyncio
from massive import RESTClient

client = RESTClient(api_key="...")

# Safe to call from async context:
snapshots = await asyncio.to_thread(
    client.get_snapshot_all, market_type=..., tickers=[...]
)
```

---

## Endpoint 1: Full Market Snapshot (primary usage)

**GET** `/v2/snapshot/locale/us/markets/stocks/tickers`

Returns the latest trade, quote, and daily bar for a list of tickers in a single API call. This is the main endpoint used by `MassiveDataSource`.

### Python SDK

```python
from massive import RESTClient
from massive.rest.models import SnapshotMarketType

client = RESTClient(api_key="...")

snapshots = client.get_snapshot_all(
    market_type=SnapshotMarketType.STOCKS,
    tickers=["AAPL", "GOOGL", "MSFT", "TSLA"],
)

for snap in snapshots:
    ticker   = snap.ticker                  # "AAPL"
    price    = snap.last_trade.price        # 190.42 (float)
    ts_ms    = snap.last_trade.timestamp    # Unix milliseconds (int)
    ts_sec   = ts_ms / 1000.0              # Convert to Unix seconds
    day_open = snap.day.open               # Today's open price
    day_high = snap.day.high
    day_low  = snap.day.low
    day_close = snap.day.close             # Today's close (or latest)
    day_vol  = snap.day.volume
    change   = snap.todays_change          # Absolute change from prev close
    change_pct = snap.todays_change_perc   # % change from prev close
    print(f"{ticker}: ${price:.2f}  ({change_pct:+.2f}%)")
```

### Response field reference

| Field | Type | Description |
|---|---|---|
| `snap.ticker` | str | Exchange symbol (e.g., `"AAPL"`) |
| `snap.last_trade.price` | float | Most recent trade price |
| `snap.last_trade.timestamp` | int | Unix **milliseconds** — divide by 1000 for seconds |
| `snap.day.open` | float | Today's open |
| `snap.day.high` | float | Today's high |
| `snap.day.low` | float | Today's low |
| `snap.day.close` | float | Today's close (last if intraday) |
| `snap.day.volume` | float | Today's volume |
| `snap.prev_day.close` | float | Previous trading day's close |
| `snap.todays_change` | float | Absolute change from previous close |
| `snap.todays_change_perc` | float | Percentage change from previous close |
| `snap.updated` | int | Last update Unix milliseconds |

### Rate limit considerations

```python
# Free tier: 5 requests/minute → poll every 15 seconds
# Including all watched tickers in ONE call keeps usage at 1 req/poll

snapshots = client.get_snapshot_all(
    market_type=SnapshotMarketType.STOCKS,
    tickers=["AAPL", "GOOGL", "MSFT", "AMZN", "TSLA", "NVDA", "META", "JPM", "V", "NFLX"],
    # All 10 tickers = 1 API call = within free tier at 15s interval
)
```

### Error handling

Common errors and their meanings:

| HTTP status | Cause | Action |
|---|---|---|
| `401 Unauthorized` | Bad or missing API key | Check `MASSIVE_API_KEY` value |
| `403 Forbidden` | Endpoint not in your plan | Upgrade tier or use a different endpoint |
| `429 Too Many Requests` | Rate limit exceeded | Increase poll interval |
| Network errors | Transient connectivity | Log and retry on next scheduled poll |

```python
try:
    snapshots = client.get_snapshot_all(
        market_type=SnapshotMarketType.STOCKS,
        tickers=tickers,
    )
except Exception as e:
    logger.error("Massive poll failed: %s", e)
    # Don't re-raise — the next poll will retry automatically
```

---

## Endpoint 2: Previous Day Bar (OHLC)

**GET** `/v2/aggs/ticker/{ticker}/prev`

Returns the previous trading day's open, high, low, close, and volume for a single ticker. Useful for computing daily change % and for seeding charts with yesterday's close.

```python
# Raw HTTP (for reference)
import requests

resp = requests.get(
    "https://api.massive.com/v2/aggs/ticker/AAPL/prev",
    params={"adjusted": True},
    headers={"Authorization": f"Bearer {api_key}"},
)
data = resp.json()

# data["results"][0] contains:
result = data["results"][0]
print(result["o"])  # open
print(result["h"])  # high
print(result["l"])  # low
print(result["c"])  # close
print(result["v"])  # volume
print(result["vw"]) # volume-weighted average price
print(result["t"])  # Unix millisecond timestamp
```

**Available on all plans** (Basic and above). Basic tier returns end-of-day data; Starter/Developer return 15-minute delayed; Advanced/Business return real-time.

---

## Endpoint 3: Custom Bars / Historical OHLC

**GET** `/v2/aggs/ticker/{ticker}/range/{multiplier}/{timespan}/{from}/{to}`

Returns OHLC bars over a custom date range. Use this to backfill charts or calculate technical indicators.

```python
import requests
from datetime import date, timedelta

api_key = "..."
ticker = "AAPL"
today = date.today()
one_year_ago = today - timedelta(days=365)

resp = requests.get(
    f"https://api.massive.com/v2/aggs/ticker/{ticker}/range/1/day"
    f"/{one_year_ago}/{today}",
    params={"adjusted": True, "sort": "asc", "limit": 5000},
    headers={"Authorization": f"Bearer {api_key}"},
)
data = resp.json()

for bar in data.get("results", []):
    ts_seconds = bar["t"] / 1000      # Convert ms → seconds
    print(f"t={ts_seconds}, o={bar['o']}, h={bar['h']}, l={bar['l']}, c={bar['c']}, v={bar['v']}")
```

### Timespan options

| `timespan` | Example use |
|---|---|
| `minute` | Intraday charts |
| `hour` | Multi-day intraday views |
| `day` | Daily candlestick charts |
| `week` | Weekly summary bars |
| `month` | Long-term trend charts |

### Parameters

| Parameter | Type | Default | Description |
|---|---|---|---|
| `multiplier` | int | — | Window size (e.g., `5` = 5-minute bars) |
| `timespan` | str | — | Unit of `multiplier` |
| `from` | str | — | Start date `YYYY-MM-DD` or Unix ms timestamp |
| `to` | str | — | End date `YYYY-MM-DD` or Unix ms timestamp |
| `adjusted` | bool | `true` | Split-adjust prices |
| `sort` | str | — | `asc` or `desc` |
| `limit` | int | 5000 | Max results (max 50000) |

---

## WebSocket Streams (not used by this project)

Massive also offers WebSocket feeds for tick-level trade data, per-second and per-minute aggregates, and quotes. These are not used in FinAlly because:

- REST polling is simpler and sufficient for a 500ms update cycle
- WebSocket connections add reconnection/heartbeat complexity
- The free tier does not include WebSocket access on most plans

If you upgrade to a paid plan and want sub-second updates, switching `MassiveDataSource` to a WebSocket client is a well-contained change (same `MarketDataSource` interface, different transport).

---

## Plan Comparison (relevant tiers)

| Feature | Starter/Developer | Advanced | Business |
|---|---|---|---|
| Data delay | 15 minutes | Real-time | Real-time |
| Rate limit | ~5 req/min | Higher | Highest |
| Snapshot endpoint | Yes | Yes | Yes |
| WebSocket | No | Yes | Yes |
| Recommended poll interval | 15s | 2-5s | 1-2s |

For development and demo purposes, the Starter tier (15-minute delayed data) is sufficient. Set `poll_interval` in `MassiveDataSource` to match your plan:

```python
# Free/Starter: 15 seconds (default)
source = MassiveDataSource(api_key=key, price_cache=cache, poll_interval=15.0)

# Advanced: 5 seconds
source = MassiveDataSource(api_key=key, price_cache=cache, poll_interval=5.0)

# Business: 2 seconds
source = MassiveDataSource(api_key=key, price_cache=cache, poll_interval=2.0)
```

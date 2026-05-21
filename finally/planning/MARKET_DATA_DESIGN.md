# Market Data Backend — Design Reference

Complete implementation reference for the `backend/app/market/` subsystem. Covers the unified interface, GBM simulator, Massive (Polygon.io) REST client, price cache, SSE streaming, and integration patterns. Includes working code snippets drawn directly from the implementation.

---

## Table of Contents

1. [Architecture Overview](#1-architecture-overview)
2. [PriceUpdate Model](#2-priceupdate-model)
3. [PriceCache](#3-pricecache)
4. [MarketDataSource Interface](#4-marketdatasource-interface)
5. [GBM Simulator](#5-gbm-simulator)
6. [Massive API Client](#6-massive-api-client)
7. [Factory — Env-Driven Selection](#7-factory--env-driven-selection)
8. [SSE Streaming](#8-sse-streaming)
9. [FastAPI Integration](#9-fastapi-integration)
10. [Watchlist Management](#10-watchlist-management)
11. [Portfolio Valuation](#11-portfolio-valuation)
12. [Adding a New Data Source](#12-adding-a-new-data-source)
13. [Testing Patterns](#13-testing-patterns)
14. [Configuration Reference](#14-configuration-reference)

---

## 1. Architecture Overview

```
┌─────────────────────────────────────────────────────────────────┐
│  Data Source (one active at a time)                             │
│                                                                 │
│  SimulatorDataSource          MassiveDataSource                 │
│  ─────────────────────        ──────────────────                │
│  GBMSimulator.step()          asyncio.to_thread(                │
│  every 500ms                    client.get_snapshot_all(...)    │
│  ──────────┐                  ) every 15s                       │
│            │                  ──────────┐                       │
│            └──────────────────┘         │                       │
│                               ▼                                 │
│                      PriceCache.update()                        │
│                      (thread-safe write)                        │
└────────────────────────────────┬────────────────────────────────┘
                                 │
              ┌──────────────────┼──────────────────┐
              ▼                  ▼                  ▼
     SSE /stream/prices   Portfolio routes    Trade execution
     (version-gated push) (cache.get_all())  (cache.get_price())
```

### Module map

```
backend/app/market/
├── __init__.py          Public re-exports
├── models.py            PriceUpdate — immutable price snapshot
├── interface.py         MarketDataSource — abstract base class
├── cache.py             PriceCache — thread-safe in-memory store
├── seed_prices.py       Starting prices and GBM params
├── simulator.py         GBMSimulator + SimulatorDataSource
├── massive_client.py    MassiveDataSource (Polygon.io REST polling)
├── factory.py           create_market_data_source() — env-driven selection
└── stream.py            FastAPI SSE endpoint factory
```

### Design principles

- **Strategy pattern** — `SimulatorDataSource` and `MassiveDataSource` both implement `MarketDataSource`. Downstream code never knows which is active.
- **Single point of truth** — `PriceCache` is the only place prices live. Producers write; consumers read.
- **Async-safe** — The Massive `RESTClient` is synchronous; it runs in `asyncio.to_thread()` to avoid blocking the event loop.
- **No globals** — Everything is wired through dependency injection. Testable without monkey-patching.

---

## 2. PriceUpdate Model

`PriceUpdate` is the canonical representation of one price tick. It is immutable (`frozen=True`) and uses `__slots__` for minimal memory footprint.

```python
# backend/app/market/models.py
from __future__ import annotations

import time
from dataclasses import dataclass, field


@dataclass(frozen=True, slots=True)
class PriceUpdate:
    ticker: str
    price: float
    previous_price: float
    timestamp: float = field(default_factory=time.time)  # Unix seconds

    @property
    def change(self) -> float:
        return round(self.price - self.previous_price, 4)

    @property
    def change_percent(self) -> float:
        if self.previous_price == 0:
            return 0.0
        return round((self.price - self.previous_price) / self.previous_price * 100, 4)

    @property
    def direction(self) -> str:
        if self.price > self.previous_price:
            return "up"
        elif self.price < self.previous_price:
            return "down"
        return "flat"

    def to_dict(self) -> dict:
        return {
            "ticker": self.ticker,
            "price": self.price,
            "previous_price": self.previous_price,
            "timestamp": self.timestamp,
            "change": self.change,
            "change_percent": self.change_percent,
            "direction": self.direction,
        }
```

### Usage examples

```python
from app.market import PriceUpdate

# Create a price tick
update = PriceUpdate(
    ticker="AAPL",
    price=190.42,
    previous_price=190.10,
    timestamp=1716123456.789,
)

# Read computed properties
print(update.change)          # 0.32
print(update.change_percent)  # 0.1683
print(update.direction)       # "up"

# Serialize for JSON / SSE
payload = update.to_dict()
# {
#   "ticker": "AAPL",
#   "price": 190.42,
#   "previous_price": 190.10,
#   "timestamp": 1716123456.789,
#   "change": 0.32,
#   "change_percent": 0.1683,
#   "direction": "up"
# }
```

### Immutability

`PriceUpdate` cannot be modified after creation. To record a new price for an existing ticker, create a new instance (the `PriceCache.update()` method handles this automatically).

```python
# This raises FrozenInstanceError
update.price = 191.00  # TypeError!

# Do this instead:
cache.update("AAPL", price=191.00)  # Creates a new PriceUpdate internally
```

---

## 3. PriceCache

Thread-safe in-memory store. One writer (the active data source) writes continuously; many readers (SSE, portfolio, trades) read concurrently. Uses `threading.Lock` because the Massive poller runs in `asyncio.to_thread()`, which uses OS threads.

```python
# backend/app/market/cache.py
from __future__ import annotations

import time
from threading import Lock
from .models import PriceUpdate


class PriceCache:
    def __init__(self) -> None:
        self._prices: dict[str, PriceUpdate] = {}
        self._lock = Lock()
        self._version: int = 0  # Bumped on every update(); used by SSE

    def update(self, ticker: str, price: float, timestamp: float | None = None) -> PriceUpdate:
        with self._lock:
            ts = timestamp or time.time()
            prev = self._prices.get(ticker)
            previous_price = prev.price if prev else price  # first update: flat

            update = PriceUpdate(
                ticker=ticker,
                price=round(price, 2),
                previous_price=round(previous_price, 2),
                timestamp=ts,
            )
            self._prices[ticker] = update
            self._version += 1
            return update

    def get(self, ticker: str) -> PriceUpdate | None:
        with self._lock:
            return self._prices.get(ticker)

    def get_all(self) -> dict[str, PriceUpdate]:
        with self._lock:
            return dict(self._prices)  # shallow copy

    def get_price(self, ticker: str) -> float | None:
        update = self.get(ticker)
        return update.price if update else None

    def remove(self, ticker: str) -> None:
        with self._lock:
            self._prices.pop(ticker, None)

    @property
    def version(self) -> int:
        return self._version
```

### Reading prices in routes

```python
from app.market import PriceCache

def get_price_cache() -> PriceCache:
    # In production this is stored on app.state; see §9 FastAPI Integration
    return app.state.price_cache

# In a portfolio route:
@router.get("/api/portfolio")
def get_portfolio(cache: PriceCache = Depends(get_price_cache)):
    all_prices = cache.get_all()

    positions = []
    for ticker, qty, avg_cost in db_positions:
        update = all_prices.get(ticker)
        if update:
            current_price = update.price
            unrealized_pnl = (current_price - avg_cost) * qty
            positions.append({
                "ticker": ticker,
                "quantity": qty,
                "avg_cost": avg_cost,
                "current_price": current_price,
                "unrealized_pnl": round(unrealized_pnl, 2),
                "change_percent": update.change_percent,
            })

    return {"positions": positions}
```

### Version counter for SSE change detection

The `version` property is a monotonically increasing integer that increments every time `update()` is called. The SSE generator stores its last-seen version and only pushes an event when the version changes, preventing redundant sends when prices haven't changed.

```python
# Simple demonstration of version-gated sending
last_version = -1

while True:
    current_version = cache.version
    if current_version != last_version:
        last_version = current_version
        data = cache.get_all()
        # push SSE event...
    await asyncio.sleep(0.5)
```

### Thread safety guarantee

All cache reads and writes are protected by a `threading.Lock`. The table below shows which threads access the cache and how:

| Thread | Access pattern |
|---|---|
| `asyncio` event loop (simulator) | `update()` called from `asyncio.create_task` (same thread as event loop) |
| OS thread pool (Massive poller) | `update()` called from `asyncio.to_thread()` — different OS thread |
| `asyncio` event loop (SSE generator) | `get_all()` called from async generator |
| `asyncio` event loop (route handlers) | `get()`, `get_price()`, `get_all()` |

The lock ensures all concurrent reads and writes are serialised correctly.

---

## 4. MarketDataSource Interface

The abstract base class every data source must implement. Defines the full lifecycle contract.

```python
# backend/app/market/interface.py
from abc import ABC, abstractmethod


class MarketDataSource(ABC):
    @abstractmethod
    async def start(self, tickers: list[str]) -> None:
        """Begin producing price updates. Call once at startup."""

    @abstractmethod
    async def stop(self) -> None:
        """Stop the background task. Safe to call multiple times."""

    @abstractmethod
    async def add_ticker(self, ticker: str) -> None:
        """Add a ticker to the active set. Takes effect on next cycle."""

    @abstractmethod
    async def remove_ticker(self, ticker: str) -> None:
        """Remove a ticker and clear it from the cache."""

    @abstractmethod
    def get_tickers(self) -> list[str]:
        """Return the list of currently tracked tickers."""
```

### Lifecycle contract

```
create_market_data_source(cache)
        │
        ▼
await source.start(["AAPL", "GOOGL", ...])   ← background task starts
        │
        ├─ await source.add_ticker("PYPL")   ← mid-session additions
        ├─ await source.remove_ticker("NFLX")
        │
        ▼
await source.stop()                           ← app shutdown
```

### Behavioral contract per method

| Method | SimulatorDataSource | MassiveDataSource |
|---|---|---|
| `start(tickers)` | Creates GBMSimulator, seeds cache from SEED_PRICES, launches asyncio task | Creates RESTClient, does first poll immediately, launches poll loop |
| `stop()` | Cancels asyncio task, awaits CancelledError | Same |
| `add_ticker(t)` | Adds to GBM (rebuilds Cholesky), seeds cache immediately | Appends to ticker list; appears on next poll (≤15s delay) |
| `remove_ticker(t)` | Removes from GBM, clears from cache | Removes from list, clears from cache |
| `get_tickers()` | Returns `GBMSimulator._tickers` copy | Returns `_tickers` list copy |

---

## 5. GBM Simulator

### Math: Geometric Brownian Motion

Each simulator tick applies the GBM formula:

```
S(t + dt) = S(t) × exp((μ - σ²/2) × dt + σ × √dt × Z)
```

| Symbol | Meaning |
|---|---|
| `S(t)` | Current price |
| `μ` (mu) | Annualized drift (expected return). Positive = trending up on average. |
| `σ` (sigma) | Annualized volatility. Higher = larger swings. |
| `dt` | Time step as a fraction of a trading year |
| `Z` | Standard normal random variable (the "shock") |

**Why GBM?**
- Prices can never go negative (exponential form)
- Fat-tailed log-normal distribution (realistic)
- Per-stock parameterisation (TSLA more volatile than V)
- Industry standard — basis of Black-Scholes pricing

### Time step calibration

```python
# 252 trading days × 6.5 hours × 3600 seconds = 5,896,800 seconds/year
# One 500ms tick = 0.5 / 5,896,800 ≈ 8.48e-8 years
TRADING_SECONDS_PER_YEAR = 252 * 6.5 * 3600  # 5,896,800
DEFAULT_DT = 0.5 / TRADING_SECONDS_PER_YEAR   # ~8.48e-8
```

With `dt = 8.48e-8` and `sigma = 0.25` (25% annualized vol), a $190 stock moves:
```
σ × √dt × price = 0.25 × √(8.48e-8) × 190 ≈ $0.014 per tick
```
About 1.4 cents per tick — smooth and realistic.

### GBMSimulator implementation

```python
# backend/app/market/simulator.py
import math
import random
import numpy as np
from .seed_prices import SEED_PRICES, TICKER_PARAMS, DEFAULT_PARAMS
from .seed_prices import CORRELATION_GROUPS, INTRA_TECH_CORR, INTRA_FINANCE_CORR
from .seed_prices import CROSS_GROUP_CORR, TSLA_CORR


class GBMSimulator:
    TRADING_SECONDS_PER_YEAR = 252 * 6.5 * 3600
    DEFAULT_DT = 0.5 / TRADING_SECONDS_PER_YEAR  # ~8.48e-8

    def __init__(
        self,
        tickers: list[str],
        dt: float = DEFAULT_DT,
        event_probability: float = 0.001,
    ) -> None:
        self._dt = dt
        self._event_prob = event_probability
        self._tickers: list[str] = []
        self._prices: dict[str, float] = {}
        self._params: dict[str, dict[str, float]] = {}
        self._cholesky: np.ndarray | None = None

        for ticker in tickers:
            self._add_ticker_internal(ticker)
        self._rebuild_cholesky()

    def step(self) -> dict[str, float]:
        n = len(self._tickers)
        if n == 0:
            return {}

        # 1. Draw n independent standard normals
        z_independent = np.random.standard_normal(n)

        # 2. Introduce sector correlations via Cholesky
        z_correlated = self._cholesky @ z_independent if self._cholesky is not None else z_independent

        result: dict[str, float] = {}
        for i, ticker in enumerate(self._tickers):
            mu = self._params[ticker]["mu"]
            sigma = self._params[ticker]["sigma"]

            # GBM update
            drift = (mu - 0.5 * sigma ** 2) * self._dt
            diffusion = sigma * math.sqrt(self._dt) * z_correlated[i]
            self._prices[ticker] *= math.exp(drift + diffusion)

            # Random shock event: ~0.1% chance per tick per ticker
            if random.random() < self._event_prob:
                shock = random.uniform(0.02, 0.05)
                sign = random.choice([-1, 1])
                self._prices[ticker] *= 1 + shock * sign

            result[ticker] = round(self._prices[ticker], 2)

        return result

    def add_ticker(self, ticker: str) -> None:
        if ticker in self._prices:
            return
        self._add_ticker_internal(ticker)
        self._rebuild_cholesky()

    def remove_ticker(self, ticker: str) -> None:
        if ticker not in self._prices:
            return
        self._tickers.remove(ticker)
        del self._prices[ticker]
        del self._params[ticker]
        self._rebuild_cholesky()

    def get_price(self, ticker: str) -> float | None:
        return self._prices.get(ticker)

    def get_tickers(self) -> list[str]:
        return list(self._tickers)

    def _add_ticker_internal(self, ticker: str) -> None:
        self._tickers.append(ticker)
        # Known tickers use SEED_PRICES; unknown tickers get a random $50-$300 start
        self._prices[ticker] = SEED_PRICES.get(ticker, random.uniform(50.0, 300.0))
        self._params[ticker] = TICKER_PARAMS.get(ticker, dict(DEFAULT_PARAMS))

    def _rebuild_cholesky(self) -> None:
        n = len(self._tickers)
        if n <= 1:
            self._cholesky = None
            return

        corr = np.eye(n)
        for i in range(n):
            for j in range(i + 1, n):
                rho = self._pairwise_correlation(self._tickers[i], self._tickers[j])
                corr[i, j] = corr[j, i] = rho
        self._cholesky = np.linalg.cholesky(corr)

    @staticmethod
    def _pairwise_correlation(t1: str, t2: str) -> float:
        tech = CORRELATION_GROUPS["tech"]
        finance = CORRELATION_GROUPS["finance"]
        if t1 == "TSLA" or t2 == "TSLA":
            return TSLA_CORR
        if t1 in tech and t2 in tech:
            return INTRA_TECH_CORR
        if t1 in finance and t2 in finance:
            return INTRA_FINANCE_CORR
        return CROSS_GROUP_CORR
```

### Correlation via Cholesky decomposition

Tech stocks and finance stocks tend to move together. The simulator reproduces this via:

1. Build an `n × n` correlation matrix from pairwise sector assignments
2. Decompose it with Cholesky: `C = L × Lᵀ`
3. Multiply independent normals by `L`: `z_correlated = L @ z_independent`

Result: `z_correlated[i]` and `z_correlated[j]` have the correlation specified in the matrix.

```python
# Correlation structure
INTRA_TECH_CORR    = 0.6   # AAPL, GOOGL, MSFT, AMZN, META, NVDA, NFLX
INTRA_FINANCE_CORR = 0.5   # JPM, V
CROSS_GROUP_CORR   = 0.3   # Cross-sector or unknown tickers
TSLA_CORR          = 0.3   # TSLA listed in tech but trades independently

# Example correlation matrix for [AAPL, MSFT, JPM]:
# [[1.0, 0.6, 0.3],
#  [0.6, 1.0, 0.3],
#  [0.3, 0.3, 1.0]]
```

The matrix is rebuilt whenever tickers are added/removed. It's O(n²) but `n < 50` in practice (~0.1ms for 10 tickers).

### Seed prices and per-ticker parameters

```python
# backend/app/market/seed_prices.py

SEED_PRICES = {
    "AAPL":  190.00,
    "GOOGL": 175.00,
    "MSFT":  420.00,
    "AMZN":  185.00,
    "TSLA":  250.00,
    "NVDA":  800.00,
    "META":  500.00,
    "JPM":   195.00,
    "V":     280.00,
    "NFLX":  600.00,
}

TICKER_PARAMS = {
    "AAPL":  {"sigma": 0.22, "mu": 0.05},  # Stable large-cap
    "GOOGL": {"sigma": 0.25, "mu": 0.05},
    "MSFT":  {"sigma": 0.20, "mu": 0.05},  # Lowest tech vol
    "AMZN":  {"sigma": 0.28, "mu": 0.05},
    "TSLA":  {"sigma": 0.50, "mu": 0.03},  # High vol, lower drift
    "NVDA":  {"sigma": 0.40, "mu": 0.08},  # High vol, strong upward drift
    "META":  {"sigma": 0.30, "mu": 0.05},
    "JPM":   {"sigma": 0.18, "mu": 0.04},  # Low vol (bank)
    "V":     {"sigma": 0.17, "mu": 0.04},  # Lowest vol (payments)
    "NFLX":  {"sigma": 0.35, "mu": 0.05},
}

DEFAULT_PARAMS = {"sigma": 0.25, "mu": 0.05}  # Unknown tickers
```

### Random shock events

Beyond normal GBM drift, the simulator injects sudden 2-5% moves at random:

```python
event_probability = 0.001  # 0.1% per tick per ticker

# Expected rate:
# 10 tickers × 2 ticks/sec × 0.001 = 0.02 events/sec
# → one event every ~50 seconds on average
```

These create the "breaking news" price spikes that make the demo compelling.

### SimulatorDataSource — async wrapper

`GBMSimulator` is pure synchronous math. `SimulatorDataSource` wraps it with the `MarketDataSource` lifecycle:

```python
# backend/app/market/simulator.py

class SimulatorDataSource(MarketDataSource):
    def __init__(
        self,
        price_cache: PriceCache,
        update_interval: float = 0.5,    # 500ms ticks
        event_probability: float = 0.001,
    ) -> None:
        self._cache = price_cache
        self._interval = update_interval
        self._event_prob = event_probability
        self._sim: GBMSimulator | None = None
        self._task: asyncio.Task | None = None

    async def start(self, tickers: list[str]) -> None:
        self._sim = GBMSimulator(tickers=tickers, event_probability=self._event_prob)

        # Seed cache with initial prices — no null prices before first tick
        for ticker in tickers:
            price = self._sim.get_price(ticker)
            if price is not None:
                self._cache.update(ticker=ticker, price=price)

        self._task = asyncio.create_task(self._run_loop(), name="simulator-loop")

    async def _run_loop(self) -> None:
        while True:
            try:
                if self._sim:
                    prices = self._sim.step()
                    for ticker, price in prices.items():
                        self._cache.update(ticker=ticker, price=price)
            except Exception:
                logger.exception("Simulator step failed")
            await asyncio.sleep(self._interval)

    async def add_ticker(self, ticker: str) -> None:
        if self._sim:
            self._sim.add_ticker(ticker)
            price = self._sim.get_price(ticker)
            if price is not None:
                # Seed immediately to avoid a null-price flash in the SSE stream
                self._cache.update(ticker=ticker, price=price)

    async def remove_ticker(self, ticker: str) -> None:
        if self._sim:
            self._sim.remove_ticker(ticker)
        self._cache.remove(ticker)

    async def stop(self) -> None:
        if self._task and not self._task.done():
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
        self._task = None

    def get_tickers(self) -> list[str]:
        return self._sim.get_tickers() if self._sim else []
```

### Why immediate seeding?

When `add_ticker()` is called, prices are written to the cache before the next 500ms tick fires. Without this, a freshly added ticker would appear in the SSE stream with a `null` price for up to half a second — a visible flash in the UI.

---

## 6. Massive API Client

`MassiveDataSource` polls the Polygon.io (rebranded "Massive") REST API and writes results to the `PriceCache`. The SDK's `RESTClient` is synchronous, so all network calls run via `asyncio.to_thread()`.

### Setup

```python
from massive import RESTClient
from massive.rest.models import SnapshotMarketType

# Synchronous client — always wrap in asyncio.to_thread() in async code
client = RESTClient(api_key=os.environ["MASSIVE_API_KEY"])
```

### Primary endpoint: get_snapshot_all

Fetches the latest trade, quote, and daily bar for a list of tickers in **one API call**:

```python
snapshots = client.get_snapshot_all(
    market_type=SnapshotMarketType.STOCKS,
    tickers=["AAPL", "GOOGL", "MSFT", "TSLA"],
)

for snap in snapshots:
    ticker      = snap.ticker                    # "AAPL"
    price       = snap.last_trade.price          # 190.42
    ts_ms       = snap.last_trade.timestamp      # Unix milliseconds
    ts_sec      = ts_ms / 1000.0                # Convert to Unix seconds

    # Daily bars
    day_open    = snap.day.open
    day_high    = snap.day.high
    day_low     = snap.day.low
    day_close   = snap.day.close
    day_volume  = snap.day.volume

    # True daily change (relative to previous trading day's close)
    change      = snap.todays_change             # absolute
    change_pct  = snap.todays_change_perc        # percentage

    print(f"{ticker}: ${price:.2f} ({change_pct:+.2f}%)")
```

### MassiveDataSource implementation

```python
# backend/app/market/massive_client.py
import asyncio
import logging
from massive import RESTClient
from massive.rest.models import SnapshotMarketType
from .cache import PriceCache
from .interface import MarketDataSource

logger = logging.getLogger(__name__)


class MassiveDataSource(MarketDataSource):
    def __init__(
        self,
        api_key: str,
        price_cache: PriceCache,
        poll_interval: float = 15.0,   # Free tier: 5 req/min → 15s
    ) -> None:
        self._api_key = api_key
        self._cache = price_cache
        self._interval = poll_interval
        self._tickers: list[str] = []
        self._task: asyncio.Task | None = None
        self._client: RESTClient | None = None

    async def start(self, tickers: list[str]) -> None:
        self._client = RESTClient(api_key=self._api_key)
        self._tickers = list(tickers)

        # Immediate first poll — cache is populated before start() returns
        await self._poll_once()

        self._task = asyncio.create_task(self._poll_loop(), name="massive-poller")
        logger.info("Massive poller started: %d tickers, %.1fs interval",
                    len(tickers), self._interval)

    async def stop(self) -> None:
        if self._task and not self._task.done():
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
        self._task = None
        self._client = None

    async def add_ticker(self, ticker: str) -> None:
        ticker = ticker.upper().strip()
        if ticker not in self._tickers:
            self._tickers.append(ticker)
            # Note: price appears on next poll cycle (≤ poll_interval delay)
            logger.info("Massive: added ticker %s (appears on next poll)", ticker)

    async def remove_ticker(self, ticker: str) -> None:
        ticker = ticker.upper().strip()
        self._tickers = [t for t in self._tickers if t != ticker]
        self._cache.remove(ticker)

    def get_tickers(self) -> list[str]:
        return list(self._tickers)

    async def _poll_loop(self) -> None:
        while True:
            await asyncio.sleep(self._interval)
            await self._poll_once()

    async def _poll_once(self) -> None:
        if not self._tickers or not self._client:
            return
        try:
            # Run the synchronous SDK call in a thread pool to avoid blocking
            snapshots = await asyncio.to_thread(self._fetch_snapshots)
            processed = 0
            for snap in snapshots:
                try:
                    price = snap.last_trade.price
                    timestamp = snap.last_trade.timestamp / 1000.0  # ms → seconds
                    self._cache.update(ticker=snap.ticker, price=price, timestamp=timestamp)
                    processed += 1
                except (AttributeError, TypeError) as e:
                    logger.warning("Skipping %s snapshot: %s",
                                   getattr(snap, "ticker", "???"), e)
            logger.debug("Massive poll: updated %d/%d tickers", processed, len(self._tickers))
        except Exception as e:
            logger.error("Massive poll failed: %s", e)
            # Don't re-raise — loop retries on next interval

    def _fetch_snapshots(self) -> list:
        return self._client.get_snapshot_all(
            market_type=SnapshotMarketType.STOCKS,
            tickers=self._tickers,
        )
```

### Error handling strategy

| HTTP status | Cause | Behavior |
|---|---|---|
| `401 Unauthorized` | Bad or missing API key | Logged as error; retry next interval |
| `403 Forbidden` | Endpoint above your plan | Logged as error; retry (won't succeed without upgrade) |
| `429 Too Many Requests` | Rate limit exceeded | Logged; increase `poll_interval` |
| Network error | Transient connectivity | Logged; retries automatically |

All errors are caught at the `_poll_once` level. The poll loop continues regardless — stale prices persist in the cache until the next successful poll.

### Rate limit guidance

```python
# Free / Starter tier: 5 requests/minute
source = MassiveDataSource(api_key=key, price_cache=cache, poll_interval=15.0)

# Advanced tier (real-time): higher limit
source = MassiveDataSource(api_key=key, price_cache=cache, poll_interval=5.0)

# Business tier
source = MassiveDataSource(api_key=key, price_cache=cache, poll_interval=2.0)
```

Batching all tickers into a single `get_snapshot_all` call keeps usage at **1 request per poll**, regardless of watchlist size.

### Daily change % with Massive

Unlike the simulator (which can only report tick-to-tick change), Massive provides the true daily change relative to the previous trading day's close:

```python
change_pct = snap.todays_change_perc  # e.g., +1.42 (%)

# The PriceUpdate.change_percent property reflects tick-to-tick change.
# For daily change, store todays_change_perc separately in the cache or
# include it in an extended response model.
```

This distinction matters for the positions table ("daily change %" column). See §9 for how to expose it via the portfolio endpoint.

### Historical bars (optional)

For backfilling charts with prior-day OHLCV data:

```python
import requests
from datetime import date, timedelta

api_key = os.environ["MASSIVE_API_KEY"]
ticker = "AAPL"
to_date = date.today()
from_date = to_date - timedelta(days=30)

resp = requests.get(
    f"https://api.massive.com/v2/aggs/ticker/{ticker}/range/1/day"
    f"/{from_date}/{to_date}",
    params={"adjusted": True, "sort": "asc", "limit": 5000},
    headers={"Authorization": f"Bearer {api_key}"},
)
data = resp.json()

for bar in data.get("results", []):
    ts = bar["t"] / 1000  # ms → seconds
    print(f"open={bar['o']} high={bar['h']} low={bar['l']} close={bar['c']} vol={bar['v']}")
```

---

## 7. Factory — Env-Driven Selection

The factory reads `MASSIVE_API_KEY` from the environment and returns the appropriate source. All downstream code is source-agnostic.

```python
# backend/app/market/factory.py
import logging
import os
from .cache import PriceCache
from .interface import MarketDataSource
from .massive_client import MassiveDataSource
from .simulator import SimulatorDataSource

logger = logging.getLogger(__name__)


def create_market_data_source(price_cache: PriceCache) -> MarketDataSource:
    """Select SimulatorDataSource or MassiveDataSource based on MASSIVE_API_KEY.

    Returns an unstarted source. Caller must await source.start(tickers).
    """
    api_key = os.environ.get("MASSIVE_API_KEY", "").strip()

    if api_key:
        logger.info("Market data source: Massive API (real data)")
        return MassiveDataSource(api_key=api_key, price_cache=price_cache)
    else:
        logger.info("Market data source: GBM Simulator")
        return SimulatorDataSource(price_cache=price_cache)
```

### Usage

```python
from app.market import PriceCache, create_market_data_source

cache = PriceCache()
source = create_market_data_source(cache)
# source is SimulatorDataSource or MassiveDataSource — caller doesn't care

await source.start(["AAPL", "GOOGL", "MSFT", ...])
# ...
await source.stop()
```

### Switching data sources in tests

```python
# In tests, bypass the factory and construct directly:
from app.market.simulator import SimulatorDataSource
from app.market.massive_client import MassiveDataSource

# Fast, deterministic, no network:
source = SimulatorDataSource(price_cache=cache, update_interval=0.1)

# With a real or mocked API key:
source = MassiveDataSource(api_key="test_key", price_cache=cache, poll_interval=1.0)
```

---

## 8. SSE Streaming

The SSE endpoint pushes price updates to connected browser clients. It uses a version-gated loop so it only sends events when prices have actually changed.

### Endpoint

```
GET /api/stream/prices
Content-Type: text/event-stream
```

### Wire format

```
retry: 1000

data: {"AAPL": {"ticker": "AAPL", "price": 190.42, "previous_price": 190.10, "timestamp": 1716123456.789, "change": 0.32, "change_percent": 0.1683, "direction": "up"}, "GOOGL": {...}, ...}

data: {"AAPL": {...}, "GOOGL": {...}, ...}
```

- `retry: 1000` — browser auto-reconnects after 1 second if the connection drops
- Each `data:` line contains all tracked tickers in a single JSON object
- Blank line `\n\n` terminates each SSE event

### Implementation

```python
# backend/app/market/stream.py
import asyncio
import json
import logging
from collections.abc import AsyncGenerator
from fastapi import APIRouter, Request
from fastapi.responses import StreamingResponse
from .cache import PriceCache

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/api/stream", tags=["streaming"])


def create_stream_router(price_cache: PriceCache) -> APIRouter:
    @router.get("/prices")
    async def stream_prices(request: Request) -> StreamingResponse:
        return StreamingResponse(
            _generate_events(price_cache, request),
            media_type="text/event-stream",
            headers={
                "Cache-Control": "no-cache",
                "Connection": "keep-alive",
                "X-Accel-Buffering": "no",   # Disable nginx buffering if proxied
            },
        )
    return router


async def _generate_events(
    price_cache: PriceCache,
    request: Request,
    interval: float = 0.5,
) -> AsyncGenerator[str, None]:
    yield "retry: 1000\n\n"

    last_version = -1
    client_ip = request.client.host if request.client else "unknown"
    logger.info("SSE client connected: %s", client_ip)

    try:
        while True:
            if await request.is_disconnected():
                logger.info("SSE client disconnected: %s", client_ip)
                break

            current_version = price_cache.version
            if current_version != last_version:
                last_version = current_version
                prices = price_cache.get_all()
                if prices:
                    data = {ticker: update.to_dict() for ticker, update in prices.items()}
                    yield f"data: {json.dumps(data)}\n\n"

            await asyncio.sleep(interval)
    except asyncio.CancelledError:
        logger.info("SSE stream cancelled for: %s", client_ip)
```

### Frontend connection

```typescript
// Connect to the SSE stream
const es = new EventSource("/api/stream/prices");

es.onmessage = (event) => {
  const updates: Record<string, PriceUpdate> = JSON.parse(event.data);

  for (const [ticker, update] of Object.entries(updates)) {
    // Flash green or red based on direction
    flashCell(ticker, update.direction);
    // Update displayed price
    setPrice(ticker, update.price);
    // Append to sparkline
    appendSparkline(ticker, update.price);
  }
};

es.onerror = () => {
  // EventSource auto-reconnects using the retry: 1000 directive
  setConnectionStatus("reconnecting");
};
```

### Per-ticker event format (TypeScript type)

```typescript
interface PriceUpdate {
  ticker: string;
  price: number;
  previous_price: number;
  timestamp: number;        // Unix seconds
  change: number;           // absolute tick-to-tick change
  change_percent: number;   // tick-to-tick % change
  direction: "up" | "down" | "flat";
}
```

---

## 9. FastAPI Integration

Complete wiring of the market data subsystem into a FastAPI application.

### Lifespan pattern

```python
# backend/app/main.py
from contextlib import asynccontextmanager
from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles
from app.market import PriceCache, create_market_data_source, create_stream_router

DEFAULT_TICKERS = [
    "AAPL", "GOOGL", "MSFT", "AMZN", "TSLA",
    "NVDA", "META", "JPM", "V", "NFLX",
]

price_cache = PriceCache()
market_source = create_market_data_source(price_cache)


@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup: start market data, seeding the cache before first request
    await market_source.start(DEFAULT_TICKERS)

    # Store on app.state for dependency injection in routes
    app.state.price_cache = price_cache
    app.state.market_source = market_source

    yield

    # Shutdown: stop the background task cleanly
    await market_source.stop()


app = FastAPI(lifespan=lifespan)

# SSE streaming
app.include_router(create_stream_router(price_cache))

# Serve frontend static files (Next.js export)
app.mount("/", StaticFiles(directory="static", html=True), name="frontend")
```

### Dependency injection for routes

```python
# backend/app/dependencies.py
from fastapi import Request
from app.market import PriceCache, MarketDataSource


def get_price_cache(request: Request) -> PriceCache:
    return request.app.state.price_cache


def get_market_source(request: Request) -> MarketDataSource:
    return request.app.state.market_source
```

### Watchlist route example

```python
# backend/app/routes/watchlist.py
from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel
from app.market import PriceCache, MarketDataSource
from app.dependencies import get_price_cache, get_market_source
import db  # your database layer

router = APIRouter(prefix="/api/watchlist", tags=["watchlist"])


class AddTickerRequest(BaseModel):
    ticker: str


@router.get("")
def get_watchlist(cache: PriceCache = Depends(get_price_cache)):
    tickers = db.get_watchlist()
    result = []
    for ticker in tickers:
        update = cache.get(ticker)
        result.append({
            "ticker": ticker,
            "price": update.price if update else None,
            "direction": update.direction if update else None,
            "change_percent": update.change_percent if update else None,
        })
    return result


@router.post("")
async def add_ticker(
    body: AddTickerRequest,
    source: MarketDataSource = Depends(get_market_source),
):
    ticker = body.ticker.upper().strip()
    if not ticker:
        raise HTTPException(status_code=422, detail="Ticker cannot be empty")

    db.add_to_watchlist(ticker)
    await source.add_ticker(ticker)
    return {"ticker": ticker, "added": True}


@router.delete("/{ticker}")
async def remove_ticker(
    ticker: str,
    source: MarketDataSource = Depends(get_market_source),
):
    ticker = ticker.upper()
    db.remove_from_watchlist(ticker)
    await source.remove_ticker(ticker)
    return {"ticker": ticker, "removed": True}
```

---

## 10. Watchlist Management

The tracked ticker set must be the union of the watchlist and any open positions. This ensures portfolio valuations are always possible even if a user removes a ticker from their watchlist after buying shares.

```python
# Correct ticker set = watchlist ∪ open positions
def get_tracked_tickers() -> list[str]:
    watchlist = set(db.get_watchlist())
    held = {pos.ticker for pos in db.get_positions() if pos.quantity > 0}
    return sorted(watchlist | held)
```

### Startup initialisation

```python
@asynccontextmanager
async def lifespan(app: FastAPI):
    # Seed with watchlist + held positions at startup
    initial_tickers = get_tracked_tickers()
    await market_source.start(initial_tickers)
    app.state.price_cache = price_cache
    app.state.market_source = market_source
    yield
    await market_source.stop()
```

### Watchlist / position changes mid-session

```python
# When adding to watchlist (always add to source)
async def add_ticker_to_watchlist(ticker: str, source: MarketDataSource):
    db.add_to_watchlist(ticker)
    await source.add_ticker(ticker)

# When removing from watchlist (only remove from source if no open position)
async def remove_ticker_from_watchlist(ticker: str, source: MarketDataSource):
    db.remove_from_watchlist(ticker)
    position = db.get_position(ticker)
    if not position or position.quantity == 0:
        await source.remove_ticker(ticker)
    # else: keep tracking for portfolio valuation

# After fully closing a position (remove only if also not on watchlist)
async def on_position_closed(ticker: str, source: MarketDataSource):
    if ticker not in db.get_watchlist():
        await source.remove_ticker(ticker)
```

### New ticker seed price (simulator)

When a user adds an unknown ticker (e.g., `PYPL`), the simulator assigns a random starting price between $50 and $300. To register a known seed price before it can be added:

```python
# backend/app/market/seed_prices.py — extend these dicts:
SEED_PRICES["PYPL"] = 65.00
TICKER_PARAMS["PYPL"] = {"sigma": 0.35, "mu": 0.03}
CORRELATION_GROUPS["finance"].add("PYPL")
```

No other code changes are needed.

---

## 11. Portfolio Valuation

Downstream code reads from `PriceCache`. It never touches the data source.

### GET /api/portfolio response shape

```json
{
  "cash_balance": 7523.40,
  "total_value": 12847.23,
  "positions": [
    {
      "ticker": "AAPL",
      "quantity": 10.0,
      "avg_cost": 188.50,
      "current_price": 190.42,
      "market_value": 1904.20,
      "unrealized_pnl": 19.20,
      "unrealized_pnl_pct": 1.019,
      "direction": "up",
      "change_percent": 0.1683
    }
  ]
}
```

### Implementation

```python
@router.get("/api/portfolio")
def get_portfolio(
    cache: PriceCache = Depends(get_price_cache),
):
    user = db.get_user()
    positions_raw = db.get_positions()
    all_prices = cache.get_all()

    positions = []
    total_market_value = 0.0

    for pos in positions_raw:
        update = all_prices.get(pos.ticker)
        current_price = update.price if update else pos.avg_cost  # fallback

        market_value = current_price * pos.quantity
        unrealized_pnl = (current_price - pos.avg_cost) * pos.quantity
        cost_basis = pos.avg_cost * pos.quantity
        unrealized_pct = (unrealized_pnl / cost_basis * 100) if cost_basis else 0.0
        total_market_value += market_value

        positions.append({
            "ticker": pos.ticker,
            "quantity": pos.quantity,
            "avg_cost": pos.avg_cost,
            "current_price": current_price,
            "market_value": round(market_value, 2),
            "unrealized_pnl": round(unrealized_pnl, 2),
            "unrealized_pnl_pct": round(unrealized_pct, 3),
            "direction": update.direction if update else "flat",
            "change_percent": update.change_percent if update else 0.0,
        })

    return {
        "cash_balance": user.cash_balance,
        "total_value": round(user.cash_balance + total_market_value, 2),
        "positions": positions,
    }
```

### Trade execution — price lookup

```python
@router.post("/api/portfolio/trade")
def execute_trade(
    body: TradeRequest,
    cache: PriceCache = Depends(get_price_cache),
):
    ticker = body.ticker.upper()
    price = cache.get_price(ticker)

    if price is None:
        raise HTTPException(
            status_code=422,
            detail=f"No price available for {ticker}. Is it on the watchlist?"
        )

    user = db.get_user()

    if body.side == "buy":
        cost = price * body.quantity
        if cost > user.cash_balance:
            raise HTTPException(
                status_code=422,
                detail=f"Insufficient funds: need ${cost:.2f}, have ${user.cash_balance:.2f}"
            )
        db.execute_buy(ticker, body.quantity, price)

    elif body.side == "sell":
        position = db.get_position(ticker)
        if not position or position.quantity < body.quantity:
            available = position.quantity if position else 0
            raise HTTPException(
                status_code=422,
                detail=f"Insufficient shares: need {body.quantity}, have {available}"
            )
        db.execute_sell(ticker, body.quantity, price)

    return {"ticker": ticker, "side": body.side, "quantity": body.quantity, "price": price}
```

---

## 12. Adding a New Data Source

To add a third implementation (e.g., a CSV replay source for backtesting), implement `MarketDataSource` and wire it into the factory.

### Step 1: Implement the interface

```python
# backend/app/market/csv_replay.py
import asyncio
import csv
import time
from .cache import PriceCache
from .interface import MarketDataSource


class CsvReplaySource(MarketDataSource):
    """Replays historical OHLCV data from a CSV file for backtesting."""

    def __init__(self, csv_path: str, price_cache: PriceCache, speed: float = 1.0) -> None:
        self._path = csv_path
        self._cache = price_cache
        self._speed = speed   # 1.0 = real-time, 10.0 = 10x faster
        self._task: asyncio.Task | None = None
        self._tickers: list[str] = []

    async def start(self, tickers: list[str]) -> None:
        self._tickers = list(tickers)
        self._task = asyncio.create_task(self._replay_loop(), name="csv-replay")

    async def stop(self) -> None:
        if self._task and not self._task.done():
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
        self._task = None

    async def add_ticker(self, ticker: str) -> None:
        if ticker not in self._tickers:
            self._tickers.append(ticker)

    async def remove_ticker(self, ticker: str) -> None:
        self._tickers = [t for t in self._tickers if t != ticker]
        self._cache.remove(ticker)

    def get_tickers(self) -> list[str]:
        return list(self._tickers)

    async def _replay_loop(self) -> None:
        rows = await asyncio.to_thread(self._load_csv)
        prev_ts = None
        for row in rows:
            ts = float(row["timestamp"])
            if prev_ts is not None:
                delay = (ts - prev_ts) / self._speed
                await asyncio.sleep(max(delay, 0))
            self._cache.update(ticker=row["ticker"], price=float(row["close"]), timestamp=ts)
            prev_ts = ts

    def _load_csv(self) -> list[dict]:
        with open(self._path) as f:
            return list(csv.DictReader(f))
```

### Step 2: Wire into the factory

```python
# backend/app/market/factory.py — extended version
def create_market_data_source(price_cache: PriceCache) -> MarketDataSource:
    csv_path = os.environ.get("CSV_REPLAY_PATH", "").strip()
    api_key = os.environ.get("MASSIVE_API_KEY", "").strip()

    if csv_path:
        logger.info("Market data source: CSV replay (%s)", csv_path)
        from .csv_replay import CsvReplaySource
        return CsvReplaySource(csv_path=csv_path, price_cache=price_cache)
    elif api_key:
        logger.info("Market data source: Massive API (real data)")
        return MassiveDataSource(api_key=api_key, price_cache=price_cache)
    else:
        logger.info("Market data source: GBM Simulator")
        return SimulatorDataSource(price_cache=price_cache)
```

No other code needs to change. Portfolio routes, SSE streaming, and trade execution all read from `PriceCache` and are completely unaware of the data source.

---

## 13. Testing Patterns

### Unit testing PriceCache

```python
# tests/market/test_cache.py
from app.market import PriceCache, PriceUpdate


def test_first_update_is_flat():
    cache = PriceCache()
    update = cache.update("AAPL", 190.00)
    assert update.direction == "flat"
    assert update.change == 0.0
    assert update.change_percent == 0.0


def test_price_increases_direction():
    cache = PriceCache()
    cache.update("AAPL", 190.00)
    update = cache.update("AAPL", 191.00)
    assert update.direction == "up"
    assert update.change == 1.0


def test_version_increments_on_update():
    cache = PriceCache()
    v0 = cache.version
    cache.update("AAPL", 190.00)
    assert cache.version == v0 + 1
    cache.update("AAPL", 191.00)
    assert cache.version == v0 + 2


def test_remove_clears_ticker():
    cache = PriceCache()
    cache.update("AAPL", 190.00)
    cache.remove("AAPL")
    assert cache.get("AAPL") is None
    assert "AAPL" not in cache
```

### Unit testing GBMSimulator

```python
# tests/market/test_simulator.py
import math
from app.market.simulator import GBMSimulator


def test_prices_stay_positive():
    sim = GBMSimulator(tickers=["AAPL", "TSLA", "NVDA"])
    for _ in range(1000):
        prices = sim.step()
        for ticker, price in prices.items():
            assert price > 0, f"{ticker} went non-positive"


def test_step_returns_all_tickers():
    sim = GBMSimulator(tickers=["AAPL", "GOOGL"])
    prices = sim.step()
    assert set(prices.keys()) == {"AAPL", "GOOGL"}


def test_add_ticker_appears_in_step():
    sim = GBMSimulator(tickers=["AAPL"])
    sim.add_ticker("MSFT")
    prices = sim.step()
    assert "MSFT" in prices


def test_remove_ticker_absent_from_step():
    sim = GBMSimulator(tickers=["AAPL", "GOOGL"])
    sim.remove_ticker("GOOGL")
    prices = sim.step()
    assert "GOOGL" not in prices


def test_shock_event_applied(monkeypatch):
    import random
    monkeypatch.setattr(random, "random", lambda: 0.0)   # always trigger event
    monkeypatch.setattr(random, "uniform", lambda a, b: 0.05)   # 5% shock
    monkeypatch.setattr(random, "choice", lambda seq: 1)        # always up

    sim = GBMSimulator(tickers=["AAPL"], event_probability=1.0)
    initial = sim.get_price("AAPL")
    prices = sim.step()
    # Price should have moved more than GBM alone would produce
    assert prices["AAPL"] > initial * 1.04


def test_dt_calibration():
    expected = 0.5 / (252 * 6.5 * 3600)
    assert math.isclose(GBMSimulator.DEFAULT_DT, expected, rel_tol=1e-9)
```

### Integration testing SimulatorDataSource

```python
# tests/market/test_simulator_source.py
import asyncio
import pytest
from app.market import PriceCache
from app.market.simulator import SimulatorDataSource


@pytest.fixture
async def running_source():
    cache = PriceCache()
    source = SimulatorDataSource(price_cache=cache, update_interval=0.05)
    await source.start(["AAPL", "MSFT"])
    yield cache, source
    await source.stop()


async def test_cache_populated_immediately(running_source):
    cache, _ = running_source
    # Cache should have data before any tick fires — seeded in start()
    assert cache.get("AAPL") is not None
    assert cache.get("MSFT") is not None


async def test_prices_update_after_one_tick(running_source):
    cache, _ = running_source
    v0 = cache.version
    await asyncio.sleep(0.1)  # wait for at least one 50ms tick
    assert cache.version > v0


async def test_add_ticker_seeds_cache_immediately(running_source):
    cache, source = running_source
    await source.add_ticker("NVDA")
    # Price should be in cache right away, before next tick
    assert cache.get("NVDA") is not None


async def test_remove_ticker_clears_cache(running_source):
    cache, source = running_source
    await source.remove_ticker("MSFT")
    assert cache.get("MSFT") is None


async def test_stop_cancels_task(running_source):
    cache, source = running_source
    await source.stop()
    v = cache.version
    await asyncio.sleep(0.15)
    # No more updates after stop
    assert cache.version == v
```

### Mocking MassiveDataSource in tests

```python
# tests/market/test_massive.py
import asyncio
from unittest.mock import MagicMock, patch
import pytest
from app.market import PriceCache
from app.market.massive_client import MassiveDataSource


def make_snapshot(ticker: str, price: float, ts_ms: int = 1716123456789) -> MagicMock:
    snap = MagicMock()
    snap.ticker = ticker
    snap.last_trade.price = price
    snap.last_trade.timestamp = ts_ms
    return snap


@pytest.fixture
def cache():
    return PriceCache()


@pytest.fixture
def source(cache):
    return MassiveDataSource(api_key="test_key", price_cache=cache, poll_interval=60.0)


async def test_start_polls_immediately(source, cache):
    fake_snapshots = [make_snapshot("AAPL", 190.42)]
    source._client = MagicMock()
    source._client.get_snapshot_all.return_value = fake_snapshots

    await source.start(["AAPL"])
    assert cache.get_price("AAPL") == 190.42
    await source.stop()


async def test_poll_updates_cache(source, cache):
    source._tickers = ["AAPL", "GOOGL"]
    source._client = MagicMock()
    source._client.get_snapshot_all.return_value = [
        make_snapshot("AAPL", 191.00),
        make_snapshot("GOOGL", 176.50),
    ]
    await source._poll_once()
    assert cache.get_price("AAPL") == 191.00
    assert cache.get_price("GOOGL") == 176.50


async def test_poll_survives_api_error(source, cache):
    source._tickers = ["AAPL"]
    source._client = MagicMock()
    source._client.get_snapshot_all.side_effect = ConnectionError("timeout")
    # Should not raise — error is logged and loop continues
    await source._poll_once()


async def test_remove_ticker_clears_cache(source, cache):
    cache.update("MSFT", 420.00)
    source._tickers = ["AAPL", "MSFT"]
    await source.remove_ticker("MSFT")
    assert cache.get("MSFT") is None
    assert "MSFT" not in source.get_tickers()
```

### Testing the factory

```python
# tests/market/test_factory.py
import os
from unittest.mock import patch
from app.market import PriceCache, create_market_data_source
from app.market.simulator import SimulatorDataSource
from app.market.massive_client import MassiveDataSource


def test_returns_simulator_without_key():
    with patch.dict(os.environ, {}, clear=True):
        os.environ.pop("MASSIVE_API_KEY", None)
        source = create_market_data_source(PriceCache())
    assert isinstance(source, SimulatorDataSource)


def test_returns_massive_with_key():
    with patch.dict(os.environ, {"MASSIVE_API_KEY": "test_key"}):
        source = create_market_data_source(PriceCache())
    assert isinstance(source, MassiveDataSource)


def test_whitespace_only_key_returns_simulator():
    with patch.dict(os.environ, {"MASSIVE_API_KEY": "   "}):
        source = create_market_data_source(PriceCache())
    assert isinstance(source, SimulatorDataSource)
```

### Running the full test suite

```bash
cd backend

# All tests
uv run --extra dev pytest -v

# Market data tests only
uv run --extra dev pytest tests/market/ -v

# With coverage
uv run --extra dev pytest --cov=app --cov-report=term-missing

# Single module
uv run --extra dev pytest tests/market/test_simulator.py -v

# Lint
uv run --extra dev ruff check app/ tests/
```

---

## 14. Configuration Reference

### Environment variables

| Variable | Default | Effect |
|---|---|---|
| `MASSIVE_API_KEY` | (empty) | If set and non-empty, uses Massive REST API. Otherwise uses GBM simulator. |
| `LLM_MOCK` | `false` | If `true`, LLM calls return deterministic mock responses (for E2E tests). |

### Simulator tuning

All parameters are passed at construction time or via `seed_prices.py`.

```python
# Faster ticks (250ms)
SimulatorDataSource(price_cache=cache, update_interval=0.25)

# More dramatic shock events (~every 10s with 10 tickers)
SimulatorDataSource(price_cache=cache, event_probability=0.005)

# Calmer simulation (no shocks)
SimulatorDataSource(price_cache=cache, event_probability=0.0)

# Per-ticker volatility — edit seed_prices.py:
TICKER_PARAMS["NVDA"] = {"sigma": 0.60, "mu": 0.10}   # More volatile NVDA
TICKER_PARAMS["V"]    = {"sigma": 0.10, "mu": 0.03}   # Even calmer Visa
```

### Massive API poll intervals

```python
# Match your Massive subscription tier:
MassiveDataSource(api_key=key, price_cache=cache, poll_interval=15.0)  # Free/Starter
MassiveDataSource(api_key=key, price_cache=cache, poll_interval=5.0)   # Advanced
MassiveDataSource(api_key=key, price_cache=cache, poll_interval=2.0)   # Business
```

### Public API imports

Everything a downstream module needs is re-exported from `app.market`:

```python
from app.market import (
    PriceUpdate,              # Immutable price snapshot dataclass
    PriceCache,               # Thread-safe price store
    MarketDataSource,         # Abstract interface (for type hints)
    create_market_data_source,# Factory function
    create_stream_router,     # SSE router factory
)
```

---

## Quick-start: Standalone Demo

A Rich terminal dashboard visualising the simulator is available without any server setup:

```bash
cd backend
uv run market_data_demo.py
```

Displays all 10 tickers with live-updating prices, colour-coded direction arrows, sparklines, and a shock-event log. Runs for 60 seconds or until `Ctrl+C`.

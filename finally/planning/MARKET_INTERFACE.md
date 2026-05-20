# Market Interface — Unified Python API

## Purpose

All market data in FinAlly flows through a single unified interface regardless of whether prices come from the Massive API or the built-in simulator. Backend code that needs prices (portfolio valuation, trade execution, SSE streaming) never calls a data source directly — it reads from the `PriceCache`. The data source is an invisible producer.

This separation means:
- New data sources (WebSocket, CSV replay) can be added by implementing one ABC
- Tests substitute a fake source without touching downstream code
- The simulator and Massive client are validated against the same behavioral contract

---

## Module Layout

```
backend/app/market/
├── __init__.py        # Public re-exports
├── interface.py       # MarketDataSource — abstract base class
├── models.py          # PriceUpdate — immutable price snapshot
├── cache.py           # PriceCache — thread-safe in-memory store
├── seed_prices.py     # Starting prices and GBM params for default tickers
├── simulator.py       # GBMSimulator + SimulatorDataSource
├── massive_client.py  # MassiveDataSource (Polygon.io REST polling)
├── factory.py         # create_market_data_source() — env-driven selection
└── stream.py          # FastAPI SSE endpoint factory
```

---

## The `PriceUpdate` Model (`models.py`)

The canonical representation of a single price tick. Frozen dataclass — immutable once created.

```python
from app.market import PriceUpdate

update = PriceUpdate(
    ticker="AAPL",
    price=190.42,
    previous_price=190.10,
    timestamp=1716123456.789,   # Unix seconds (float)
)

update.change          # 0.32  (absolute)
update.change_percent  # 0.1683 (percent)
update.direction       # "up" | "down" | "flat"
update.to_dict()       # dict for JSON / SSE serialization
```

### `to_dict()` output shape

```json
{
  "ticker": "AAPL",
  "price": 190.42,
  "previous_price": 190.10,
  "timestamp": 1716123456.789,
  "change": 0.32,
  "change_percent": 0.1683,
  "direction": "up"
}
```

This is the exact payload sent over the SSE stream and consumed by the frontend.

---

## The `PriceCache` (`cache.py`)

Thread-safe in-memory store. One writer (the active data source), multiple readers (SSE, portfolio, trades).

```python
from app.market import PriceCache

cache = PriceCache()

# Writing (done by the data source internally)
update = cache.update("AAPL", price=190.42)           # returns PriceUpdate
update = cache.update("AAPL", price=191.00, timestamp=1716123500.0)

# Reading
update: PriceUpdate | None = cache.get("AAPL")
price: float | None        = cache.get_price("AAPL")  # shorthand
all_prices: dict[str, PriceUpdate] = cache.get_all()  # shallow copy

# Watchlist management
cache.remove("GOOGL")

# SSE change detection
version: int = cache.version  # increments on every update()
```

### Thread safety

`PriceCache` uses a `threading.Lock` internally. All reads and writes acquire the lock. It is safe to read from the SSE async generator (main event loop thread) while the Massive poller writes from `asyncio.to_thread()`.

### Version counter

The `version` property is a monotonically increasing integer bumped on every `update()` call. The SSE generator compares its last-seen version against the current version to decide whether to push an event. This prevents unnecessary sends when prices haven't changed.

---

## The `MarketDataSource` ABC (`interface.py`)

```python
from abc import ABC, abstractmethod

class MarketDataSource(ABC):
    @abstractmethod
    async def start(self, tickers: list[str]) -> None: ...
    
    @abstractmethod
    async def stop(self) -> None: ...
    
    @abstractmethod
    async def add_ticker(self, ticker: str) -> None: ...
    
    @abstractmethod
    async def remove_ticker(self, ticker: str) -> None: ...
    
    @abstractmethod
    def get_tickers(self) -> list[str]: ...
```

### Contract

| Method | Behavior |
|---|---|
| `start(tickers)` | Starts a background task that periodically writes prices to the cache. Call once at app startup. |
| `stop()` | Cancels the background task. Safe to call multiple times. |
| `add_ticker(ticker)` | Adds a ticker to the active set. Takes effect on the next update cycle. |
| `remove_ticker(ticker)` | Removes a ticker and clears it from the cache. |
| `get_tickers()` | Returns the current list of actively tracked tickers. |

Both `SimulatorDataSource` and `MassiveDataSource` conform to this interface identically.

---

## The Factory (`factory.py`)

```python
from app.market import PriceCache, create_market_data_source

cache = PriceCache()
source = create_market_data_source(cache)
# → MassiveDataSource  if MASSIVE_API_KEY is set and non-empty
# → SimulatorDataSource otherwise
```

### Selection logic

```python
# factory.py
api_key = os.environ.get("MASSIVE_API_KEY", "").strip()

if api_key:
    return MassiveDataSource(api_key=api_key, price_cache=cache)
else:
    return SimulatorDataSource(price_cache=cache)
```

The returned source is **not yet started**. Call `await source.start(tickers)` after creation.

---

## Full Startup / Shutdown Pattern

```python
# backend/app/main.py (FastAPI lifespan)
from contextlib import asynccontextmanager
from fastapi import FastAPI
from app.market import PriceCache, create_market_data_source, create_stream_router

DEFAULT_TICKERS = ["AAPL", "GOOGL", "MSFT", "AMZN", "TSLA",
                   "NVDA", "META", "JPM", "V", "NFLX"]

price_cache = PriceCache()
market_source = create_market_data_source(price_cache)

@asynccontextmanager
async def lifespan(app: FastAPI):
    await market_source.start(DEFAULT_TICKERS)
    yield
    await market_source.stop()

app = FastAPI(lifespan=lifespan)
app.include_router(create_stream_router(price_cache))
```

---

## Reading Prices in API Routes

Portfolio routes and trade execution read from the cache — never from the source directly.

```python
from app.market import PriceCache

# In a route that needs current prices:
def get_portfolio(cache: PriceCache = Depends(get_price_cache)):
    all_prices = cache.get_all()
    
    for ticker, update in all_prices.items():
        current_price = update.price
        daily_change_pct = update.change_percent  # tick-to-tick, not daily
```

### Daily change %

**Important distinction**: `PriceUpdate.change_percent` reflects the change between the *previous tick* and the *current tick*, not the change since market open. For a true daily % change:

- **With Massive API**: use `snap.todays_change_perc` from the snapshot response (see `MASSIVE_API_KEY.md`)
- **With simulator**: track the session-start price per ticker and calculate `(current - session_start) / session_start * 100`

The PLAN.md notes this as an open question — both implementations currently expose tick-to-tick change only.

---

## Managing the Watchlist

When the user adds or removes a ticker via the watchlist API, the route calls through to the market source:

```python
# Add a ticker
await market_source.add_ticker("PYPL")
# → SimulatorDataSource: adds to GBM, seeds cache immediately with starting price
# → MassiveDataSource: appends to ticker list, appears in next poll (≤15s delay)

# Remove a ticker
await market_source.remove_ticker("NFLX")
# → Both implementations: removes from active set AND clears from cache
```

---

## SSE Streaming (`stream.py`)

The `create_stream_router` factory wires the price cache into a FastAPI router:

```python
from app.market import create_stream_router

router = create_stream_router(price_cache)
# Registers: GET /api/stream/prices  (text/event-stream)
```

### SSE event format

Each event is a JSON object mapping ticker → `PriceUpdate.to_dict()`:

```
retry: 1000

data: {"AAPL": {"ticker": "AAPL", "price": 190.42, "previous_price": 190.10, "timestamp": 1716123456.789, "change": 0.32, "change_percent": 0.1683, "direction": "up"}, "GOOGL": {...}, ...}

data: {...}
```

The frontend connects with `new EventSource("/api/stream/prices")` and receives updates every ~500ms when prices have changed (version-gated).

---

## Writing a New Data Source

To add a third implementation (e.g., a CSV replay source for backtesting):

```python
# backend/app/market/csv_replay.py
from .cache import PriceCache
from .interface import MarketDataSource

class CsvReplaySource(MarketDataSource):
    def __init__(self, csv_path: str, price_cache: PriceCache) -> None:
        self._path = csv_path
        self._cache = price_cache
        self._task = None

    async def start(self, tickers: list[str]) -> None:
        # Load CSV, start asyncio task that replays rows at wall-clock speed
        self._task = asyncio.create_task(self._replay_loop())

    async def stop(self) -> None:
        if self._task:
            self._task.cancel()

    async def add_ticker(self, ticker: str) -> None:
        pass  # CSV replay tracks whatever's in the file

    async def remove_ticker(self, ticker: str) -> None:
        self._cache.remove(ticker)

    def get_tickers(self) -> list[str]:
        return []  # implemented as needed
```

Then add it to `factory.py` with its own env-var trigger. No other code needs to change.

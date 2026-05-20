# Market Simulator — Design & Code Structure

## Purpose

The simulator generates realistic-looking stock prices when no `MASSIVE_API_KEY` is set. It is the default data source — zero external dependencies, runs in-process, starts instantly. It produces prices that:

- Move continuously at ~500ms intervals
- Drift realistically over time (not pure random walk)
- Correlate across tickers by sector (tech stocks move together)
- Occasionally spike with dramatic 2-5% moves for visual interest

The simulator is intentionally faithful to financial math (Geometric Brownian Motion) while remaining simple enough that any developer can understand and modify it.

---

## Math: Geometric Brownian Motion (GBM)

GBM is the standard model for stock price dynamics. Each tick:

```
S(t+dt) = S(t) * exp((mu - sigma²/2) * dt + sigma * sqrt(dt) * Z)
```

Where:
- `S(t)` — current price
- `mu` — annualized drift (expected return). Positive = upward trend on average.
- `sigma` — annualized volatility. Higher = wider price swings.
- `dt` — time step as a fraction of a trading year
- `Z` — standard normal random variable (the "shock")

### Why GBM?

- **Prices stay positive**: the exponential form means `S` can never go negative
- **Log-normal distribution**: realistic fat tail behavior
- **Configurable per stock**: TSLA has higher `sigma` than JPM
- **Industry standard**: the basis of Black-Scholes and most simulation tools

### Time step calibration

The simulator runs at 500ms intervals. A trading year is approximately:

```
252 trading days × 6.5 hours/day × 3600 seconds/hour = 5,896,800 seconds
dt = 0.5 / 5,896,800 ≈ 8.48e-8  (one tick as fraction of a year)
```

With this tiny `dt`, each tick produces sub-cent moves that accumulate naturally. At 2 ticks/second, a stock with `sigma=0.25` (25% annualized vol) will move ~±0.6 cents per tick on a $190 stock — realistic and visually smooth.

---

## Code Structure

### `GBMSimulator` (core engine)

```python
# backend/app/market/simulator.py

class GBMSimulator:
    TRADING_SECONDS_PER_YEAR = 252 * 6.5 * 3600  # 5,896,800
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
```

**State:**
- `_tickers` — ordered list (order matters for the Cholesky matrix)
- `_prices` — current price per ticker
- `_params` — `{"sigma": float, "mu": float}` per ticker
- `_cholesky` — lower-triangular matrix for generating correlated random draws

### `step()` — the hot path

Called every 500ms. Returns a dict of new prices.

```python
def step(self) -> dict[str, float]:
    n = len(self._tickers)
    if n == 0:
        return {}

    # 1. Draw n independent standard normal samples
    z_independent = np.random.standard_normal(n)

    # 2. Apply Cholesky to introduce sector correlations
    z_correlated = self._cholesky @ z_independent  # shape: (n,)

    result: dict[str, float] = {}
    for i, ticker in enumerate(self._tickers):
        mu, sigma = self._params[ticker]["mu"], self._params[ticker]["sigma"]

        # 3. GBM update
        drift = (mu - 0.5 * sigma**2) * self._dt
        diffusion = sigma * math.sqrt(self._dt) * z_correlated[i]
        self._prices[ticker] *= math.exp(drift + diffusion)

        # 4. Random shock event (~0.1% chance)
        if random.random() < self._event_prob:
            shock = random.uniform(0.02, 0.05)
            sign = random.choice([-1, 1])
            self._prices[ticker] *= 1 + shock * sign

        result[ticker] = round(self._prices[ticker], 2)

    return result
```

---

## Correlation: Cholesky Decomposition

Stocks in the same sector tend to move together. The simulator reproduces this via a correlation matrix and Cholesky decomposition.

### How it works

1. Build an `n × n` correlation matrix where `corr[i,j]` is the pairwise correlation between ticker `i` and ticker `j`
2. Compute the lower-triangular Cholesky factor `L` such that `L @ L.T = corr`
3. Multiply independent standard normals `z` by `L` → correlated normals: `L @ z`

If `z_i` and `z_j` are independent N(0,1), then `L @ z` has the covariance structure of `corr`. Tickers that correlate at 0.6 will tend to move in the same direction 60% of the time.

### Correlation structure

```python
# backend/app/market/seed_prices.py

CORRELATION_GROUPS = {
    "tech":    {"AAPL", "GOOGL", "MSFT", "AMZN", "META", "NVDA", "NFLX"},
    "finance": {"JPM", "V"},
}

INTRA_TECH_CORR    = 0.6   # Tech stocks move together strongly
INTRA_FINANCE_CORR = 0.5   # Finance stocks correlate moderately
CROSS_GROUP_CORR   = 0.3   # Cross-sector or unknown tickers
TSLA_CORR          = 0.3   # TSLA is in tech but behaves independently
```

### Matrix rebuild

The Cholesky matrix is rebuilt whenever tickers are added or removed (`_rebuild_cholesky()`). This is O(n²) but `n < 50` in practice, so it's negligible.

```python
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
```

---

## Seed Prices and Per-Ticker Parameters

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
    "AAPL":  {"sigma": 0.22, "mu": 0.05},  # Stable, moderate growth
    "GOOGL": {"sigma": 0.25, "mu": 0.05},
    "MSFT":  {"sigma": 0.20, "mu": 0.05},  # Lowest vol in tech
    "AMZN":  {"sigma": 0.28, "mu": 0.05},
    "TSLA":  {"sigma": 0.50, "mu": 0.03},  # High vol, lower drift
    "NVDA":  {"sigma": 0.40, "mu": 0.08},  # High vol, strong upward drift
    "META":  {"sigma": 0.30, "mu": 0.05},
    "JPM":   {"sigma": 0.18, "mu": 0.04},  # Low vol (bank)
    "V":     {"sigma": 0.17, "mu": 0.04},  # Lowest vol overall (payments)
    "NFLX":  {"sigma": 0.35, "mu": 0.05},
}

# Fallback for dynamically added tickers (e.g., user adds PYPL mid-session)
DEFAULT_PARAMS = {"sigma": 0.25, "mu": 0.05}
```

### Dynamic ticker starting price

If the user adds a ticker that isn't in `SEED_PRICES`, it starts at a random price between $50 and $300:

```python
self._prices[ticker] = SEED_PRICES.get(ticker, random.uniform(50.0, 300.0))
```

---

## Random Shock Events

Beyond normal GBM drift, the simulator injects occasional dramatic moves:

```python
event_probability = 0.001  # 0.1% chance per tick per ticker
shock_magnitude = random.uniform(0.02, 0.05)  # 2-5% move
shock_sign = random.choice([-1, 1])            # Up or down
```

With 10 tickers at 2 ticks/second:
- Expected events: `10 tickers × 2 ticks/s × 0.001 = 0.02 events/sec`
- One event every ~50 seconds on average
- Creates the visual "breaking news" moments that make the demo compelling

---

## `SimulatorDataSource` (async wrapper)

`GBMSimulator` is pure synchronous math. `SimulatorDataSource` wraps it in the `MarketDataSource` lifecycle.

```python
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
        
        # Seed cache immediately — SSE has prices before first tick
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
            # Seed cache immediately — ticker has a price before next tick
            price = self._sim.get_price(ticker)
            if price is not None:
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
```

### Key design decisions

**Why asyncio.sleep, not a timer?** The loop sleeps after each step. This means the effective tick rate is `step_duration + 0.5s`. Since `step()` takes ~0.1ms for 10 tickers (numpy is fast), this barely matters. If `step()` ever became slow, switch to a fixed-rate scheduler.

**Why seed the cache before the loop starts?** So that the SSE endpoint and any `/api/watchlist` calls that arrive before the first 500ms tick don't return empty data. The seed prices come from `SEED_PRICES` constants, not generated values.

**Why does `add_ticker` seed immediately?** Without the immediate seed, a ticker added mid-session would appear in the SSE stream with `null` prices for up to 500ms — a subtle UI flash.

---

## Tuning the Simulator

### Faster/slower ticks

```python
# 250ms ticks (faster, more CPU)
source = SimulatorDataSource(price_cache=cache, update_interval=0.25)

# 1s ticks (slower, demo on slow hardware)
source = SimulatorDataSource(price_cache=cache, update_interval=1.0)
```

### More dramatic events

```python
# 0.5% chance per tick → event every ~10s with 10 tickers
source = SimulatorDataSource(price_cache=cache, event_probability=0.005)
```

### Adding a new default ticker with custom params

In `seed_prices.py`:

```python
SEED_PRICES["PYPL"] = 65.00
TICKER_PARAMS["PYPL"] = {"sigma": 0.35, "mu": 0.03}
CORRELATION_GROUPS["finance"].add("PYPL")
```

No other changes needed — the simulator and correlation matrix pick it up automatically.

### Modifying volatility of existing tickers

```python
# Make NVDA even more volatile
TICKER_PARAMS["NVDA"] = {"sigma": 0.60, "mu": 0.10}
```

---

## Testing the Simulator

The test suite in `backend/tests/market/` covers the simulator at two levels:

### Unit tests (`test_simulator.py`) — 17 tests

- GBM output stays positive for all tickers
- Prices update each step (≠ previous value)
- Step returns all expected tickers
- Dynamic add/remove updates the ticker list and rebuilds Cholesky
- Event probability: mock `random.random` to force events, verify magnitude
- Correlation: multiple steps show expected correlation sign (more statistical than deterministic)
- `DEFAULT_DT` computed correctly against known constants

### Integration tests (`test_simulator_source.py`) — 10 tests

- `start()` populates the cache before returning
- Cache has data for all tickers after first loop iteration
- `add_ticker()` updates cache immediately
- `remove_ticker()` clears ticker from cache
- `stop()` cancels the background task cleanly
- Source can be stopped and garbage-collected without errors

### Running tests

```bash
cd backend
uv run --extra dev pytest tests/market/test_simulator.py -v
uv run --extra dev pytest tests/market/test_simulator_source.py -v
```

---

## Demo

A standalone Rich terminal dashboard shows the simulator in action:

```bash
cd backend
uv run market_data_demo.py
```

Displays all 10 tickers with live-updating prices, sparklines, color-coded direction arrows, and an event log for shock events. Runs 60 seconds or until Ctrl+C.

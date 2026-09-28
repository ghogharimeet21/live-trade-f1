"""
metrics.py — Zero-impact metrics collection

Design contract:
  - Trading threads ONLY call put_nowait() → never blocks, never slows strategy
  - MonitorServer drains this queue in its own asyncio loop
  - MetricsStore holds rolling state for dashboard snapshot requests
"""

import time
import threading
from dataclasses import dataclass, field
from collections import deque
from typing import Any
import queue


# ── Event types ──────────────────────────────────────────────────────────── #

@dataclass
class TickEvent:
    """Fired by StrategyEngine on every candle tick (live + closed)."""
    symbol: str
    price: float
    is_closed: bool
    candle_start_ms: int
    candle_queue_depth: int
    order_queue_depth: int
    strategy_eval_ms: float     # time strategy.on_candle() took
    candle_latency_ms: float    # time from put_nowait → strategy pick-up
    open: float = 0.0
    high: float = 0.0
    low: float = 0.0
    volume: float = 0.0
    fast_sma: float | None = None
    slow_sma: float | None = None
    rsi: float | None = None
    warmup_count: int = 0
    warmup_target: int = 50
    strategy_ready: bool = False
    ts: float = field(default_factory=time.time)


@dataclass
class SignalEvent:
    """Fired when strategy emits a buy/sell signal."""
    symbol: str
    side: str        # "BUY" | "SELL"
    price: float
    reason: str
    order_id: str
    candle_start_ms: int | None = None
    ts: float = field(default_factory=time.time)


@dataclass
class FillEvent:
    """Fired when OrderExecutor confirms a paper fill."""
    order_id: str
    symbol: str
    side: str
    quantity: float
    fill_price: float
    slippage: float
    signal_to_fill_ms: float    # latency from OrderRequest.created_at → fill
    realized_pnl: float         # 0 if opening, non-zero if closing
    total_realized_pnl: float
    trade_count: int
    candle_start_ms: int | None = None
    ts: float = field(default_factory=time.time)


@dataclass
class PositionEvent:
    """Fired whenever position state changes."""
    symbol: str
    side: str        # "LONG" | "SHORT" | "FLAT"
    entry_price: float
    quantity: float
    ts: float = field(default_factory=time.time)


# ── Metrics Store (thread-safe snapshot) ─────────────────────────────────── #

class MetricsStore:
    """
    Rolling in-memory store. Dashboard reads this for initial snapshot.
    All public read access is guarded by a single RLock.
    """

    MAX_TICKS   = 200   # price history points
    MAX_CANDLES = 500   # aggregated chart candles
    MAX_SIGNALS = 50    # signal log entries
    MAX_FILLS   = 50    # fill log entries
    MAX_LATENCY = 100   # latency history points

    def __init__(self):
        self._lock = threading.RLock()

        # Price history [{"ts": float, "price": float, "closed": bool, "open": float, "high": float, "low": float, "volume": float}]
        self.price_history: deque = deque(maxlen=self.MAX_TICKS)

        # Aggregated OHLCV + indicator state for the TradingView-style chart.
        # The monitor thread owns this state, so the trading threads never touch it.
        self.chart_candles: deque = deque(maxlen=self.MAX_CANDLES)
        self._chart_by_start: dict[int, dict] = {}

        # Latency history [{"ts": float, "eval_ms": float, "latency_ms": float}]
        self.latency_history: deque = deque(maxlen=self.MAX_LATENCY)

        # Signal log
        self.signals: deque = deque(maxlen=self.MAX_SIGNALS)

        # Fill log
        self.fills: deque = deque(maxlen=self.MAX_FILLS)

        # Live state
        self.latest_price: float = 0.0
        self.latest_open: float = 0.0
        self.latest_high: float = 0.0
        self.latest_low: float = 0.0
        self.latest_volume: float = 0.0
        self.symbol: str = ""
        self.position_side: str = "FLAT"
        self.position_entry: float = 0.0
        self.position_qty: float = 0.0
        self.realized_pnl: float = 0.0
        self.trade_count: int = 0
        self.tick_count: int = 0
        self.signal_count: int = 0
        self.candle_queue_depth: int = 0
        self.order_queue_depth: int = 0

        # Strategy indicators & warmup
        self.fast_sma: float | None = None
        self.slow_sma: float | None = None
        self.rsi: float | None = None
        self.warmup_count: int = 0
        self.warmup_target: int = 50
        self.strategy_ready: bool = False

        # Rolling latency averages
        self._eval_ms_sum: float = 0.0
        self._latency_ms_sum: float = 0.0
        self._latency_count: int = 0

    def seed_historical(self, candles: list[dict], *, fast_sma=None, slow_sma=None, rsi=None, warmup_count=0, warmup_target=50, strategy_ready=False):
        """Seed UI-only chart state before live threads start.

        This is intentionally called during startup, before the live pipeline
        begins. It never runs in the strategy or executor hot path.
        """
        with self._lock:
            self.chart_candles.clear()
            self._chart_by_start.clear()

            for raw in candles[-self.MAX_CANDLES:]:
                candle = {
                    "time": int(raw["candle_start_ms"]) // 1000,
                    "candle_start_ms": int(raw["candle_start_ms"]),
                    "open": float(raw["open"]),
                    "high": float(raw["high"]),
                    "low": float(raw["low"]),
                    "close": float(raw["close"]),
                    "volume": float(raw.get("volume", 0.0)),
                    "closed": True,
                    "fast_sma": raw.get("fast_sma"),
                    "slow_sma": raw.get("slow_sma"),
                    "rsi": raw.get("rsi"),
                }
                self.chart_candles.append(candle)
                self._chart_by_start[candle["candle_start_ms"]] = candle

            if candles:
                last = candles[-1]
                self.symbol = str(last.get("symbol", self.symbol))
                self.latest_price = float(last.get("close", 0.0))
                self.latest_open = float(last.get("open", 0.0))
                self.latest_high = float(last.get("high", 0.0))
                self.latest_low = float(last.get("low", 0.0))
                self.latest_volume = float(last.get("volume", 0.0))

            self.fast_sma = fast_sma
            self.slow_sma = slow_sma
            self.rsi = rsi
            self.warmup_count = warmup_count
            self.warmup_target = warmup_target
            self.strategy_ready = strategy_ready

    def apply_tick(self, e: TickEvent):
        with self._lock:
            self.symbol = e.symbol
            self.latest_price = e.price
            self.latest_open = e.open
            self.latest_high = e.high
            self.latest_low = e.low
            self.latest_volume = e.volume
            self.fast_sma = e.fast_sma
            self.slow_sma = e.slow_sma
            self.rsi = e.rsi
            self.warmup_count = e.warmup_count
            self.warmup_target = e.warmup_target
            self.strategy_ready = e.strategy_ready

            self.tick_count += 1
            self.candle_queue_depth = e.candle_queue_depth
            self.order_queue_depth = e.order_queue_depth
            self.price_history.append({
                "ts": e.ts,
                "price": e.price,
                "open": e.open,
                "high": e.high,
                "low": e.low,
                "volume": e.volume,
                "closed": e.is_closed,
            })

            # O(1) chart update: replace the current candle on every live tick,
            # or append exactly one new candle when the candle timestamp changes.
            candle = self._chart_by_start.get(e.candle_start_ms)
            if candle is None:
                if len(self.chart_candles) >= self.MAX_CANDLES:
                    oldest = self.chart_candles.popleft()
                    self._chart_by_start.pop(oldest["candle_start_ms"], None)

                candle = {
                    "time": e.candle_start_ms // 1000,
                    "candle_start_ms": e.candle_start_ms,
                    "open": e.open,
                    "high": e.high,
                    "low": e.low,
                    "close": e.price,
                    "volume": e.volume,
                    "closed": e.is_closed,
                    "fast_sma": e.fast_sma,
                    "slow_sma": e.slow_sma,
                    "rsi": e.rsi,
                }
                self.chart_candles.append(candle)
                self._chart_by_start[e.candle_start_ms] = candle
            else:
                candle["open"] = e.open
                candle["high"] = e.high
                candle["low"] = e.low
                candle["close"] = e.price
                candle["volume"] = e.volume
                candle["closed"] = e.is_closed
                candle["fast_sma"] = e.fast_sma
                candle["slow_sma"] = e.slow_sma
                candle["rsi"] = e.rsi

            self.latency_history.append({
                "ts": e.ts,
                "eval_ms": e.strategy_eval_ms,
                "latency_ms": e.candle_latency_ms,
            })
            self._eval_ms_sum += e.strategy_eval_ms
            self._latency_ms_sum += e.candle_latency_ms
            self._latency_count += 1

    def apply_signal(self, e: SignalEvent):
        with self._lock:
            self.signal_count += 1
            self.signals.appendleft({
                "ts": e.ts,
                "side": e.side,
                "price": e.price,
                "reason": e.reason,
                "order_id": e.order_id,
                "candle_start_ms": e.candle_start_ms,
            })

    def apply_fill(self, e: FillEvent):
        with self._lock:
            self.realized_pnl = e.total_realized_pnl
            self.trade_count = e.trade_count
            self.fills.appendleft({
                "ts": e.ts,
                "order_id": e.order_id,
                "side": e.side,
                "qty": e.quantity,
                "fill_price": e.fill_price,
                "slippage": e.slippage,
                "signal_to_fill_ms": e.signal_to_fill_ms,
                "realized_pnl": e.realized_pnl,
                "candle_start_ms": e.candle_start_ms,
            })

    def apply_position(self, e: PositionEvent):
        with self._lock:
            self.position_side = e.side
            self.position_entry = e.entry_price
            self.position_qty = e.quantity

    def snapshot(self) -> dict:
        """Full state dump for initial dashboard load."""
        with self._lock:
            n = max(self._latency_count, 1)
            eval_list = [h["eval_ms"] for h in self.latency_history] if self.latency_history else [0.0]
            lat_list = [h["latency_ms"] for h in self.latency_history] if self.latency_history else [0.0]

            return {
                "symbol":            self.symbol,
                "latest_price":      self.latest_price,
                "latest_open":       self.latest_open,
                "latest_high":       self.latest_high,
                "latest_low":        self.latest_low,
                "latest_volume":     self.latest_volume,
                "tick_count":        self.tick_count,
                "signal_count":      self.signal_count,
                "trade_count":       self.trade_count,
                "realized_pnl":      round(self.realized_pnl, 4),
                "position_side":     self.position_side,
                "position_entry":    self.position_entry,
                "position_qty":      self.position_qty,
                "candle_q_depth":    self.candle_queue_depth,
                "order_q_depth":     self.order_queue_depth,
                "avg_eval_ms":       round(self._eval_ms_sum / n, 3),
                "avg_latency_ms":    round(self._latency_ms_sum / n, 3),
                "min_eval_ms":       round(min(eval_list), 3) if eval_list else 0.0,
                "max_eval_ms":       round(max(eval_list), 3) if eval_list else 0.0,
                "fast_sma":          round(self.fast_sma, 2) if self.fast_sma is not None else None,
                "slow_sma":          round(self.slow_sma, 2) if self.slow_sma is not None else None,
                "rsi":               round(self.rsi, 2) if self.rsi is not None else None,
                "warmup_count":      self.warmup_count,
                "warmup_target":     self.warmup_target,
                "strategy_ready":    self.strategy_ready,
                "price_history":     list(self.price_history),
                "chart_candles":     list(self.chart_candles),
                "latency_history":   list(self.latency_history),
                "signals":           list(self.signals),
                "fills":             list(self.fills),
            }

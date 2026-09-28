from dataclasses import dataclass, field
from typing import List
from enums import CandleColour, OrderSide, OrderType, OrderStatus, PositionSide
from utils import seconds_to_hms
import time
import uuid


@dataclass(frozen=True)
class Quote:
    symbol: str
    date: int
    time: int
    open: float
    high: float
    low: float
    close: float
    volume: float
    is_closed: bool
    # Unix milliseconds for the start of the candle.
    # Used to distinguish repeated live updates from different candles.
    candle_start_ms: int = 0
    # Monotonic timestamp captured immediately before the quote enters
    # candle_queue. This lets the engine measure real queue latency.
    enqueued_at: float = field(default_factory=time.perf_counter, repr=False, compare=False)

    @property
    def candle_colour(self) -> CandleColour | None:
        if not self.is_closed:
            return None

        if self.close > self.open:
            return CandleColour.GREEN

        if self.close < self.open:
            return CandleColour.RED

        return CandleColour.NEUTRAL

    def __str__(self):
        return (
            f"symbol={self.symbol}, "
            f"date={self.date}, "
            f"time={seconds_to_hms(self.time)}, "
            f"O={self.open} H={self.high} L={self.low} C={self.close}, "
            f"vol={self.volume:.4f}, "
            f"closed={self.is_closed}"
        )


@dataclass
class OrderRequest:
    """
    Represents a signal from the strategy that should be executed.
    Created by StrategyEngine, consumed by OrderExecutor.
    """
    symbol: str
    side: OrderSide
    order_type: OrderType
    quantity: float
    price: float
    limit_price: float | None = None
    signal_reason: str = ""
    order_id: str = field(default_factory=lambda: str(uuid.uuid4())[:8])
    created_at: float = field(default_factory=time.time)
    # Candle that caused the signal. Useful for deduplication/auditing.
    signal_candle_start_ms: int | None = None

    def __str__(self):
        return (
            f"[{self.order_id}] {self.side.value} {self.order_type.value} "
            f"{self.quantity} {self.symbol} @ {self.price:.4f} | {self.signal_reason}"
        )


@dataclass
class Fill:
    """
    Represents a confirmed paper/live order fill.
    Created by OrderExecutor.
    """
    order_id: str
    symbol: str
    side: OrderSide
    quantity: float
    fill_price: float
    status: OrderStatus
    filled_at: float = field(default_factory=time.time)
    slippage: float = 0.0

    def __str__(self):
        return (
            f"[FILL {self.order_id}] {self.side.value} {self.quantity} {self.symbol} "
            f"@ {self.fill_price:.4f} | slippage={self.slippage:.4f}"
        )


@dataclass
class Position:
    """
    Tracks an open position for one symbol.
    """
    symbol: str
    side: PositionSide
    entry_price: float
    quantity: float
    opened_at: float = field(default_factory=time.time)

    @property
    def is_open(self) -> bool:
        return self.side != PositionSide.FLAT

    def unrealized_pnl(self, current_price: float) -> float:
        if not self.is_open:
            return 0.0
        if self.side == PositionSide.LONG:
            return (current_price - self.entry_price) * self.quantity
        if self.side == PositionSide.SHORT:
            return (self.entry_price - current_price) * self.quantity
        return 0.0

    def __str__(self):
        return (
            f"{self.symbol} {self.side.value} "
            f"qty={self.quantity} entry={self.entry_price:.4f}"
        )


@dataclass
class Filter:
    filterType: str
    minPrice: str | None = None
    maxPrice: str | None = None
    tickSize: str | None = None
    minQty: str | None = None
    maxQty: str | None = None
    stepSize: str | None = None


@dataclass
class Instrument:
    expiryDate: int
    filters: List[Filter]
    symbol: str
    side: str
    strikePrice: str
    underlying: str
    unit: int
    liquidationFeeRate: str
    minQty: str
    maxQty: str
    initialMargin: str
    maintenanceMargin: str
    minInitialMargin: str
    minMaintenanceMargin: str
    priceScale: int
    quantityScale: int
    quoteAsset: str
    status: str
    contractType: str
    underlyingType: str

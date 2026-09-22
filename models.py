from dataclasses import dataclass
from typing import List
from enums import CandleColour
from utils import seconds_to_hms


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

    @property
    def candle_colour(self) -> CandleColour | None:
        if not self.is_closed:
            return

        if self.close > self.open:
            return CandleColour.GREEN

        if self.close < self.open:
            return CandleColour.RED

        return CandleColour.NEUTRAL

    def __str__(self):
        return f"symbol={self.symbol}, date={self.date}, time={seconds_to_hms(self.time)}, open={self.open}, high={self.high}, low={self.low}, close={self.close}, volume={self.volume}"


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

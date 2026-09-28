import logging
import threading
from models import Quote, OrderRequest
from enums import OrderSide, OrderType, PositionSide
from indicators import SMA, RSI
from collections import deque


logger = logging.getLogger(__name__)


class Strategy:
    """
    Pure signal logic — no threading, no I/O, no side effects.

    Rules:
      - BUY  when fast SMA crosses ABOVE slow SMA and RSI < 60
      - SELL when fast SMA crosses BELOW slow SMA and RSI > 40

    Returns an OrderRequest when a signal fires, otherwise None.
    Strategy only acts on CLOSED candles to avoid repainting.
    """

    def __init__(
        self,
        sma_periods: list[int],
        rsi_period: int,
        trade_qty: float = 0.001,
    ):
        if len(sma_periods) != 2:
            raise ValueError("Exactly two SMA periods required.")

        self.sma_periods = sorted(sma_periods)
        self.rsi_period = rsi_period
        self.trade_qty = trade_qty

        self.fast_sma = SMA(self.sma_periods[0])
        self.slow_sma = SMA(self.sma_periods[1])
        self.rsi = RSI(self.rsi_period)

        # Keep a rolling history of values for crossover detection and other logic
        self.fast_history = deque(maxlen=10)
        self.slow_history = deque(maxlen=10)
        self.rsi_history = deque(maxlen=10)

    @property
    def warmup_count(self) -> int:
        return len(self.slow_sma.values)

    @property
    def warmup_target(self) -> int:
        return self.sma_periods[1]

    @property
    def is_ready(self) -> bool:
        return (
            len(self.fast_history) > 0 
            and len(self.slow_history) > 0 
            and len(self.rsi_history) > 0
        )

    @property
    def latest_fast(self) -> float | None:
        return self.fast_history[-1] if self.fast_history else None

    @property
    def latest_slow(self) -> float | None:
        return self.slow_history[-1] if self.slow_history else None

    @property
    def latest_rsi(self) -> float | None:
        return self.rsi_history[-1] if self.rsi_history else None


    def on_candle(self, quote: Quote) -> OrderRequest | None:
        """
        Called by StrategyEngine on every candle tick.
        Only generates signals on CLOSED candles.
        Returns an OrderRequest or None.
        """
        fast = self.fast_sma.update(quote.close)
        slow = self.slow_sma.update(quote.close)
        rsi  = self.rsi.update(quote.close)

        # Not enough data yet
        if fast is None or slow is None or rsi is None:
            return None

        # Only act on confirmed, closed candles
        if not quote.is_closed:
            return None
            
        # Candle is closed, commit to history
        self.fast_history.append(fast)
        self.slow_history.append(slow)
        self.rsi_history.append(rsi)

        signal = None

        # --- Crossover detection ---
        if len(self.fast_history) >= 2 and len(self.slow_history) >= 2:
            prev_fast = self.fast_history[-2]
            prev_slow = self.slow_history[-2]

            was_below = prev_fast <= prev_slow
            is_above  = fast > slow

            was_above = prev_fast >= prev_slow
            is_below  = fast < slow

            # Bullish cross + RSI not overbought
            if was_below and is_above and rsi < 60:
                signal = OrderRequest(
                    symbol=quote.symbol,
                    side=OrderSide.BUY,
                    order_type=OrderType.MARKET,
                    quantity=self.trade_qty,
                    price=quote.close,
                    signal_reason=f"SMA_CROSS_UP fast={fast:.2f} slow={slow:.2f} rsi={rsi:.1f}",
                )
                logger.info("▲ BUY signal: %s", signal.signal_reason)

            # Bearish cross + RSI not oversold
            elif was_above and is_below and rsi > 40:
                signal = OrderRequest(
                    symbol=quote.symbol,
                    side=OrderSide.SELL,
                    order_type=OrderType.MARKET,
                    quantity=self.trade_qty,
                    price=quote.close,
                    signal_reason=f"SMA_CROSS_DOWN fast={fast:.2f} slow={slow:.2f} rsi={rsi:.1f}",
                )
                logger.info("▼ SELL signal: %s", signal.signal_reason)

        return signal

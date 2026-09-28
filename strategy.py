import logging
from collections import deque

from models import Quote, OrderRequest
from enums import OrderSide, OrderType
from indicators import SMA, RSI


logger = logging.getLogger(__name__)


class Strategy:
    """
    Tick-reactive strategy with candle-safe indicator state.

    IMPORTANT DESIGN:
      - Closed candles update the real/committed SMA + RSI state exactly once.
      - Live candle updates use indicator.preview(), which NEVER mutates that
        committed state.
      - Signals are evaluated intrabar, so the strategy can react immediately.
      - Crossover detection uses the previous evaluation relation, preventing
        the same crossover from firing repeatedly on every trade tick.
      - Strategy state is owned by ONE StrategyEngine thread only.
    """

    def __init__(
        self,
        sma_periods: list[int],
        rsi_period: int,
        trade_qty: float = 0.001,
    ):
        if len(sma_periods) != 2:
            raise ValueError("Exactly two SMA periods required.")
        if trade_qty <= 0:
            raise ValueError("trade_qty must be positive.")

        fast, slow = sorted(sma_periods)
        self.sma_periods = [fast, slow]
        self.rsi_period = rsi_period
        self.trade_qty = trade_qty

        # These are COMMITTED candle indicators.
        self.fast_sma = SMA(fast)
        self.slow_sma = SMA(slow)
        self.rsi = RSI(rsi_period)

        # Historical indicator snapshots for crossover context/debugging.
        self.fast_history = deque(maxlen=10)
        self.slow_history = deque(maxlen=10)
        self.rsi_history = deque(maxlen=10)

        # Current values shown to the dashboard. These may be intrabar previews.
        self._latest_fast: float | None = None
        self._latest_slow: float | None = None
        self._latest_rsi: float | None = None

        # Number of COMPLETED candles committed to indicator state.
        self._closed_candle_count = 0

        # Last confirmed relationship from a closed candle:
        # -1 = fast < slow, 0 = equal, +1 = fast > slow.
        self._last_closed_relation: int | None = None

        # Intrabar relationship for the currently forming candle.
        self._live_candle_start_ms: int | None = None
        self._live_relation: int | None = None

        # Last candle we committed, protects against duplicate closed events.
        self._last_committed_candle_start_ms: int | None = None

    @property
    def warmup_count(self) -> int:
        return self._closed_candle_count

    @property
    def warmup_target(self) -> int:
        # Slow SMA needs `slow` closes. Wilder RSI needs `rsi_period + 1` closes.
        return max(self.sma_periods[1], self.rsi_period + 1)

    @property
    def is_ready(self) -> bool:
        return (
            self._closed_candle_count >= self.warmup_target
            and len(self.fast_sma.values) >= self.sma_periods[0]
            and len(self.slow_sma.values) >= self.sma_periods[1]
            and self.rsi.avg_gain is not None
            and self.rsi.avg_loss is not None
        )

    @property
    def latest_fast(self) -> float | None:
        return self._latest_fast

    @property
    def latest_slow(self) -> float | None:
        return self._latest_slow

    @property
    def latest_rsi(self) -> float | None:
        return self._latest_rsi

    @staticmethod
    def _relation(fast: float, slow: float) -> int:
        if fast > slow:
            return 1
        if fast < slow:
            return -1
        return 0

    def _cross_signal(
        self,
        quote: Quote,
        fast: float,
        slow: float,
        rsi: float,
        previous_relation: int | None,
        current_relation: int,
    ) -> OrderRequest | None:
        if previous_relation is None:
            return None

        # Equal is treated as neutral/boundary. A transition from <= to >
        # is bullish; >= to < is bearish.
        bullish_cross = previous_relation <= 0 and current_relation > 0
        bearish_cross = previous_relation >= 0 and current_relation < 0

        if bullish_cross and rsi < 60:
            signal = OrderRequest(
                symbol=quote.symbol,
                side=OrderSide.BUY,
                order_type=OrderType.MARKET,
                quantity=self.trade_qty,
                price=quote.close,
                signal_reason=(
                    f"SMA_CROSS_UP fast={fast:.2f} "
                    f"slow={slow:.2f} rsi={rsi:.1f} "
                    f"intrabar={not quote.is_closed}"
                ),
                signal_candle_start_ms=quote.candle_start_ms,
            )
            logger.info("▲ BUY signal: %s", signal.signal_reason)
            return signal

        if bearish_cross and rsi > 40:
            signal = OrderRequest(
                symbol=quote.symbol,
                side=OrderSide.SELL,
                order_type=OrderType.MARKET,
                quantity=self.trade_qty,
                price=quote.close,
                signal_reason=(
                    f"SMA_CROSS_DOWN fast={fast:.2f} "
                    f"slow={slow:.2f} rsi={rsi:.1f} "
                    f"intrabar={not quote.is_closed}"
                ),
                signal_candle_start_ms=quote.candle_start_ms,
            )
            logger.info("▼ SELL signal: %s", signal.signal_reason)
            return signal

        return None

    def _commit_closed_candle(self, quote: Quote) -> tuple[float | None, float | None, float | None]:
        """Commit one candle into the real indicator state, exactly once."""
        if quote.candle_start_ms == self._last_committed_candle_start_ms:
            return (
                self._latest_fast,
                self._latest_slow,
                self._latest_rsi,
            )

        fast = self.fast_sma.update(quote.close)
        slow = self.slow_sma.update(quote.close)
        rsi = self.rsi.update(quote.close)

        self._last_committed_candle_start_ms = quote.candle_start_ms
        self._closed_candle_count += 1

        if fast is not None:
            self.fast_history.append(fast)
        if slow is not None:
            self.slow_history.append(slow)
        if rsi is not None:
            self.rsi_history.append(rsi)

        return fast, slow, rsi

    def on_candle(self, quote: Quote) -> OrderRequest | None:
        """
        Process every live candle update and every closed candle event.

        Live update:
            committed history + current close -> preview indicators -> signal

        Closed update:
            current close is committed exactly once.
            If the candle was already evaluated live, no duplicate crossover
            is emitted at close.
        """
        if quote.is_closed:
            fast, slow, rsi = self._commit_closed_candle(quote)

            if fast is None or slow is None or rsi is None:
                self._latest_fast = fast
                self._latest_slow = slow
                self._latest_rsi = rsi
                self._live_candle_start_ms = None
                self._live_relation = None
                return None

            self._latest_fast = fast
            self._latest_slow = slow
            self._latest_rsi = rsi

            relation = self._relation(fast, slow)

            # If we already evaluated this candle intrabar, the crossover was
            # already handled. Otherwise (e.g. a queue drop) evaluate it now.
            already_evaluated = (
                self._live_candle_start_ms == quote.candle_start_ms
            )

            signal = None
            if self.is_ready and not already_evaluated:
                signal = self._cross_signal(
                    quote,
                    fast,
                    slow,
                    rsi,
                    self._last_closed_relation,
                    relation,
                )

            self._last_closed_relation = relation
            self._live_candle_start_ms = None
            self._live_relation = relation
            return signal

        # ── LIVE / INTRABAR PATH ────────────────────────────────────────── #
        fast = self.fast_sma.preview(quote.close)
        slow = self.slow_sma.preview(quote.close)
        rsi = self.rsi.preview(quote.close)

        self._latest_fast = fast
        self._latest_slow = slow
        self._latest_rsi = rsi

        if fast is None or slow is None or rsi is None:
            return None

        relation = self._relation(fast, slow)

        if self._live_candle_start_ms != quote.candle_start_ms:
            # First live update of a new candle: compare against the last
            # CONFIRMED candle relationship, not against another tick in the
            # current candle.
            previous_relation = self._last_closed_relation
            self._live_candle_start_ms = quote.candle_start_ms
        else:
            previous_relation = self._live_relation

        signal = None
        if self.is_ready:
            signal = self._cross_signal(
                quote,
                fast,
                slow,
                rsi,
                previous_relation,
                relation,
            )

        # Always update relation, even if RSI blocks the trade. This means the
        # strategy only fires on a NEW crossover rather than every subsequent
        # tick while the condition remains true.
        self._live_relation = relation
        return signal

    def warmup(self, quotes: list[Quote]) -> list[dict]:
        """
        Feed completed historical candles into committed indicator state and
        return UI-only indicator snapshots for chart seeding.

        This is startup work, before the live StrategyEngine thread starts.
        """
        snapshots: list[dict] = []

        for quote in quotes:
            if not quote.is_closed:
                raise ValueError("warmup() requires closed quotes only")

            self.on_candle(quote)
            snapshots.append({
                "symbol": quote.symbol,
                "candle_start_ms": quote.candle_start_ms,
                "open": quote.open,
                "high": quote.high,
                "low": quote.low,
                "close": quote.close,
                "volume": quote.volume,
                "fast_sma": self.latest_fast,
                "slow_sma": self.latest_slow,
                "rsi": self.latest_rsi,
            })

        # Warmup should establish the last confirmed relation, but should not
        # create a live candle context.
        if self.latest_fast is not None and self.latest_slow is not None:
            self._last_closed_relation = self._relation(
                self.latest_fast,
                self.latest_slow,
            )
        self._live_candle_start_ms = None
        self._live_relation = self._last_closed_relation
        return snapshots

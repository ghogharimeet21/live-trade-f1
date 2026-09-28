"""
engine.py — StrategyEngine

Thread 2 of the trading pipeline:

    [WebSocket/Feed] -> candle_queue -> [StrategyEngine]
                                     -> order_queue -> [OrderExecutor]

The StrategyEngine owns the Strategy object. No other thread touches strategy
state. Live candle updates are processed immediately, while the strategy itself
keeps indicator state safe by using non-mutating previews for intrabar values.
"""

import logging
import queue
import threading
import time

from models import Quote, OrderRequest
from strategy import Strategy
from metrics import TickEvent, SignalEvent


logger = logging.getLogger(__name__)


class StrategyEngine(threading.Thread):
    def __init__(
        self,
        strategy: Strategy,
        candle_queue: queue.Queue,
        order_queue: queue.Queue,
        metrics_queue: queue.Queue | None = None,
    ):
        super().__init__(daemon=True, name="StrategyEngine")
        self.strategy = strategy
        self.candle_queue = candle_queue
        self.order_queue = order_queue
        self.metrics_queue = metrics_queue
        self._stop_event = threading.Event()

    @property
    def is_running(self) -> bool:
        return not self._stop_event.is_set()

    def start(self):
        self._stop_event.clear()
        super().start()
        logger.info("StrategyEngine started.")

    def run(self):
        while not self._stop_event.is_set():
            try:
                quote: Quote = self.candle_queue.get(timeout=0.25)
            except queue.Empty:
                continue

            try:
                queue_latency_ms = max(
                    0.0,
                    (time.perf_counter() - quote.enqueued_at) * 1000.0,
                )

                t0 = time.perf_counter()
                try:
                    signal: OrderRequest | None = self.strategy.on_candle(quote)
                except Exception:
                    logger.exception("Strategy raised an exception on quote: %s", quote)
                    signal = None
                eval_ms = (time.perf_counter() - t0) * 1000.0

                candle_latency_ms = queue_latency_ms + eval_ms

                if self.metrics_queue is not None:
                    try:
                        self.metrics_queue.put_nowait(
                            TickEvent(
                                symbol=quote.symbol,
                                price=quote.close,
                                candle_start_ms=quote.candle_start_ms,
                                open=quote.open,
                                high=quote.high,
                                low=quote.low,
                                volume=quote.volume,
                                is_closed=quote.is_closed,
                                candle_queue_depth=self.candle_queue.qsize(),
                                order_queue_depth=self.order_queue.qsize(),
                                strategy_eval_ms=round(eval_ms, 4),
                                candle_latency_ms=round(candle_latency_ms, 4),
                                fast_sma=self.strategy.latest_fast,
                                slow_sma=self.strategy.latest_slow,
                                rsi=self.strategy.latest_rsi,
                                warmup_count=self.strategy.warmup_count,
                                warmup_target=self.strategy.warmup_target,
                                strategy_ready=self.strategy.is_ready,
                            )
                        )
                    except queue.Full:
                        pass

                if signal is not None:
                    try:
                        # app.py uses an unbounded order queue. Orders should
                        # not be silently dropped just because monitoring or a
                        # temporary burst filled a small buffer.
                        self.order_queue.put_nowait(signal)
                    except queue.Full:
                        # Kept for custom bounded queue implementations.
                        logger.critical("ORDER QUEUE FULL — signal NOT forwarded: %s", signal)
                        continue

                    if self.metrics_queue is not None:
                        try:
                            self.metrics_queue.put_nowait(
                                SignalEvent(
                                    symbol=signal.symbol,
                                    side=signal.side.value,
                                    price=signal.price,
                                    reason=signal.signal_reason,
                                    order_id=signal.order_id,
                                    candle_start_ms=signal.signal_candle_start_ms,
                                )
                            )
                        except queue.Full:
                            pass

            finally:
                self.candle_queue.task_done()

        logger.info("StrategyEngine stopped.")

    def stop(self):
        self._stop_event.set()
        if self.is_alive() and threading.current_thread() is not self:
            self.join(timeout=5)

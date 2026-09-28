"""
engine.py — StrategyEngine

Thread 2 of 3 in the pipeline:

    [WS Thread] → candle_queue → [StrategyEngine] → order_queue → [OrderExecutor]

Responsibilities:
  - Drain candle_queue as fast as possible
  - Pass each Quote to the strategy
  - If strategy returns a signal, put it on order_queue (non-blocking)
  - Emit timing metrics to metrics_queue (non-blocking, zero impact)
  - Never do any I/O or sleeping inside the strategy call path
"""

import queue
import logging
import threading
import time

from models import Quote, OrderRequest
from strategy import Strategy
from metrics import TickEvent, SignalEvent


logger = logging.getLogger(__name__)


class StrategyEngine(threading.Thread):
    """
    Consumes Quote objects from `candle_queue`.
    Runs the strategy and puts OrderRequest signals onto `order_queue`.
    Emits TickEvent and SignalEvent to `metrics_queue` (put_nowait — never blocks).

    Design decisions:
      - daemon=True so it dies cleanly when main thread exits
      - queue.get(timeout=1) lets us check _running flag periodically
      - strategy is NOT thread-safe by default — only ONE StrategyEngine
        should own a given Strategy instance
      - metrics_queue is optional — if None, metrics are silently skipped
    """

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
        self._running = False

    def start(self):
        self._running = True
        super().start()
        logger.info("StrategyEngine started.")

    def run(self):
        while self._running:
            # Record when we picked the candle off the queue
            enqueue_ts = None

            try:
                quote: Quote = self.candle_queue.get(timeout=1)
                dequeue_ts = time.perf_counter()
            except queue.Empty:
                continue

            # Measure strategy evaluation time
            t0 = time.perf_counter()
            try:
                signal: OrderRequest | None = self.strategy.on_candle(quote)
            except Exception:
                logger.exception("Strategy raised an exception on candle: %s", quote)
                self.candle_queue.task_done()
                continue
            eval_ms = (time.perf_counter() - t0) * 1000

            # Approximate candle latency: from WS put to our dequeue
            # We measure it as the time it spent in the queue (imperfect but useful)
            candle_latency_ms = (time.perf_counter() - dequeue_ts) * 1000 + eval_ms

            self.candle_queue.task_done()

            # ── Emit tick metrics (non-blocking) ──────────────────────────── #
            if self.metrics_queue is not None:
                try:
                    self.metrics_queue.put_nowait(TickEvent(
                        symbol=quote.symbol,
                        price=quote.close,
                        open=quote.open,
                        high=quote.high,
                        low=quote.low,
                        volume=quote.volume,
                        is_closed=quote.is_closed,
                        candle_queue_depth=self.candle_queue.qsize(),
                        order_queue_depth=self.order_queue.qsize(),
                        strategy_eval_ms=round(eval_ms, 4),
                        candle_latency_ms=round(candle_latency_ms, 4),
                        fast_sma=getattr(self.strategy, "latest_fast", None),
                        slow_sma=getattr(self.strategy, "latest_slow", None),
                        rsi=getattr(self.strategy, "latest_rsi", None),
                        warmup_count=getattr(self.strategy, "warmup_count", 0),
                        warmup_target=getattr(self.strategy, "warmup_target", 50),
                        strategy_ready=getattr(self.strategy, "is_ready", False),
                    ))
                except queue.Full:
                    pass  # monitor is lagging — silently drop, never block

            if signal is not None:
                # ── Forward signal to executor ─────────────────────────────── #
                try:
                    self.order_queue.put_nowait(signal)
                except queue.Full:
                    logger.warning("order_queue is full — signal dropped: %s", signal)

                # ── Emit signal metrics (non-blocking) ────────────────────── #
                if self.metrics_queue is not None:
                    try:
                        self.metrics_queue.put_nowait(SignalEvent(
                            symbol=signal.symbol,
                            side=signal.side.value,
                            price=signal.price,
                            reason=signal.signal_reason,
                            order_id=signal.order_id,
                        ))
                    except queue.Full:
                        pass

        logger.info("StrategyEngine stopped.")

    def stop(self):
        self._running = False
        self.join(timeout=5)

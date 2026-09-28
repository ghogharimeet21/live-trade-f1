"""
executor.py — OrderExecutor

Thread 3 of 3 in the pipeline:

    [WS Thread] → candle_queue → [StrategyEngine] → order_queue → [OrderExecutor]

Responsibilities:
  - Drain order_queue
  - Simulate paper fills (instant fill at signal price ± slippage)
  - Update PositionManager
  - Emit FillEvent + PositionEvent to metrics_queue (non-blocking)
  - In future: swap _execute_paper() for _execute_live() with broker API call

Design:
  - Single worker thread → orders are processed serially (correct for paper)
  - For live multi-symbol, use one executor per symbol or ThreadPoolExecutor
"""

import queue
import logging
import threading
import time
import random

from models import OrderRequest, Fill
from enums import OrderStatus
from position import PositionManager
from metrics import FillEvent, PositionEvent


logger = logging.getLogger(__name__)


class OrderExecutor(threading.Thread):
    """
    Consumes OrderRequest from `order_queue`.
    Simulates paper fills and updates the PositionManager.
    Emits FillEvent and PositionEvent to `metrics_queue` (non-blocking).

    Paper trading slippage:
      By default adds a tiny random slippage (±0–0.01%) to simulate
      realistic fill conditions. Set slippage_bps=0 to disable.
    """

    def __init__(
        self,
        order_queue: queue.Queue,
        position_manager: PositionManager,
        slippage_bps: float = 1.0,
        metrics_queue: queue.Queue | None = None,
    ):
        super().__init__(daemon=True, name="OrderExecutor")
        self.order_queue = order_queue
        self.position_manager = position_manager
        self.slippage_bps = slippage_bps
        self.metrics_queue = metrics_queue
        self._running = False

    def start(self):
        self._running = True
        super().start()
        logger.info("OrderExecutor started (paper mode).")

    def run(self):
        while self._running:
            try:
                order: OrderRequest = self.order_queue.get(timeout=1)
            except queue.Empty:
                continue

            try:
                self._execute_paper(order)
            except Exception:
                logger.exception("Executor error on order: %s", order)
            finally:
                self.order_queue.task_done()

        logger.info("OrderExecutor stopped.")

    def _execute_paper(self, order: OrderRequest):
        """
        Simulate an instant paper fill.
        Replace this method with broker API call for live trading.
        """
        # Simulate tiny slippage
        slippage_factor = random.uniform(0, self.slippage_bps) / 10_000
        fill_price = order.price * (1 + slippage_factor)
        slippage   = fill_price - order.price

        fill = Fill(
            order_id=order.order_id,
            symbol=order.symbol,
            side=order.side,
            quantity=order.quantity,
            fill_price=fill_price,
            status=OrderStatus.FILLED,
            slippage=slippage,
        )

        # Measure signal-to-fill latency
        signal_to_fill_ms = (time.time() - order.created_at) * 1000

        logger.info("💰 %s", fill)

        realized = self.position_manager.on_fill(fill)

        if realized != 0.0:
            logger.info("📊 Summary: %s", self.position_manager.summary())

        # ── Emit metrics (non-blocking) ────────────────────────────────── #
        if self.metrics_queue is not None:
            try:
                self.metrics_queue.put_nowait(FillEvent(
                    order_id=order.order_id,
                    symbol=order.symbol,
                    side=order.side.value,
                    quantity=order.quantity,
                    fill_price=fill_price,
                    slippage=slippage,
                    signal_to_fill_ms=round(signal_to_fill_ms, 3),
                    realized_pnl=realized,
                    total_realized_pnl=self.position_manager.realized_pnl,
                    trade_count=self.position_manager.trade_count,
                ))
            except queue.Full:
                pass

            # Also emit position update
            pos = self.position_manager.position
            try:
                self.metrics_queue.put_nowait(PositionEvent(
                    symbol=order.symbol,
                    side=pos.side.value if pos else "FLAT",
                    entry_price=pos.entry_price if pos else 0.0,
                    quantity=pos.quantity if pos else 0.0,
                ))
            except queue.Full:
                pass

    # ------------------------------------------------------------------ #
    # Future: swap this in for live trading                               #
    # ------------------------------------------------------------------ #
    # def _execute_live(self, order: OrderRequest):
    #     response = broker_client.place_order(
    #         symbol=order.symbol,
    #         side=order.side.value,
    #         type=order.order_type.value,
    #         quantity=order.quantity,
    #     )
    #     fill = Fill(...)
    #     self.position_manager.on_fill(fill)

    def stop(self):
        self._running = False
        self.join(timeout=5)

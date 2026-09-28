"""
executor.py — OrderExecutor

Thread 3 of the trading pipeline.

One executor thread owns PositionManager, so fills are applied serially and
there is no concurrent mutation of position state.
"""

import logging
import queue
import random
import threading
import time

from models import OrderRequest, Fill
from enums import OrderStatus, OrderSide
from position import PositionManager
from metrics import FillEvent, PositionEvent


logger = logging.getLogger(__name__)


class OrderExecutor(threading.Thread):
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
        self.slippage_bps = max(0.0, slippage_bps)
        self.metrics_queue = metrics_queue
        self._stop_event = threading.Event()

    def start(self):
        self._stop_event.clear()
        super().start()
        logger.info("OrderExecutor started (paper mode).")

    def run(self):
        while not self._stop_event.is_set():
            try:
                order: OrderRequest = self.order_queue.get(timeout=0.25)
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
        # Adverse slippage:
        # BUY  -> pay higher
        # SELL -> receive lower
        slippage_factor = random.uniform(0.0, self.slippage_bps) / 10_000.0
        if order.side == OrderSide.BUY:
            fill_price = order.price * (1.0 + slippage_factor)
        else:
            fill_price = order.price * (1.0 - slippage_factor)

        slippage = fill_price - order.price

        fill = Fill(
            order_id=order.order_id,
            symbol=order.symbol,
            side=order.side,
            quantity=order.quantity,
            fill_price=fill_price,
            status=OrderStatus.FILLED,
            slippage=slippage,
        )

        signal_to_fill_ms = (time.time() - order.created_at) * 1000.0

        logger.info("💰 %s", fill)

        realized = self.position_manager.on_fill(fill)

        if realized != 0.0:
            logger.info("📊 Summary: %s", self.position_manager.summary())

        if self.metrics_queue is not None:
            try:
                self.metrics_queue.put_nowait(
                    FillEvent(
                        order_id=order.order_id,
                        symbol=order.symbol,
                        side=order.side.value,
                        quantity=order.quantity,
                        fill_price=fill_price,
                        candle_start_ms=order.signal_candle_start_ms,
                        slippage=slippage,
                        signal_to_fill_ms=round(signal_to_fill_ms, 3),
                        realized_pnl=realized,
                        total_realized_pnl=self.position_manager.realized_pnl,
                        trade_count=self.position_manager.trade_count,
                    )
                )
            except queue.Full:
                pass

            pos = self.position_manager.position
            try:
                self.metrics_queue.put_nowait(
                    PositionEvent(
                        symbol=order.symbol,
                        side=pos.side.value if pos else "FLAT",
                        entry_price=pos.entry_price if pos else 0.0,
                        quantity=pos.quantity if pos else 0.0,
                    )
                )
            except queue.Full:
                pass

    def stop(self):
        self._stop_event.set()
        if self.is_alive() and threading.current_thread() is not self:
            self.join(timeout=5)

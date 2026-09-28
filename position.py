"""
position.py — PositionManager

Tracks the current position and realized PnL for one symbol.
Called exclusively from OrderExecutor (single thread) — no locking needed.

Paper trading model:
  - BUY  → opens LONG  (or closes SHORT)
  - SELL → opens SHORT (or closes LONG)
  - Simple flat/long/short model, no partial fills
"""

import logging
import time
from models import Position, Fill
from enums import OrderSide, PositionSide


logger = logging.getLogger(__name__)


class PositionManager:
    """
    Manages position state and PnL tracking for a single symbol.

    Intentionally simple for paper trading:
      - One position at a time (no pyramiding)
      - Market orders only (instant fill at signal price)
    """

    def __init__(self, symbol: str):
        self.symbol = symbol
        self.position: Position | None = None
        self.realized_pnl: float = 0.0
        self.trade_count: int = 0

    @property
    def is_flat(self) -> bool:
        return self.position is None or not self.position.is_open

    def on_fill(self, fill: Fill) -> float:
        """
        Update position state from a fill.
        Returns realized PnL for this fill (0 if opening, non-zero if closing).
        """
        realized = 0.0

        if self.is_flat:
            # Open a new position
            side = (
                PositionSide.LONG
                if fill.side == OrderSide.BUY
                else PositionSide.SHORT
            )
            self.position = Position(
                symbol=self.symbol,
                side=side,
                entry_price=fill.fill_price,
                quantity=fill.quantity,
            )
            logger.info(
                "📂 Position opened: %s",
                self.position,
            )

        else:
            # Close existing position
            pos = self.position

            if pos.side == PositionSide.LONG and fill.side == OrderSide.SELL:
                realized = (fill.fill_price - pos.entry_price) * pos.quantity
                self._close_position(realized)

            elif pos.side == PositionSide.SHORT and fill.side == OrderSide.BUY:
                realized = (pos.entry_price - fill.fill_price) * pos.quantity
                self._close_position(realized)

            else:
                # Same direction — not handled in simple model, log and skip
                logger.warning(
                    "Ignored fill: already %s, got %s signal. "
                    "Close existing first.",
                    pos.side.value,
                    fill.side.value,
                )

        return realized

    def _close_position(self, realized: float):
        self.realized_pnl += realized
        self.trade_count += 1
        direction = "✅ PROFIT" if realized >= 0 else "❌ LOSS"
        logger.info(
            "%s | Realized PnL: %.4f USDT | Total PnL: %.4f USDT | Trades: %d",
            direction,
            realized,
            self.realized_pnl,
            self.trade_count,
        )
        self.position = None

    def mark_to_market(self, current_price: float):
        """Log unrealized PnL at current price."""
        if self.is_flat:
            return
        upnl = self.position.unrealized_pnl(current_price)
        logger.debug(
            "Unrealized PnL: %.4f USDT | Position: %s",
            upnl,
            self.position,
        )

    def summary(self) -> str:
        return (
            f"Symbol={self.symbol} | "
            f"Position={'FLAT' if self.is_flat else self.position} | "
            f"Realized PnL={self.realized_pnl:.4f} | "
            f"Trades={self.trade_count}"
        )

import json
import logging
import queue
import threading
import time
from datetime import datetime
from zoneinfo import ZoneInfo

from websockets.exceptions import ConnectionClosed
from websockets.sync.client import connect

from models import Quote


logger = logging.getLogger(__name__)
IST = ZoneInfo("Asia/Kolkata")


class WebSocketClient:
    """
    Dedicated market-data thread.

    The receive loop performs only network receive -> parse callback. It does
    not run strategy logic or execution logic.
    """

    def __init__(self, url: str, subscribe_message: dict | None = None, on_message=None):
        self.url = url
        self.subscribe_message = subscribe_message
        self.on_message = on_message
        self._stop_event = threading.Event()
        self._thread: threading.Thread | None = None
        self._ws = None

    def start(self):
        if self._thread and self._thread.is_alive():
            logger.warning("WebSocket already running.")
            return

        self._stop_event.clear()
        self._thread = threading.Thread(
            target=self._run,
            daemon=True,
            name="MarketDataWS",
        )
        self._thread.start()

    def _run(self):
        reconnect_delay = 1.0

        while not self._stop_event.is_set():
            try:
                logger.info("Connecting: %s", self.url)

                with connect(
                    self.url,
                    open_timeout=10,
                    close_timeout=5,
                ) as ws:
                    self._ws = ws
                    reconnect_delay = 1.0
                    logger.info("WebSocket connected.")

                    if self.subscribe_message:
                        ws.send(json.dumps(self.subscribe_message))

                    while not self._stop_event.is_set():
                        try:
                            message = ws.recv(timeout=5)
                        except TimeoutError:
                            # Timeout is not a disconnect. Continue waiting.
                            continue

                        if message is None:
                            break

                        if self.on_message:
                            self.on_message(message)

            except ConnectionClosed as exc:
                if not self._stop_event.is_set():
                    logger.warning("WebSocket connection closed: %s", exc)

            except Exception:
                if not self._stop_event.is_set():
                    logger.exception("WebSocket error")

            finally:
                self._ws = None

            if not self._stop_event.is_set():
                # Bounded reconnect backoff. We use Event.wait() so stop()
                # interrupts the delay immediately instead of waiting 3 sec.
                self._stop_event.wait(reconnect_delay)
                reconnect_delay = min(reconnect_delay * 2.0, 15.0)

        logger.info("WebSocket thread stopped.")

    def stop(self):
        self._stop_event.set()

        if self._ws is not None:
            try:
                self._ws.close()
            except Exception:
                pass

        if (
            self._thread
            and self._thread.is_alive()
            and self._thread is not threading.current_thread()
        ):
            self._thread.join(timeout=5)

        logger.info("WebSocket stopped.")


class CandleBuilder:
    """
    Converts individual trades into OHLCV candles.

    LIVE ticks are intentionally emitted, because the strategy is allowed to
    react intrabar. The strategy uses non-mutating indicator previews, so these
    repeated updates do not corrupt the committed candle history.

    Queue policy:
      - live quote: never block network thread; drop if queue is temporarily full
      - closed quote: retry briefly because losing a completed candle is more
        important than preserving one intermediate live tick
    """

    def __init__(self, symbol: str, interval: str, candle_queue):
        self.symbol = symbol
        self.interval = interval
        self.candle_queue = candle_queue
        self.interval_seconds = self._interval_to_seconds(interval)

        self._candle_start = None
        self._open = None
        self._high = None
        self._low = None
        self._close = None
        self._volume = 0.0

    @staticmethod
    def _interval_to_seconds(interval: str) -> int:
        if interval.endswith("m"):
            try:
                return int(interval[:-1]) * 60
            except ValueError:
                pass
        if interval.endswith("s"):
            try:
                return int(interval[:-1])
            except ValueError:
                pass
        if interval.endswith("h"):
            try:
                return int(interval[:-1]) * 3600
            except ValueError:
                pass
        raise ValueError(f"Unsupported candle interval: {interval}")

    def add_trade(self, timestamp_ms: int, price: float, quantity: float):
        timestamp_seconds = timestamp_ms // 1000
        candle_start = (
            timestamp_seconds // self.interval_seconds
        ) * self.interval_seconds

        if self._candle_start is None:
            self._start_candle(candle_start, price, quantity)
            self._emit(closed=False)
            return

        # Ignore a late/out-of-order trade from a candle that we have already
        # moved past. It must not mutate the current candle.
        if candle_start < self._candle_start:
            logger.debug(
                "Ignoring out-of-order trade: candle=%s current=%s",
                candle_start,
                self._candle_start,
            )
            return

        if candle_start == self._candle_start:
            self._update_candle(price, quantity)
            self._emit(closed=False)
            return

        # New candle. The previous one is now complete from the feed's point
        # of view, so emit exactly one closed event for it.
        self._emit(closed=True)

        if candle_start > self._candle_start + self.interval_seconds:
            logger.warning(
                "Candle gap detected for %s: previous=%s new=%s",
                self.symbol,
                self._candle_start,
                candle_start,
            )

        self._start_candle(candle_start, price, quantity)
        self._emit(closed=False)

    def _start_candle(self, candle_start: int, price: float, quantity: float):
        self._candle_start = candle_start
        self._open = price
        self._high = price
        self._low = price
        self._close = price
        self._volume = quantity

    def _update_candle(self, price: float, quantity: float):
        self._high = max(self._high, price)
        self._low = min(self._low, price)
        self._close = price
        self._volume += quantity

    def _emit(self, closed: bool):
        if self._candle_start is None:
            return

        candle_dt = datetime.fromtimestamp(self._candle_start, tz=IST)

        # Capture this BEFORE queue insertion. StrategyEngine then has a true
        # queue-wait measurement.
        enqueued_at = time.perf_counter()

        quote = Quote(
            symbol=self.symbol,
            date=int(candle_dt.strftime("%Y%m%d")),
            time=(
                candle_dt.hour * 3600
                + candle_dt.minute * 60
                + candle_dt.second
            ),
            open=self._open,
            high=self._high,
            low=self._low,
            close=self._close,
            volume=self._volume,
            is_closed=closed,
            candle_start_ms=self._candle_start * 1000,
            enqueued_at=enqueued_at,
        )

        if closed:
            try:
                self.candle_queue.put(quote, timeout=0.5)
            except queue.Full:
                logger.critical(
                    "CLOSED CANDLE DROPPED for %s — strategy state may need resync",
                    self.symbol,
                )
        else:
            try:
                self.candle_queue.put_nowait(quote)
            except queue.Full:
                # Intermediate live updates are disposable; the next trade
                # will produce a newer snapshot of the same candle.
                logger.debug("Live candle update dropped for %s", self.symbol)


class BinanceSpotFeed:
    """Binance Spot trade feed -> CandleBuilder."""

    BASE_URL = "wss://stream.binance.com:9443/ws"

    def __init__(self, symbol: str, interval: str = "1m", candle_queue=None):
        self.symbol = symbol.upper()
        self.interval = interval

        if candle_queue is None:
            raise ValueError("candle_queue is required")

        self.candle_builder = CandleBuilder(
            symbol=self.symbol,
            interval=interval,
            candle_queue=candle_queue,
        )

        topic = f"{self.symbol.lower()}@trade"
        self.client = WebSocketClient(
            url=f"{self.BASE_URL}/{topic}",
            on_message=self._handle_message,
        )

    def start(self):
        self.client.start()

    def stop(self):
        self.client.stop()

    def _handle_message(self, message):
        try:
            data = json.loads(message)
        except json.JSONDecodeError:
            logger.warning("Invalid Binance message: %r", message)
            return

        if data.get("e") != "trade":
            return

        try:
            trade_timestamp = int(data["T"])
            price = float(data["p"])
            quantity = float(data["q"])
        except (KeyError, TypeError, ValueError) as exc:
            logger.warning("Invalid trade message: %s", exc)
            return

        self.candle_builder.add_trade(
            timestamp_ms=trade_timestamp,
            price=price,
            quantity=quantity,
        )

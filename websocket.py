import json
import time
import threading
import logging

from datetime import datetime
from zoneinfo import ZoneInfo

from websockets.exceptions import ConnectionClosed
from websockets.sync.client import connect

from models import Quote


logger = logging.getLogger(__name__)

IST = ZoneInfo("Asia/Kolkata")


class WebSocketClient:
    """
    Generic WebSocket connection handler.

    It only knows how to:
    - connect
    - receive messages
    - reconnect
    - stop
    """

    def __init__(
        self,
        url: str,
        subscribe_message: dict | None = None,
        on_message=None,
    ):
        self.url = url
        self.subscribe_message = subscribe_message
        self.on_message = on_message

        self._running = False
        self._thread = None
        self._ws = None

    def start(self):
        if self._thread and self._thread.is_alive():
            logger.warning("WebSocket already running.")
            return

        self._running = True

        self._thread = threading.Thread(
            target=self._run,
            daemon=True,
        )

        self._thread.start()

    def _run(self):
        while self._running:

            try:
                logger.info(
                    "Connecting: %s",
                    self.url,
                )

                with connect(
                    self.url,
                    open_timeout=10,
                    close_timeout=5,
                ) as ws:

                    self._ws = ws

                    logger.info("WebSocket connected.")

                    if self.subscribe_message:
                        ws.send(
                            json.dumps(
                                self.subscribe_message
                            )
                        )

                    while self._running:

                        try:
                            message = ws.recv(
                                timeout=5
                            )

                        except TimeoutError:
                            continue

                        if message is None:
                            break

                        if self.on_message:
                            self.on_message(message)

            except ConnectionClosed as exc:

                if self._running:
                    logger.warning(
                        "WebSocket connection closed: %s",
                        exc,
                    )

            except Exception as exc:

                if self._running:
                    logger.exception(
                        "WebSocket error: %s",
                        exc,
                    )

            finally:
                self._ws = None

            if self._running:
                time.sleep(3)

        logger.info(
            "WebSocket thread stopped."
        )

    def stop(self):
        self._running = False

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

        logger.info(
            "WebSocket stopped."
        )


class CandleBuilder:
    """
    Converts individual trades into OHLCV candles.
    """

    def __init__(
        self,
        symbol: str,
        interval: str,
        on_candle=None,
    ):
        self.symbol = symbol
        self.interval = interval
        self.on_candle = on_candle

        self.interval_seconds = (
            self._interval_to_seconds(interval)
        )

        self._candle_start = None

        self._open = None
        self._high = None
        self._low = None
        self._close = None
        self._volume = 0.0

    @staticmethod
    def _interval_to_seconds(interval: str) -> int:

        mapping = {
            "1s": 1,
            "1m": 60,
            "3m": 180,
            "5m": 300,
            "15m": 900,
            "30m": 1800,
            "1h": 3600,
        }

        try:
            return mapping[interval]

        except KeyError:
            raise ValueError(
                f"Unsupported candle interval: {interval}"
            )

    def add_trade(
        self,
        timestamp_ms: int,
        price: float,
        quantity: float,
    ):
        """
        Add one trade to the candle builder.
        """

        timestamp_seconds = timestamp_ms // 1000

        candle_start = (
            timestamp_seconds
            // self.interval_seconds
        ) * self.interval_seconds

        # First trade received
        if self._candle_start is None:

            self._start_candle(
                candle_start,
                price,
                quantity,
            )

            self._emit(closed=False)

            return

        # Trade belongs to current candle
        if candle_start == self._candle_start:

            self._update_candle(
                price,
                quantity,
            )

            self._emit(closed=False)

            return

        # New candle started
        if candle_start > self._candle_start:

            # Close previous candle
            self._emit(closed=True)

            # Start new candle
            self._start_candle(
                candle_start,
                price,
                quantity,
            )

            # Emit new live candle
            self._emit(closed=False)

    def _start_candle(
        self,
        candle_start: int,
        price: float,
        quantity: float,
    ):

        self._candle_start = candle_start

        self._open = price
        self._high = price
        self._low = price
        self._close = price
        self._volume = quantity

    def _update_candle(
        self,
        price: float,
        quantity: float,
    ):

        self._high = max(
            self._high,
            price,
        )

        self._low = min(
            self._low,
            price,
        )

        self._close = price

        self._volume += quantity

    def _emit(self, closed: bool):

        if self._candle_start is None:
            return

        candle_dt = datetime.fromtimestamp(
            self._candle_start,
            tz=IST,
        )

        quote = Quote(
            symbol=self.symbol,
            date=int(
                candle_dt.strftime("%Y%m%d")
            ),
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
        )

        if self.on_candle:
            self.on_candle(
                quote,
            )


class BinanceSpotFeed:
    """
    Binance Spot trade feed.

    Receives individual trades and
    converts them into candles.
    """

    BASE_URL = (
        "wss://stream.binance.com:9443/ws"
    )

    def __init__(
        self,
        symbol: str,
        interval: str = "1m",
        on_candle=None,
    ):
        self.symbol = symbol.upper()
        self.interval = interval

        self.candle_builder = CandleBuilder(
            symbol=self.symbol,
            interval=interval,
            on_candle=on_candle,
        )

        # IMPORTANT:
        # We subscribe to trades, not klines.
        topic = (
            f"{self.symbol.lower()}@trade"
        )

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
            logger.warning(
                "Invalid Binance message: %r",
                message,
            )
            return

        # We only want trade events
        if data.get("e") != "trade":
            return

        try:
            trade_timestamp = int(
                data["T"]
            )

            price = float(
                data["p"]
            )

            quantity = float(
                data["q"]
            )

        except (
            KeyError,
            TypeError,
            ValueError,
        ) as exc:

            logger.warning(
                "Invalid trade message: %s",
                exc,
            )
            return

        self.candle_builder.add_trade(
            timestamp_ms=trade_timestamp,
            price=price,
            quantity=quantity,
        )
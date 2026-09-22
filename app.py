import time
import logging

from callbacks import on_candle
from websocket import BinanceSpotFeed


logging.basicConfig(
    level=logging.INFO,
    format=(
        "%(asctime)s "
        "[%(levelname)s] "
        "[%(name)s] : "
        "%(message)s"
    ),
)


feed = BinanceSpotFeed(
    symbol="BTCUSDT",
    interval="2m",
    on_candle=on_candle,
)

feed.start()

# feed2 = BinanceSpotFeed(
#     symbol="ETHUSDT",
#     interval="4m",
#     on_candle=on_candle
# )

# feed2.start()


try:

    while True:
        time.sleep(1)

except KeyboardInterrupt:

    print("\nStopping...")
    feed.stop()
    # feed2.stop()
import queue
import time
import logging
from websocket import BinanceSpotFeed
from strategy import Strategy
from engine import StrategyEngine
from executor import OrderExecutor
from position import PositionManager
from metrics import MetricsStore
from monitor import MonitorServer


logging.basicConfig(
    level=logging.INFO,
    format=(
        "%(asctime)s "
        "[%(levelname)s] "
        "[%(name)s] : "
        "%(message)s"
    ),
)

logger = logging.getLogger(__name__)


# ── Queues ────────────────────────────────────────────────────────────────── #
# maxsize=0 for candles (unbounded, drops only on network extreme)
candle_queue:  queue.Queue = queue.Queue(maxsize=0)
order_queue:   queue.Queue = queue.Queue(maxsize=100)
metrics_queue: queue.Queue = queue.Queue(maxsize=5000)


# ── Strategy ──────────────────────────────────────────────────────────────── #
strategy = Strategy(
    sma_periods=[12, 50],
    rsi_period=14,
    trade_qty=0.001,
)


# ── Position Manager ──────────────────────────────────────────────────────── #
position_manager = PositionManager(symbol="BTCUSDT")


# ── Thread 4: Real-time Monitor Server (FastAPI + WebSockets) ─────────────── #
metrics_store = MetricsStore()
monitor = MonitorServer(
    metrics_queue=metrics_queue,
    metrics_store=metrics_store,
    host="127.0.0.1",
    port=8765,
)


# ── Thread 2: Strategy Engine (instrumented, zero I/O) ────────────────────── #
engine = StrategyEngine(
    strategy=strategy,
    candle_queue=candle_queue,
    order_queue=order_queue,
    metrics_queue=metrics_queue,
)


# ── Thread 3: Order Executor (paper mode with slippage) ───────────────────── #
executor = OrderExecutor(
    order_queue=order_queue,
    position_manager=position_manager,
    slippage_bps=1.0,     # 1 basis point slippage simulation
    metrics_queue=metrics_queue,
)


# ── Thread 1: WebSocket Feed (Binance live kline) ─────────────────────────── #
feed = BinanceSpotFeed(
    symbol="BTCUSDT",
    interval="2m",
    candle_queue=candle_queue,
)


# ── Start Pipeline ────────────────────────────────────────────────────────── #
monitor.start()
engine.start()
executor.start()
feed.start()

logger.info("=================================================================")
logger.info("🚀 F1 Live Paper Trading Service is Running")
logger.info("📡 Real-time Monitoring Dashboard: http://127.0.0.1:8765")
logger.info("📊 Symbol: BTCUSDT | Timeframe: 2m | Strategy: SMA(12,50) + RSI(14)")
logger.info("⏹️  Press Ctrl+C to stop cleanly")
logger.info("=================================================================")

try:
    while True:
        time.sleep(1)

except KeyboardInterrupt:
    logger.info("\nShutting down trading pipeline...")

    # Stop in reverse order: WS feed first, then drain queues, then workers
    feed.stop()

    # Give engine time to drain remaining candles
    candle_queue.join()

    engine.stop()
    order_queue.join()
    executor.stop()

    logger.info("📊 Final Session Position: %s", position_manager.summary())
    logger.info("✅ Shutdown complete.")
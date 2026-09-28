import logging
import os
import queue
import signal
import time

from websocket import BinanceSpotFeed
from strategy import Strategy
from engine import StrategyEngine
from executor import OrderExecutor
from position import PositionManager
from metrics import MetricsStore
from historical_feed import BinanceHistoricalFeed
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


# ── Thread-safe pipeline queues ───────────────────────────────────────────── #
# Live candle updates are high-frequency and disposable; the CandleBuilder
# drops an intermediate live update only if this queue is temporarily full.
candle_queue: queue.Queue = queue.Queue(maxsize=10_000)

# Orders are rare and must not be silently lost because of queue pressure.
order_queue: queue.Queue = queue.Queue(maxsize=0)

# Dashboard/monitoring is intentionally isolated from the trading path.
metrics_queue: queue.Queue = queue.Queue(maxsize=5_000)


# ── Strategy ─────────────────────────────────────────────────────────────── #
SYMBOL = os.getenv("SYMBOL", "BTCUSDT").upper()
INTERVAL = os.getenv("INTERVAL", "2m")
TRADE_QTY = float(os.getenv("TRADE_QTY", "0.001"))
SLIPPAGE_BPS = float(os.getenv("SLIPPAGE_BPS", "1.0"))
MONITOR_HOST = os.getenv("MONITOR_HOST", "127.0.0.1")
MONITOR_PORT = int(os.getenv("MONITOR_PORT", "8765"))


strategy = Strategy(
    sma_periods=[12, 50],
    rsi_period=14,
    trade_qty=TRADE_QTY,
)


# ── Position Manager ──────────────────────────────────────────────────────── #
position_manager = PositionManager(symbol=SYMBOL)


# ── Startup: historical warmup + chart seed (NOT on live hot path) ──────── #
metrics_store = MetricsStore()
historical_feed = BinanceHistoricalFeed()

try:
    warmup_quotes = historical_feed.get_recent_quotes(
        symbol=SYMBOL,
        interval=INTERVAL,
        limit=max(150, strategy.warmup_target + 25),
    )
    if len(warmup_quotes) < strategy.warmup_target:
        raise RuntimeError(
            f"Insufficient historical candles for warmup: "
            f"got={len(warmup_quotes)} need={strategy.warmup_target}"
        )

    chart_history = strategy.warmup(warmup_quotes)
    metrics_store.seed_historical(
        chart_history,
        fast_sma=strategy.latest_fast,
        slow_sma=strategy.latest_slow,
        rsi=strategy.latest_rsi,
        warmup_count=strategy.warmup_count,
        warmup_target=strategy.warmup_target,
        strategy_ready=strategy.is_ready,
    )
    logger.info(
        "Historical warmup complete: %d closed candles | ready=%s",
        len(warmup_quotes),
        strategy.is_ready,
    )
except Exception:
    logger.exception("Historical warmup failed — refusing to start live pipeline")
    raise SystemExit(1)


# ── Thread 4: Monitor ─────────────────────────────────────────────────────── #
monitor = MonitorServer(
    metrics_queue=metrics_queue,
    metrics_store=metrics_store,
    host=MONITOR_HOST,
    port=MONITOR_PORT,
)


# ── Thread 2: Strategy Engine ────────────────────────────────────────────── #
engine = StrategyEngine(
    strategy=strategy,
    candle_queue=candle_queue,
    order_queue=order_queue,
    metrics_queue=metrics_queue,
)


# ── Thread 3: Order Executor ─────────────────────────────────────────────── #
executor = OrderExecutor(
    order_queue=order_queue,
    position_manager=position_manager,
    slippage_bps=SLIPPAGE_BPS,
    metrics_queue=metrics_queue,
)


# ── Thread 1: Market Data ────────────────────────────────────────────────── #
feed = BinanceSpotFeed(
    symbol=SYMBOL,
    interval=INTERVAL,
    candle_queue=candle_queue,
)


# ── Start pipeline ────────────────────────────────────────────────────────── #
monitor.start()
engine.start()
executor.start()
feed.start()

logger.info("=================================================================")
logger.info("🚀 F1 Live Paper Trading Service is Running")
logger.info("📡 Real-time Monitoring Dashboard: http://localhost:%d", MONITOR_PORT)
logger.info("📊 Symbol: %s | Timeframe: %s | Strategy: SMA(12,50) + RSI(14)", SYMBOL, INTERVAL)
logger.info("⚡ Strategy evaluates LIVE candle updates (intrabar)")
logger.info("🧵 Threads: MarketDataWS → StrategyEngine → OrderExecutor → Monitor")
logger.info("⏹️  Press Ctrl+C to stop cleanly")
logger.info("=================================================================")

shutdown_requested = False


def _request_shutdown(signum, _frame):
    global shutdown_requested
    if shutdown_requested:
        return
    shutdown_requested = True
    logger.info("Received %s — shutting down trading pipeline...", signal.Signals(signum).name)


signal.signal(signal.SIGINT, _request_shutdown)
signal.signal(signal.SIGTERM, _request_shutdown)

try:
    while not shutdown_requested:
        time.sleep(1)

finally:
    logger.info("\nShutting down trading pipeline...")

    # Stop market data first so no new work is produced.
    feed.stop()

    # Finish already queued candles, then stop the strategy worker.
    candle_queue.join()
    engine.stop()

    # Finish already queued orders, then stop the executor.
    order_queue.join()
    executor.stop()

    logger.info("📊 Final Session Position: %s", position_manager.summary())
    logger.info("✅ Shutdown complete.")

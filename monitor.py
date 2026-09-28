"""
monitor.py — Monitoring WebSocket server (Thread 4)

This runs a FastAPI/uvicorn ASGI server in a background thread.
It is 100% read-only with respect to the trading pipeline:

  - StrategyEngine / OrderExecutor put events into `metrics_queue` (put_nowait, non-blocking)
  - MonitorServer drains metrics_queue in an asyncio background task
  - Connected dashboard clients receive JSON pushes via WebSocket
  - MonitorServer NEVER writes to candle_queue or order_queue

Thread isolation:
  - asyncio event loop runs in its own OS thread (separate from all trading threads)
  - metrics_queue is the ONLY shared object — reads are non-blocking (get_nowait)
"""

import asyncio
import json
import logging
import queue
import threading
from pathlib import Path

import uvicorn
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.responses import HTMLResponse

from metrics import MetricsStore, TickEvent, SignalEvent, FillEvent, PositionEvent


logger = logging.getLogger(__name__)

DASHBOARD_HTML = (Path(__file__).parent / "dashboard.html").read_text()


class ConnectionManager:
    """Tracks all active dashboard WebSocket connections."""

    def __init__(self):
        self._clients: list[WebSocket] = []
        self._lock = asyncio.Lock()

    async def connect(self, ws: WebSocket):
        await ws.accept()
        async with self._lock:
            self._clients.append(ws)
        logger.info("Dashboard client connected. Total: %d", len(self._clients))

    async def disconnect(self, ws: WebSocket):
        async with self._lock:
            self._clients = [c for c in self._clients if c is not ws]
        logger.info("Dashboard client disconnected. Total: %d", len(self._clients))

    async def broadcast(self, msg: dict):
        """Send to all connected clients; silently drop dead connections."""
        if not self._clients:
            return
        data = json.dumps(msg)
        dead = []
        async with self._lock:
            clients = list(self._clients)
        for ws in clients:
            try:
                await ws.send_text(data)
            except Exception:
                dead.append(ws)
        for ws in dead:
            await self.disconnect(ws)


class MonitorServer:
    """
    Wraps the FastAPI app + uvicorn server in a daemon thread.
    Call start() once from app.py — it returns immediately.
    """

    def __init__(
        self,
        metrics_queue: queue.Queue,
        metrics_store: MetricsStore,
        host: str = "127.0.0.1",
        port: int = 8765,
    ):
        self.metrics_queue = metrics_queue
        self.metrics_store = metrics_store
        self.host = host
        self.port = port

        self._manager = ConnectionManager()
        self._loop: asyncio.AbstractEventLoop | None = None
        self._thread: threading.Thread | None = None

        self.app = self._build_app()

    def _build_app(self) -> FastAPI:
        app = FastAPI(title="F1 Monitor", docs_url=None, redoc_url=None)

        @app.get("/", response_class=HTMLResponse)
        async def dashboard():
            html_path = Path(__file__).parent / "dashboard.html"
            return HTMLResponse(html_path.read_text(encoding="utf-8"))

        @app.websocket("/ws")
        async def ws_endpoint(websocket: WebSocket):
            await self._manager.connect(websocket)
            # Send full snapshot on connect
            await websocket.send_text(json.dumps({
                "type": "snapshot",
                "data": self.metrics_store.snapshot(),
            }))
            try:
                while True:
                    # Keep connection alive; we push, client doesn't send
                    await websocket.receive_text()
            except WebSocketDisconnect:
                await self._manager.disconnect(websocket)

        return app

    async def _drain_metrics(self):
        """
        Asyncio task that drains the metrics_queue and broadcasts events.
        Runs as a background coroutine in the monitor's event loop.
        Uses get_nowait() so it never blocks the event loop.
        """
        while True:
            drained = 0
            while drained < 50:   # max 50 events per loop iteration
                try:
                    event = self.metrics_queue.get_nowait()
                except queue.Empty:
                    break

                self.metrics_queue.task_done()
                drained += 1

                if isinstance(event, TickEvent):
                    self.metrics_store.apply_tick(event)
                    await self._manager.broadcast({
                        "type": "tick",
                        "data": {
                            "symbol":              event.symbol,
                            "price":               event.price,
                            "open":                event.open,
                            "high":                event.high,
                            "low":                 event.low,
                            "volume":              event.volume,
                            "is_closed":           event.is_closed,
                            "tick_count":          self.metrics_store.tick_count,
                            "candle_queue_depth":  event.candle_queue_depth,
                            "order_queue_depth":   event.order_queue_depth,
                            "strategy_eval_ms":    event.strategy_eval_ms,
                            "candle_latency_ms":   event.candle_latency_ms,
                            "fast_sma":            round(event.fast_sma, 2) if event.fast_sma is not None else None,
                            "slow_sma":            round(event.slow_sma, 2) if event.slow_sma is not None else None,
                            "rsi":                 round(event.rsi, 2) if event.rsi is not None else None,
                            "warmup_count":        event.warmup_count,
                            "warmup_target":       event.warmup_target,
                            "strategy_ready":      event.strategy_ready,
                        }
                    })

                elif isinstance(event, SignalEvent):
                    self.metrics_store.apply_signal(event)
                    await self._manager.broadcast({
                        "type": "signal",
                        "data": {
                            "symbol":       event.symbol,
                            "side":         event.side,
                            "price":        event.price,
                            "reason":       event.reason,
                            "order_id":     event.order_id,
                            "signal_count": self.metrics_store.signal_count,
                            "ts":           event.ts,
                        }
                    })

                elif isinstance(event, FillEvent):
                    self.metrics_store.apply_fill(event)
                    await self._manager.broadcast({
                        "type": "fill",
                        "data": {
                            "order_id":          event.order_id,
                            "symbol":            event.symbol,
                            "side":              event.side,
                            "quantity":          event.quantity,
                            "fill_price":        event.fill_price,
                            "slippage":          event.slippage,
                            "signal_to_fill_ms": event.signal_to_fill_ms,
                            "realized_pnl":      event.realized_pnl,
                            "total_realized_pnl": event.total_realized_pnl,
                            "trade_count":       event.trade_count,
                            "ts":                event.ts,
                        }
                    })

                elif isinstance(event, PositionEvent):
                    self.metrics_store.apply_position(event)
                    await self._manager.broadcast({
                        "type": "position",
                        "data": {
                            "symbol":      event.symbol,
                            "side":        event.side,
                            "entry_price": event.entry_price,
                            "quantity":    event.quantity,
                            "ts":          event.ts,
                        }
                    })

            # Yield control; 50ms poll keeps dashboard snappy without hogging CPU
            await asyncio.sleep(0.05)

    def _run(self):
        """Target for the monitor daemon thread."""
        self._loop = asyncio.new_event_loop()
        asyncio.set_event_loop(self._loop)

        # Start the metrics drain task
        self._loop.create_task(self._drain_metrics())

        config = uvicorn.Config(
            app=self.app,
            host=self.host,
            port=self.port,
            loop="none",          # we provide the loop
            log_level="warning",  # quiet — don't spam trading logs
            access_log=False,
        )
        server = uvicorn.Server(config)
        self._loop.run_until_complete(server.serve())

    def start(self):
        self._thread = threading.Thread(
            target=self._run,
            daemon=True,
            name="MonitorServer",
        )
        self._thread.start()
        logger.info(
            "📡 Monitor dashboard: http://%s:%d",
            self.host,
            self.port,
        )

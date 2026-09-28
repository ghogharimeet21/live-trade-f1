# F1

## Live dashboard

Open `http://127.0.0.1:8765` while the paper-trading service is running.

The dashboard now includes a TradingView-style execution chart using TradingView Lightweight Charts 5.2.1. It is UI/monitoring-only: chart aggregation and rendering stay on the monitor/browser side, while strategy and order execution remain on their existing dedicated threads.

Chart features:
- live 2m candles built from the same candle updates used by the strategy
- live SMA(12) and SMA(50) overlays
- RSI(14) pane with 30/50/70 guides
- strategy BUY/SELL signal markers
- paper fill markers
- current LONG/SHORT entry price line
- crosshair, fit, and follow-live controls

The Lightweight Charts library is loaded from the browser CDN, so the dashboard needs outbound browser access to `unpkg.com`.

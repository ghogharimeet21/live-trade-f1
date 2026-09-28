import time
from datetime import timezone, datetime

import requests

from constants import BINANCE_HISTORICAL_URL
from models import Quote
from utils import date_to_ms, split_datetime


# Binance Spot /api/v3/klines does not provide a native 2m interval.
# Keep the live feed at 2m by constructing those candles from 1m REST klines
# during startup/backfill. This is startup-only work and never touches the
# live strategy/executor threads.
NATIVE_BINANCE_INTERVALS = {
    "1s",
    "1m",
    "3m",
    "5m",
    "15m",
    "30m",
    "1h",
    "2h",
    "4h",
    "6h",
    "8h",
    "12h",
    "1d",
    "3d",
    "1w",
    "1M",
}


class BinanceHistoricalFeed:
    """REST helper for historical strategy warmup/backfill.

    Native Binance intervals are requested directly.
    Custom minute intervals (for example 2m) are built from Binance 1m
    klines so the live CandleBuilder and historical warmup use the same
    candle definition.
    """

    MAX_LIMIT = 1000

    def get_data(
        self,
        symbol: str,
        start_date: int,
        end_date: int,
        interval: str,
        timeout: float = 10.0,
    ):
        start_ts = date_to_ms(start_date)
        end_ts = date_to_ms(end_date)

        target_interval_ms = self._interval_to_ms(interval)
        source_interval, multiplier = self._source_interval(interval)

        # Fetch enough source candles to construct the requested interval.
        source_rows = self._fetch_range(
            symbol=symbol,
            start_ts=start_ts,
            end_ts=end_ts,
            source_interval=source_interval,
            timeout=timeout,
        )

        if multiplier == 1:
            rows = [
                row for row in source_rows
                if start_ts <= int(row[0]) < end_ts
            ]
        else:
            rows = self._aggregate_rows(
                source_rows,
                target_interval_ms=target_interval_ms,
                start_ts=start_ts,
                end_ts=end_ts,
            )

        return [self._row_to_quote(symbol, row) for row in rows]

    def get_recent_quotes(
        self,
        symbol: str,
        interval: str,
        limit: int = 200,
        timeout: float = 10.0,
    ):
        """Return recent COMPLETED target candles for startup warmup/chart seed.

        For a custom interval such as 2m, Binance 1m klines are aggregated
        into exactly the same epoch-aligned candle boundaries used by the
        live CandleBuilder.
        """
        if limit <= 0:
            return []

        target_interval_ms = self._interval_to_ms(interval)
        source_interval, multiplier = self._source_interval(interval)
        now_ms = int(time.time() * 1000)

        # Ask for a little extra history so we still have `limit` completed
        # target candles after removing the currently-forming candle and
        # aggregating source bars.
        source_limit = min(
            self.MAX_LIMIT,
            max(10, limit * multiplier + multiplier + 5),
        )

        response = requests.get(
            BINANCE_HISTORICAL_URL,
            params={
                "symbol": symbol.upper(),
                "interval": source_interval,
                "limit": source_limit,
                "endTime": now_ms,
            },
            timeout=timeout,
        )
        response.raise_for_status()
        source_rows = response.json()

        if multiplier == 1:
            rows = [
                row for row in source_rows
                if int(row[0]) + target_interval_ms <= now_ms
            ]
        else:
            rows = self._aggregate_rows(
                source_rows,
                target_interval_ms=target_interval_ms,
                start_ts=None,
                end_ts=now_ms,
            )
            rows = [
                row for row in rows
                if int(row[0]) + target_interval_ms <= now_ms
            ]

        return [
            self._row_to_quote(symbol, row)
            for row in rows[-limit:]
        ]

    def _fetch_range(
        self,
        symbol: str,
        start_ts: int,
        end_ts: int,
        source_interval: str,
        timeout: float,
    ) -> list[list]:
        """Fetch a finite range with Binance 1000-row pagination."""
        rows: list[list] = []
        current_start = start_ts

        while current_start < end_ts:
            response = requests.get(
                BINANCE_HISTORICAL_URL,
                params={
                    "symbol": symbol.upper(),
                    "interval": source_interval,
                    "startTime": current_start,
                    "endTime": end_ts,
                    "limit": self.MAX_LIMIT,
                },
                timeout=timeout,
            )
            response.raise_for_status()
            data = response.json()

            if not data:
                break

            rows.extend(
                row for row in data
                if start_ts <= int(row[0]) < end_ts
            )

            last_open_time = int(data[-1][0])
            if last_open_time < current_start:
                break

            # One millisecond beyond the last open time prevents requesting
            # the same first row again on the next page.
            next_start = last_open_time + 1
            if next_start <= current_start:
                break

            current_start = next_start

            if len(data) < self.MAX_LIMIT:
                break

        return rows

    @staticmethod
    def _aggregate_rows(
        source_rows: list[list],
        target_interval_ms: int,
        start_ts: int | None,
        end_ts: int,
    ) -> list[list]:
        """Aggregate 1m OHLCV rows into target epoch-aligned candles."""
        groups: dict[int, list] = {}

        for row in source_rows:
            open_time_ms = int(row[0])
            if start_ts is not None and open_time_ms < start_ts:
                continue
            if open_time_ms >= end_ts:
                continue

            group_start = (open_time_ms // target_interval_ms) * target_interval_ms
            group = groups.get(group_start)

            if group is None:
                # Preserve the Binance kline tuple shape as far as the rest
                # of this application cares: open, high, low, close, volume.
                groups[group_start] = [
                    group_start,       # 0 open time
                    row[1],            # 1 open
                    row[2],            # 2 high
                    row[3],            # 3 low
                    row[4],            # 4 close
                    row[5],            # 5 volume
                ]
                continue

            group[2] = max(float(group[2]), float(row[2]))
            group[3] = min(float(group[3]), float(row[3]))
            group[4] = row[4]
            group[5] = float(group[5]) + float(row[5])

        # Only return target candles that are fully covered by the requested
        # range. Recent calls additionally filter incomplete current candles.
        return [
            row
            for group_start, row in sorted(groups.items())
            if group_start + target_interval_ms <= end_ts
        ]

    @staticmethod
    def _source_interval(interval: str) -> tuple[str, int]:
        if interval in NATIVE_BINANCE_INTERVALS:
            return interval, 1

        # Custom minute intervals are built from 1m candles.
        if interval.endswith("m"):
            try:
                minutes = int(interval[:-1])
            except ValueError as exc:
                raise ValueError(f"Unsupported interval: {interval}") from exc

            if minutes <= 0:
                raise ValueError(f"Unsupported interval: {interval}")

            return "1m", minutes

        raise ValueError(
            f"Unsupported Binance/custom interval: {interval}. "
            "Use a native Binance interval or a positive minute interval "
            "such as 2m."
        )

    @staticmethod
    def _interval_to_ms(interval: str) -> int:
        if not interval:
            raise ValueError("Interval cannot be empty")

        units = {
            "s": 1000,
            "m": 60_000,
            "h": 3_600_000,
            "d": 86_400_000,
            "w": 7 * 86_400_000,
        }

        # Months are not used by the live engine and are intentionally left
        # out because their duration is calendar-dependent.
        suffix = interval[-1]
        if suffix not in units:
            raise ValueError(f"Unsupported interval: {interval}")

        try:
            amount = int(interval[:-1])
        except ValueError as exc:
            raise ValueError(f"Unsupported interval: {interval}") from exc

        if amount <= 0:
            raise ValueError(f"Unsupported interval: {interval}")

        return amount * units[suffix]

    @staticmethod
    def _row_to_quote(symbol: str, row: list) -> Quote:
        open_time_ms = int(row[0])
        date_obj = split_datetime(open_time_ms)

        return Quote(
            symbol=symbol.upper(),
            date=date_obj.date,
            time=date_obj.time,
            open=float(row[1]),
            high=float(row[2]),
            low=float(row[3]),
            close=float(row[4]),
            volume=float(row[5]),
            is_closed=True,
            candle_start_ms=open_time_ms,
        )


# Backward-compatible alias for the typo used by older code.
BianaceHistoricalFeed = BinanceHistoricalFeed

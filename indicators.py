from collections import deque


class SMA:
    """Simple moving average with a non-mutating intrabar preview."""

    def __init__(self, period: int):
        if period <= 0:
            raise ValueError("SMA period must be positive")

        self.period = period
        self.values = deque(maxlen=period)
        self.total = 0.0

    def update(self, value: float) -> float | None:
        if len(self.values) == self.period:
            self.total -= self.values[0]

        self.values.append(value)
        self.total += value

        if len(self.values) < self.period:
            return None

        return self.total / self.period

    def preview(self, value: float) -> float | None:
        """
        Calculate SMA as if `value` were the current candle close without
        changing the committed indicator state.
        """
        if len(self.values) < self.period - 1:
            return None

        total = self.total
        if len(self.values) == self.period:
            total -= self.values[0]

        total += value
        return total / self.period


class RSI:
    """
    RSI using Wilder's smoothing.

    `update()` commits one completed candle.
    `preview()` evaluates the current live candle without mutating state.
    """

    def __init__(self, period: int = 14):
        if period <= 0:
            raise ValueError("RSI period must be positive")

        self.period = period
        self.prev_close: float | None = None

        # Seed state during the first `period` price changes.
        self._gain_sum = 0.0
        self._loss_sum = 0.0
        self._seed_count = 0

        # Wilder-smoothed state after warmup.
        self.avg_gain: float | None = None
        self.avg_loss: float | None = None

    @staticmethod
    def _value(avg_gain: float, avg_loss: float) -> float:
        if avg_loss == 0:
            return 100.0
        rs = avg_gain / avg_loss
        return 100.0 - (100.0 / (1.0 + rs))

    def update(self, close: float) -> float | None:
        if self.prev_close is None:
            self.prev_close = close
            return None

        change = close - self.prev_close
        self.prev_close = close
        gain = max(change, 0.0)
        loss = max(-change, 0.0)

        if self.avg_gain is None or self.avg_loss is None:
            self._gain_sum += gain
            self._loss_sum += loss
            self._seed_count += 1

            if self._seed_count < self.period:
                return None

            self.avg_gain = self._gain_sum / self.period
            self.avg_loss = self._loss_sum / self.period
            return self._value(self.avg_gain, self.avg_loss)

        self.avg_gain = (
            self.avg_gain * (self.period - 1) + gain
        ) / self.period
        self.avg_loss = (
            self.avg_loss * (self.period - 1) + loss
        ) / self.period

        return self._value(self.avg_gain, self.avg_loss)

    def preview(self, close: float) -> float | None:
        """Calculate one intrabar RSI value without changing state."""
        if self.prev_close is None:
            return None

        change = close - self.prev_close
        gain = max(change, 0.0)
        loss = max(-change, 0.0)

        # Still in RSI warmup. We can preview the would-be next seed value.
        if self.avg_gain is None or self.avg_loss is None:
            count = self._seed_count + 1
            gain_sum = self._gain_sum + gain
            loss_sum = self._loss_sum + loss
            if count < self.period:
                return None

            return self._value(
                gain_sum / self.period,
                loss_sum / self.period,
            )

        avg_gain = (
            self.avg_gain * (self.period - 1) + gain
        ) / self.period
        avg_loss = (
            self.avg_loss * (self.period - 1) + loss
        ) / self.period

        return self._value(avg_gain, avg_loss)


class EMA:
    """Exponential Moving Average — O(1) per update, seeds from SMA."""

    def __init__(self, period: int):
        self.period = period
        self.alpha = 2 / (period + 1)
        self._sma_seed = SMA(period)
        self.value: float | None = None

    def update(self, value: float) -> float | None:
        if self.value is None:
            seed = self._sma_seed.update(value)
            if seed is None:
                return None
            self.value = seed
            return self.value
        self.value = (value - self.value) * self.alpha + self.value
        return self.value


class MACD:
    def __init__(self, fast: int = 12, slow: int = 26, signal: int = 9):
        self.fast_ema = EMA(fast)
        self.slow_ema = EMA(slow)
        self.signal_ema = EMA(signal)
        self.macd: float | None = None
        self.signal: float | None = None
        self.histogram: float | None = None

    def update(self, value: float) -> tuple[float, float, float] | None:
        fast = self.fast_ema.update(value)
        slow = self.slow_ema.update(value)
        if fast is None or slow is None:
            return None

        self.macd = fast - slow
        self.signal = self.signal_ema.update(self.macd)
        if self.signal is None:
            return None

        self.histogram = self.macd - self.signal
        return self.macd, self.signal, self.histogram


class BollingerBands:
    def __init__(self, period: int = 20, num_std: float = 2.0):
        self.period = period
        self.num_std = num_std
        self.values: deque[float] = deque(maxlen=period)
        self.sum = 0.0
        self.sum_sq = 0.0

    def update(self, value: float) -> tuple[float, float, float] | None:
        if len(self.values) == self.period:
            oldest = self.values[0]
            self.sum -= oldest
            self.sum_sq -= oldest * oldest

        self.values.append(value)
        self.sum += value
        self.sum_sq += value * value

        if len(self.values) < self.period:
            return None

        mean = self.sum / self.period
        variance = max(self.sum_sq / self.period - mean * mean, 0.0)
        std = variance ** 0.5

        upper = mean + self.num_std * std
        lower = mean - self.num_std * std
        return upper, mean, lower


class ATR:
    def __init__(self, period: int = 14):
        self.period = period
        self.prev_close: float | None = None
        self.avg_tr: float | None = None
        self._tr_sum = 0.0
        self._count = 0

    def update(self, high: float, low: float, close: float) -> float | None:
        if self.prev_close is None:
            self.prev_close = close
            return None

        tr = max(
            high - low,
            abs(high - self.prev_close),
            abs(low - self.prev_close),
        )
        self.prev_close = close

        if self.avg_tr is None:
            self._tr_sum += tr
            self._count += 1
            if self._count < self.period:
                return None
            self.avg_tr = self._tr_sum / self.period
            return self.avg_tr

        self.avg_tr = (self.avg_tr * (self.period - 1) + tr) / self.period
        return self.avg_tr


class Stochastic:
    def __init__(self, k_period: int = 14, d_period: int = 3):
        self.k_period = k_period
        self.highs: deque[float] = deque(maxlen=k_period)
        self.lows: deque[float] = deque(maxlen=k_period)
        self.d_sma = SMA(d_period)

    def update(self, high: float, low: float, close: float) -> tuple[float, float] | None:
        self.highs.append(high)
        self.lows.append(low)

        if len(self.highs) < self.k_period:
            return None

        highest = max(self.highs)
        lowest = min(self.lows)
        rng = highest - lowest

        k = 100.0 if rng == 0 else (close - lowest) / rng * 100.0
        d = self.d_sma.update(k)
        if d is None:
            return None
        return k, d

from logging import getLogger
from datetime import datetime, timedelta, timezone
from typing import List

logger = getLogger(__name__)


def make_date_obj(date, date_format="%Y%m%d") -> datetime:
    return datetime.strptime(str(date), date_format)

def date_to_ms(date_str: str) -> int:
    dt = datetime.strptime(str(date_str), "%Y%m%d").replace(tzinfo=timezone.utc)
    return int(dt.timestamp() * 1000)


class DateTime:
    def __init__(self, date, time):
        self.date = date
        self.time = time
def split_datetime(ms: int) -> DateTime:
    dt = datetime.fromtimestamp(
        ms / 1000, tz=timezone.utc
    )  # ← correct UTC, not deprecated
    date_int = int(dt.strftime("%y%m%d"))
    seconds = dt.hour * 3600 + dt.minute * 60 + dt.second

    return DateTime( date_int, seconds)


def shift_date(date: str | int, shift: int, date_format: str = "%Y%m%d") -> int:
    return int(
        (datetime.strptime(str(date), date_format) + timedelta(days=shift)).strftime(
            date_format
        )
    )


def hms_to_seconds(time_str: str) -> int:
    hours, minutes, seconds = map(int, time_str.split(":"))
    if hours > 23:
        raise ValueError(
            f"in {time_str} hour={hours} is not valid please enter less then 24"
        )
    if minutes > 59:
        raise ValueError(
            f"in {time_str} minute={minutes} is not valid please enter less then 60"
        )
    if seconds > 59:
        raise ValueError(
            f"in {time_str} seconds={seconds} is not valid please enter less then 60"
        )
    return (hours * 3600) + minutes * 60 + seconds


def seconds_to_hms(seconds: int) -> str:
    h, remainder = divmod(seconds, 3600)
    m, s = divmod(remainder, 60)
    return f"{h:02d}:{m:02d}:{s:02d}"


def get_atm(spot, strike_gap):
    return round(spot / strike_gap) * strike_gap


def get_date_span(
    start_date: int, end_date: int, date_format: str = "%Y%m%d"
) -> List[int]:

    start = make_date_obj(start_date, date_format)
    end = make_date_obj(end_date, date_format)

    dates: List[int] = []

    while start <= end:
        dates.append(int(start.strftime("%Y%m%d")))
        start += timedelta(days=1)

    return dates


def calc_position_size(
    account_equity: float,
    risk_per_trade: float,
    stop_distance: float,
    min_qty: float = 0.0,
    qty_step: float = 0.0,
    min_notional: float = 0.0,
    entry_price: float | None = None,
    max_notional: float | None = None,
) -> float:
    """
    Risk-based position sizing, generalized for ANY strategy that risks a
    fixed fraction of account equity per trade rather than a fixed dollar
    amount — not specific to any one strategy's indicators or entry logic.

    Core formula (see any volatility-stop trend/breakout strategy spec):
        risk_amount     = account_equity * risk_per_trade
        position_qty    = risk_amount / stop_distance

    Then the raw quantity is clamped against exchange-style constraints,
    since a mathematically "correct" size is useless if the exchange
    rejects the order:
      - min_qty     -> quantity below this is not tradeable; returns 0.0
                       (skip the trade) rather than silently rounding up
                       into a bigger-than-intended risk.
      - qty_step    -> quantity is floored to the nearest step (never
                       rounded up — rounding up would silently increase
                       risk beyond risk_per_trade).
      - min_notional / max_notional -> require entry_price to check
                       quantity * entry_price against exchange notional
                       floors/ceilings. Skipped (not enforced) if
                       entry_price is not supplied, since notional can't
                       be evaluated without it.

    Returns 0.0 (never raises) when the resulting size fails a constraint
    — the caller (strategy execute()) should treat 0.0 as "do not place
    this trade" and move on, consistent with the spec's fail-closed
    principle: an unsizeable trade is skipped, never guessed at.

    Parameters
    ----------
    account_equity : float
        Current account equity (or a fixed backtest starting balance) to
        risk a fraction of. Must be > 0.
    risk_per_trade : float
        Fraction of account_equity to risk if the stop is hit, e.g. 0.01
        for 1%. Must be > 0 and <= 1.
    stop_distance : float
        Absolute price distance between entry and stop-loss (already
        computed by the caller, e.g. via ATR * multiplier). Must be > 0.
    min_qty, qty_step, min_notional, max_notional : float, optional
        Exchange-style constraints. Each defaults to 0.0 / None, meaning
        "not enforced" — callers backtesting without exchange filters can
        omit them entirely and get the raw risk-based quantity.
    entry_price : float, optional
        Required only if min_notional or max_notional is provided.

    Returns
    -------
    float
        The position quantity to trade, already step-rounded and
        constraint-checked, or 0.0 if the trade should be skipped.
    """
    if account_equity <= 0:
        raise ValueError("account_equity must be positive")
    if not (0 < risk_per_trade <= 1):
        raise ValueError("risk_per_trade must be in (0, 1]")
    if stop_distance <= 0:
        raise ValueError("stop_distance must be positive")

    risk_amount = account_equity * risk_per_trade
    quantity = risk_amount / stop_distance

    if qty_step and qty_step > 0:
        # Floor to the step — never round up, that would exceed the
        # intended risk_per_trade.
        steps = quantity // qty_step
        quantity = steps * qty_step

    if min_qty and quantity < min_qty:
        return 0.0

    if (min_notional or max_notional) and entry_price is not None:
        notional = quantity * entry_price
        if min_notional and notional < min_notional:
            return 0.0
        if max_notional and notional > max_notional:
            return 0.0

    return quantity
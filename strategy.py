import logging
from models import Quote

logger = logging.getLogger(__name__)


from indicators import SMA, RSI


class Strategy:
    def __init__(self, sma_periods: list, RSI_period):
        self.sma_periods = sorted(sma_periods)
        self.RSI_period = RSI_period

        self.rsi = RSI(self.RSI_period)
        self.fast_sma = SMA(self.sma_periods[0])
        self.slow_sma = SMA(self.sma_periods[1])
        # self.smas = {period: SMA(period) for period in self.sma_periods}

        self.prev_fast_sam = None
        self.prev_slow_sam = None
        

    def on_candle(
        self,
        quote: Quote,
    ):
        if len(self.sma_periods) > 2:
            raise ValueError("no more then two periods allowed!")
        
        fast_sma = self.fast_sma.update(quote.close)
        slow_sma = self.slow_sma.update(quote.close)

        rsi_value = self.rsi.update(quote.close)

        if self.prev_fast_sam is None and self.prev_slow_sam is None:
            self.prev_fast_sam = fast_sma
            self.prev_slow_sam = slow_sma
            return

        
        



        ...


import logging
from models import Quote

logger = logging.getLogger(__name__)



from indicators import SMA, RSI


class Strategy:
    def __init__(self, sma_periods: list, RSI_period):
        self.sma_periods = sorted(sma_periods)
        self.RSI_period = RSI_period

        self.rsi = RSI(self.RSI_period)

        self.smas = {
            period: SMA(period)
            for period in self.sma_periods
        }




    def on_candle(self, quote: Quote, is_closed: bool):
        sma_values = {
            period: sma.update(quote.close)
            for period, sma in self.smas.items()
        }

        rsi_value = self.rsi.update(quote.close)


        
        ...
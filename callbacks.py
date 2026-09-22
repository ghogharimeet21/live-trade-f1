from models import Quote
import logging

from strategy import Strategy


logger = logging.getLogger(__name__)


strategy = Strategy([12, 50], 14)


def on_candle(quote: Quote):
    logger.info(quote)  #if quote.is_closed else None

    strategy.on_candle(quote)
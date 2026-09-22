from models import Quote
from enums import Direction


class Contraction:
    def __init__(self, counts):
        self.counts = counts
        self.prev_quotes = []

    def on_quote(self, quote: Quote, is_closed: bool) -> Direction:

        if not is_closed:
            return

        if len(self.prev_quotes) < self.counts:
            self.prev_quotes.append(quote)
            return

        self.prev_quotes.append(quote)

        self.prev_quotes.pop(0)


def is_doji(quote: Quote) -> bool:
    body = abs(quote.close - quote.open)
    candle_range = quote.high - quote.low
    return True if candle_range == 0 else (body <= candle_range * 0.1)

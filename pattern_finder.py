from models import Quote


class Contraction:
    def __init__(self, counts):
        self.counts = counts
        self.prev_quotes = []


    def on_quote(self, quote: Quote, is_closed: bool):

        if not is_closed:
            return

        if len(self.prev_quotes) < self.counts:
            self.prev_quotes.append(quote)
            return

        

        
import requests
from utils import date_to_ms, split_datetime
from constants import BINANCE_HISTORICAL_URL




class BianaceHistoricalFeed:

    def get_data(
            symbol: str,
            start_date: int, 
            end_date: int,
            interval: str
    ):
        start_ts = date_to_ms(start_date)
        end_ts = date_to_ms(end_date)

        quotes = []

        current_start = start_ts

        while current_start < end_ts:
            response = requests.get(
                BINANCE_HISTORICAL_URL,
                params={
                    "symbol": symbol,
                    "interval": interval,
                    "startTime": current_start,
                    "endTime": end_ts,
                    "limit": 1000,
                },
            )
            response.raise_for_status()

            data = response.json()

            if not data:
                break

            for row in data:
                utc = row[0]

                if utc >= end_ts:
                    return quotes

                date_int, time_seconds = split_datetime(utc)

                quotes.append({
                    "date":date_int,
                    "time":time_seconds,
                    "open":float(row[1]),
                    "high":float(row[2]),
                    "low":float(row[3]),
                    "close":float(row[4]),
                    "volume":float(row[5]),
                })
        return quotes







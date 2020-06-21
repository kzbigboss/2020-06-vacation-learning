import re
import pandas as pd
import random


def get_interested_stocks():
    interested_stocks = [
        # Randomly selected tech stock stickers
        'AMZN',
        'AAPL',
        'FB',
        'GOOG',
        'NFLX',
        'TSLA',
        'INTC',
        'MSFT',
        'AMD',
        'WORK',
        'ZM'
    ]

    interested_stocks_payload = []

    for stock in interested_stocks:
        interested_stocks_payload.append(
            {"symbol": stock}
        )

    return random.sample(interested_stocks_payload, k=10)


def generate_data_check_query(symbol):
    query_base = """
        with expected_timestamps as (
            select
                capture_minute,
                '{0}' as symbol
            from
                unnest(sequence (date_trunc('hour', date_add ('hour', - 1, localtimestamp)),
                        date_add ('minute',
                            - 1,
                            date_trunc('hour', localtimestamp)),
                        interval '1' minute)) as t (capture_minute)),
        actual_timestamps as (
            select
                date_trunc('minute', from_unixtime (cast(t as integer))) capture_minute,
                symbol
            from
                "stock-quote-landing-raw"
            where
                symbol = '{0}'
                and from_unixtime (cast(t as integer))
                between date_trunc('hour', date_add ('hour', - 1, localtimestamp))
                and date_trunc('hour', localtimestamp))
        select
            *
        from
            expected_timestamps
        
        except
        
        select
            *
        from
            actual_timestamps
    """ \
        .format(symbol)

    query_flat = re.sub('\s+', ' ', query_base)

    return query_flat


def main():
    print(generate_data_check_query("AMZN"))


if __name__ == "__main__":
    main()

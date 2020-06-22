"""
This function's purpose is to query the stock data via Athena
and determine which minutes need to be acquired to complete
our data lake.
"""

"""
Function Steps
1 - Get list of interested stocks
2 - Feed in the interested hour
3 - Run a query to find all missing minutes for all interested stocks
4 - Parse through query results and organize each missing minute by stock symbol
5 - Request each stock symbol's missing minutes
"""

import helper as h
import datetime
import re


def get_epoch_prior_hour_range():
    prior_hour = datetime.datetime.now() + datetime.timedelta(hours=-1)
    prior_hour_start = int(datetime.datetime(prior_hour.year
                                             , prior_hour.month
                                             , prior_hour.day
                                             , prior_hour.hour
                                             ).timestamp()
                           )
    prior_hour_end = int(datetime.datetime(prior_hour.year
                                           , prior_hour.month
                                           , prior_hour.day
                                           , prior_hour.hour
                                           , 59  # minute
                                           , 59  # second
                                           ).timestamp()
                         )

    return prior_hour_start, prior_hour_end


def generate_data_check_query(query_range_start, query_range_end, stock_list):
    # unix_timestamp_start = 1592776800
    # unix_timestamp_end = 1592780340
    # stock_list = ['AMZN', 'AAPL', 'FB']

    query_base = """
    with expected_timestamps as (
    select
        capture_minute,
        symbols
    from
        unnest(sequence (from_unixtime ({unix_timestamp_start}),
                from_unixtime ({unix_timestamp_end}),
                interval '1' minute)) as t (capture_minute),
        unnest(array {stock_list}) as t (symbols)),
    actual_timestamps as (
        select
            date_trunc('minute', from_unixtime (cast(t as integer))) capture_minute,
            symbol
        from
            "stock-quote-landing-raw"
        where
            from_unixtime (cast(t as integer))
            between from_unixtime ({unix_timestamp_start})
            and from_unixtime ({unix_timestamp_end}))
    select
        *
    from
        expected_timestamps
    except
    select
        *
    from
        actual_timestamps
    order by
        2,
        1
    """ \
        .format(unix_timestamp_start=query_range_start,
                unix_timestamp_end=query_range_end,
                stock_list=stock_list)

    query_flat = re.sub('\s+', ' ', query_base)

    return query_flat


def lambda_handler(event, context):
    if "query_range_start" in event:
        query_range_start = event["query_range_start"]
        query_range_end = event["query_range_end"]
    else:
        query_range_start, query_range_end = get_epoch_prior_hour_range()

    interested_stocks = h.get_interested_stocks()

    query = generate_data_check_query(query_range_start, query_range_end, interested_stocks)

    pass

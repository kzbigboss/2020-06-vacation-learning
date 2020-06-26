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
    # for testing, variables to test query execution in Athena console
    # unix_timestamp_start = 1592776800
    # unix_timestamp_end = 1592780340
    # stock_list = ['AMZN', 'AAPL', 'FB']

    # the below SQL prepares two data sets:
    # [A] - Expected_timestamps: derived by unnesting a list of stock symbols and generating
    #           the minutes found in a given range of time
    # [B] - Actual_timestamps: actual data queried in the data lake with the
    #           timestamp field ('t' below) is truncated/rounded to the minute the data was captured.
    # With these data sets, we perform a minus (called 'except in Presto/Athena) between the two data sets
    # to determine which expected timestamps are not found in the actual timestamps.

    query_base = """
    with expected_timestamps as (
    select
        cast(to_unixtime(capture_minute) as integer) as capture_minute,
        symbols
    from
        unnest(sequence (from_unixtime ({unix_timestamp_start}),
                from_unixtime ({unix_timestamp_end}),
                interval '1' minute)) as t (capture_minute),
        unnest(array {stock_list}) as t (symbols)),
    actual_timestamps as (
        select
            cast(to_unixtime(date_trunc('minute', from_unixtime (cast(t as integer)))) as integer) capture_minute,
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
    # set query range variables if they exist in the Lambda event
    if 'minute_start' in event:
        minute_start = event['minute_start']
        minute_end = event['minute_end']
    else:
        minute_start, minute_end = get_epoch_prior_hour_range()

    # pull the stocks symbols we are interested in
    interested_stocks = h.get_interested_stocks()

    # generate a query to check for missing minutes between the provide time ranges
    # for the stock symbols we are interested in
    query = generate_data_check_query(minute_start, minute_end, interested_stocks)

    # for testing, replace with this query to validate below will handle a query with zero records
    # query = 'select 1 except select 1'

    # set variables to run query in Athena
    database = h.get_environ_variable('athena_database')
    workgroup = 'primary'

    # run Athena query
    query_data = h.get_query_from_athena(query, database, workgroup)

    # Parse Athena results for any missing minutes of data for each stock symbol
    missing_minutes = h.parse_missing_minutes(query_data)

    response = {}

    # Handle if there are missing minutes
    if len(missing_minutes) == 0:
        response["health_pass"] = True
    else:
        response["health_pass"] = False
        response["missing_minutes"] = missing_minutes
        response["repair_settings"] = h.get_job_repair_settings()

    return response

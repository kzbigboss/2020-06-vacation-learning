import re
import os
import boto3
import json
import pandas as pd


def get_environ_variable(variable_name):
    """
    Helper to grab Lambda's env vars

    :param variable_name: string, named of
    :return:
    """

    try:
        variable = os.environ[variable_name]
    except:
        print("Environment variable name not found")
        exit()

    return variable


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

    # interested_stocks_payload = []
    #
    # for stock in interested_stocks:
    #     interested_stocks_payload.append(
    #         {"symbol": stock}
    #     )

    return interested_stocks


def send_to_sqs(payload, queue_url):
    queue = boto3.client('sqs')

    payload_string = json.dumps(payload)

    response = queue.send_message(
        QueueUrl=queue_url,
        MessageBody=payload_string
    )

    print("Sent payload: %s" % payload_string)


def push_to_data_stream(payload, stream_name):
    print("Payload: %s" % (json.dumps(payload)))

    data_stream = boto3.client('kinesis')

    response = data_stream.put_record(
        StreamName=stream_name,
        Data=json.dumps(payload).encode('utf-8')+b'\n',
        PartitionKey="examplekey"
    )

    return response


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

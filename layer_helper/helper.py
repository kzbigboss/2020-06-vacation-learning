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


def main():
    pass


if __name__ == "__main__":
    main()

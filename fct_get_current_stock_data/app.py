import helper as h
import json
import requests
import boto3
import base64
from botocore.exceptions import ClientError
import datetime


def get_current_finnhub_quote(symbol):
    """
    Function to ping Finnhub.io and grab latest available stock
    quote data. See docs for response attributes:
    https://finnhub.io/docs/api#quote

    :param symbol: string, Stock symbol

    :return: dict, Finnhub.io content response
    """

    request_url = 'https://finnhub.io/api/v1/quote?'

    request_parameters = [
        'symbol=' + symbol,
        'token=' + h.get_finnhub_api_token()
    ]

    request_string = request_url + '&'.join(request_parameters)

    r = requests.get(request_string)

    return r.json()


def get_now_epoch_minute_rounded():
    """
    Return the epoch time rounding to the current minute.

    :return: int, epoch time
    """
    now = datetime.datetime.now()
    return int(datetime.datetime(now.year
                                 , now.month
                                 , now.day
                                 , now.hour
                                 , now.minute
                                 )
               .timestamp()
               )


def parse_sqs_messages(event):
    """
    Parse through expected SQS messages to parse bodies
        containing stock symbols to getcurrent quote data for.

    :param event: dict, AWS Lambda provided event payload

    :return messages: dict, parsed stock symbols
    """
    messages = []

    for message in event['Records']:
        message_dict = json.loads(message['body'])
        messages.append(message_dict)

    return messages


def prepare_payload(symbol, epoch_now, finnhub_data):
    """
    Add additional attributes to Finnhub data to
    help with downstream analytics

    :param symbol: string, symbol of stock
    :param epoch_now: int, epoch value as of the data capture datetime
    :param finnhub_data: dict, finnhub API response

    :return: dict, transformed finnhub API response
    """
    finnhub_data['capture_time'] = epoch_now  # TODO Should time be sourced from the finnhub response instead?
    finnhub_data['symbol'] = symbol

    return finnhub_data


def lambda_handler(event, context):
    # Prepare SQS message bodies for processing
    messages = parse_sqs_messages(event)

    # Process each SQS message body
    for message in messages:
        # Prepare variable values
        symbol = message['symbol']
        epoch_now = get_now_epoch_minute_rounded()

        # Pull latest stock quote via Finnhub
        finnhub_data = get_current_finnhub_quote(symbol)

        # Prepare payload for data stream
        payload = prepare_payload(
            symbol,
            epoch_now,
            finnhub_data
        )

        # Push payload into data stream
        response = h.push_to_data_stream(
            payload,
            h.get_environ_variable("finnhubdatastream")
        )

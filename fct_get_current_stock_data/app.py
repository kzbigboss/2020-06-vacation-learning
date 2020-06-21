import helper as h
import json
import requests
import boto3
import base64
from botocore.exceptions import ClientError
import datetime


def get_finnhub_api_token():
    """
    Pull Finnhub.io from AWS Secrets manager

    :return string, secret API token
    """

    # Modified from Secrets Manager Console page

    secret_name = "dev/token/finnhub"
    region_name = "us-west-2"

    session = boto3.session.Session()
    client = session.client(
        service_name='secretsmanager',
        region_name=region_name
    )

    secret = ''

    try:
        get_secret_value_response = client.get_secret_value(
            SecretId=secret_name
        )
    except ClientError as e:
        if e.response['Error']['Code'] == 'DecryptionFailureException':
            # Secrets Manager can't decrypt the protected secret text using the provided KMS key.
            # Deal with the exception here, and/or rethrow at your discretion.
            raise e
        elif e.response['Error']['Code'] == 'InternalServiceErrorException':
            # An error occurred on the server side.
            # Deal with the exception here, and/or rethrow at your discretion.
            raise e
        elif e.response['Error']['Code'] == 'InvalidParameterException':
            # You provided an invalid value for a parameter.
            # Deal with the exception here, and/or rethrow at your discretion.
            raise e
        elif e.response['Error']['Code'] == 'InvalidRequestException':
            # You provided a parameter value that is not valid for the current state of the resource.
            # Deal with the exception here, and/or rethrow at your discretion.
            raise e
        elif e.response['Error']['Code'] == 'ResourceNotFoundException':
            # We can't find the resource that you asked for.
            # Deal with the exception here, and/or rethrow at your discretion.
            raise e
    else:
        # Decrypts secret using the associated KMS CMK.
        # Depending on whether the secret is a string or binary, one of these fields will be populated.
        if 'SecretString' in get_secret_value_response:
            secret = get_secret_value_response['SecretString']
        else:
            secret = base64.b64decode(get_secret_value_response['SecretBinary'])

    return json.loads(secret)['token_finnhub']


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
        'token=' + get_finnhub_api_token()
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

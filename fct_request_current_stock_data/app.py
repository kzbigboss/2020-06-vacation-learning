"""
Function Purpose: Request a pull of current stock data
  for each interested stock symbol by submitting a
  payload to SQS.
"""


import os
import boto3
import json
import helper as h


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


# def get_interested_stocks():
#     interested_stocks = [
#         # Randomly selected tech stock stickers
#         'AMZN',
#         'AAPL',
#         'FB',
#         'GOOG',
#         'NFLX',
#         'TSLA',
#         'INTC',
#         'MSFT',
#         'AMD',
#         'WORK',
#         'ZM'
#     ]
#
#     interested_stocks_payload = []
#
#     for stock in interested_stocks:
#         interested_stocks_payload.append(
#             {"symbol": stock}
#         )
#
#     return random.sample(interested_stocks_payload, k=10)


def send_to_sqs(payload, queue_url):
    queue = boto3.client('sqs')

    payload_string = json.dumps(payload)

    response = queue.send_message(
        QueueUrl=queue_url,
        MessageBody=payload_string
    )

    print("Sent payload: %s" % payload_string)


def lambda_handler(event, context):
    # Get a list of stock payloads we want to request
    # a current stock quote for.
    interested_stock_payload = h.get_interested_stocks()
    print(interested_stock_payload)

    # # Pull the queue we are sending these payloads to.
    # queue_url = get_environ_variable("requeststockdataqueue")
    #
    # # For each stock payload, send to a queue that
    # # will process the request.
    # for stock_payload in interested_stock_payload:
    #     response = send_to_sqs(stock_payload, queue_url)

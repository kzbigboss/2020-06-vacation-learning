"""
Function Purpose: Request a pull of current stock data
  for each interested stock symbol by submitting a
  payload to SQS.
"""
import random
import helper as h


def prepare_stock_payload():
    interested_stocks = h.get_interested_stocks()

    interested_stocks_payload = []

    for stock in interested_stocks:
        interested_stocks_payload.append(
            {"symbol": stock}
        )

    # Pick a random sample of {Size - 1} of the interested_stocks
    random_sample = random.sample(interested_stocks_payload, k=len(interested_stocks_payload)-1)

    # Force that we always have the Amazon stock in our random sample
    amzn_stock = {"symbol": "AMZN"}
    if amzn_stock not in random_sample:
        random_sample.append(amzn_stock)

    return random_sample


def lambda_handler(event, context):
    # Get a list of stock payloads we want to request
    # a current stock quote for.
    interested_stock_payload = prepare_stock_payload()

    # Pull the queue we are sending these payloads to.
    queue_url = h.get_environ_variable("requeststockdataqueue")

    # For each stock payload, send to a queue that
    # will process the request.
    for stock_payload in interested_stock_payload:
        response = h.send_to_sqs(stock_payload, queue_url)
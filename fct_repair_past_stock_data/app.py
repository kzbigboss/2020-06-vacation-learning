import helper as h
import requests
import datetime


def get_past_finnhub_data(symbol, minute_start, minute_end):
    request_url = "https://finnhub.io/api/v1/stock/candle?"

    request_parameters = [
        'symbol=' + symbol,
        'resolution=1',  # hard coding resolution to per-minute frequency
        'from=' + str(minute_start),
        'to=' + str(minute_end),
        'token=' + h.get_finnhub_api_token()
    ]

    request_string = request_url + '&'.join(request_parameters)

    print("request string: {}".format(request_string))

    j = requests.get(request_string).json()

    j.pop("s")  # remove Finnhub's status response

    return j


def find_missing_data(past_data, interested_minutes):
    found_data = []

    if len(past_data) > 0:
        for i, timestamp in enumerate(past_data["t"]):
            if timestamp in interested_minutes:
                found_data.append({
                    "c": past_data["c"][i],
                    "h": past_data["h"][i],
                    "l": past_data["l"][i],
                    "o": past_data["o"][i],
                    "t": past_data["t"][i]
                })

    return found_data


def prepare_payloads(symbol, found_data):
    for d in found_data:
        d["symbol"] = symbol
        d["data_state"] = "repaired"


def perform_repair(symbol, interested_minutes, minute_start, minute_end):
    repairs = 0

    past_data = get_past_finnhub_data(symbol, minute_start, minute_end)

    found_data = find_missing_data(past_data, interested_minutes)

    prepare_payloads(symbol, found_data)

    for payload in found_data:
        response = h.push_to_data_stream(
            payload=payload,
            stream_name=h.get_environ_variable("repairstream")
        )
        repairs += 1

    return repairs


def lambda_handler(event, context):
    print(event)
    # Parse event for necessary variables
    missing_minutes_by_symbol = event["missing_minutes"]
    minute_start = event["range"]["minute_start"]
    minute_end = event["range"]["minute_end"]

    repairs = 0

    for symbol in missing_minutes_by_symbol:
        interested_minutes = missing_minutes_by_symbol[symbol]

        repairs += perform_repair(symbol, interested_minutes, minute_start, minute_end)

    return {
        "repairs_made": repairs,
        "latest_repair_attempt": datetime.datetime.now().timestamp()
    }

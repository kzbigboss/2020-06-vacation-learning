if __name__ == "__main__":
    import requests
    import random
    import pandas as pd
    import datetime

    # get the data
    symbol = "AAPL"
    resolution = "1"
    minute_start = "1593068400"
    minute_end = "1593154799"
    token = "brjs1f7rh5r9g3ot9oa0"

    url = "https://finnhub.io/api/v1/stock/candle?symbol=AAPL&resolution=1&from=1592827200&to=1592913600&token=brjs1f7rh5r9g3ot9oa0"

    result = requests.get(url)

    j = result.json()

    j.pop("s")

    # reduce to interested observations

    df = pd.DataFrame.from_dict(j, orient='index').transpose()
    df.t.astype

    # for each interested observation, prepare payload

    for i, timestamp in enumerate(j["t"]):
        if timestamp in interested_timestamps:
            data_chunk = {
                "symbol": symbol,
                "c": j["c"][i],
                "h": j["h"][i],
                "l": j["l"][i],
                "o": j["o"][i],
                "t": j["t"][i]
            }

            print(data_chunk)

    pass

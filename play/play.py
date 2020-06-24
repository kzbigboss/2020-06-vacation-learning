import json
import pandas as pd


if __name__ == '__main__':

    json_file = 'athena_query_response.json'

    with open(json_file) as f:
        query_response = json.load(f)

    clean_list = [[data.get('VarCharValue') for data in row['Data']]
                  for row in query_response['ResultSet']['Rows']]

    df = pd.DataFrame(clean_list[1:], columns=clean_list[0])

    missing_minutes = {}

    for i in df.index:
        symbol = df['symbols'][i]
        capture_minute = df['capture_minute'][i]

        if symbol not in missing_minutes:
            missing_minutes[symbol] = [capture_minute]
        else:
            missing_minutes[symbol].append(capture_minute)
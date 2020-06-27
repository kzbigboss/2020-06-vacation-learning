import helper as h


def lambda_handler(event, context):
    missing_minutes = event["missing_minutes"]

    for symbol in missing_minutes:
        failed_minutes = missing_minutes[symbol]

        for failed_minute in failed_minutes:
            payload = {
                "symbol": symbol,
                "t": failed_minute,
                "data_state": "failed"
            }

            response = h.push_to_data_stream(
                payload=payload,
                stream_name=h.get_environ_variable("failurestream")
            )

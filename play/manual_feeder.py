from datetime import datetime, timezone, timedelta

if __name__ == "__main__":
    start_datetime = datetime(2020, 6, 26, 0, 0, 0, tzinfo=timezone.utc)
    end_datetime = datetime(2020, 6, 26, 23, 0, 0, tzinfo=timezone.utc)
    step = timedelta(hours=1)

    while start_datetime <= end_datetime:
        delta = timedelta(minutes=59, seconds=59)

        payload = {
            "range": {
                "minute_start": int(start_datetime.timestamp()),
                "minute_end": int((start_datetime + delta).timestamp())
            }
        }

        print(payload)

        start_datetime += delta

import os
import boto3
import json
# import pandas as pd
import time


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
        Data=json.dumps(payload).encode('utf-8') + b'\n',
        PartitionKey="examplekey"
    )

    return response


def submit_athena_query(query, database, workgroup):
    athena_client = boto3.client('athena')

    response = athena_client.start_query_execution(
        QueryString=query,
        QueryExecutionContext={
            'Database': database
        },
        WorkGroup=workgroup
    )

    return response['QueryExecutionId']


def wait_for_athena_results(query_id):
    athena_client = boto3.client('athena')

    wait_state = ['QUEUED', 'RUNNING']

    fail_state = ['FAILED', 'CANCELLED']

    while True:
        # get the query's execution response
        query_execution = athena_client.get_query_execution(
            QueryExecutionId=query_id
        )

        # parse the execution response to find the current query status
        status = query_execution['QueryExecution']['Status']['State']
        print("Current status for {}: {}".format(query_id, status))

        # handle the current query status
        if status in wait_state:
            time.sleep(1)
        elif status in fail_state:
            print("QUERY FAILED")
            return None
        elif status == 'SUCCEEDED':
            return None


def get_athena_query_results(query_id):
    athena_client = boto3.client('athena')

    response = athena_client.get_query_results(
        QueryExecutionId=query_id
    )

    return response


# def parse_athena_results(query_data_dict):
#
#     results = {}
#
#     for row in query_data_dict['ResultSet']['Rows'][1:]:
#         values = []
#         for field in row['Data']:
#             try:
#                 values.append(list(field.values())[0])
#             except:
#                 values.append(list(' '))
#
#     print(values)
#
#     return 1


def submit_and_retrieve_athena_query(query, database, workgroup):
    query_id = submit_athena_query(query, database, workgroup)

    wait_for_athena_results(query_id)

    athena_results = get_athena_query_results(query_id)

    # data = parse_athena_results(athena_results)

    return athena_results


def main():
    pass


if __name__ == "__main__":
    main()

import os
import boto3
import json
import pandas as pd
import time
import base64
from botocore.exceptions import ClientError


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


def parse_missing_minutes(query_result):
    # Use list comprehension to walk through the Rows in ResultSet
    # and move the data into a list
    clean_list = [[data.get('VarCharValue') for data in row['Data']]
                  for row in query_result['ResultSet']['Rows']]

    # Transform the list into a data frame
    df = pd.DataFrame(clean_list[1:], columns=clean_list[0])

    # Establish a `missing_minutes` payload dictionary
    missing_minutes = {}

    # Walk through the data frame
    for i in df.index:
        # Set variables
        symbol = df['symbols'][i]
        capture_minute = int(df['capture_minute'][i])

        # if symbol isn't yet in `missing_minutes`, add it and the related capture_minute
        if symbol not in missing_minutes:
            missing_minutes[symbol] = [capture_minute]
        # append the capture minute to the related symbol
        else:
            missing_minutes[symbol].append(capture_minute)

    return missing_minutes


def get_query_from_athena(query, database, workgroup):
    query_id = submit_athena_query(query, database, workgroup)

    wait_for_athena_results(query_id)

    athena_results = get_athena_query_results(query_id)

    return athena_results


def get_job_repair_settings():
    return {
        "attempt_repair": True,
        "attempt_count": 0,
        "attempts_limit": 3
    }


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


def main():
    pass


if __name__ == "__main__":
    main()

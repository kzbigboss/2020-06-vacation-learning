import os
import boto3
import json

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


def build_stock_query(database, table):
    query = """
            SELECT COUNT(*) FROM "{}"."{}"
            """ \
        .format(database, table) \
        .strip()

    return query


def run_athena_query(query):
    athena_client = boto3.client("athena")

    response = athena_client.start_query_execution(
        QueryString=query,
        WorkGroup='primary'
    )

    return 1


def get_athena_result(qid):
    athena_client = boto3.client("athena")

    response = athena_client.get_query_results(
        QueryExecutionId=qid
    )

    response_json = json.dumps(response)

    pass

def lambda_handler(event, context):
    # database = get_environ_variable("datalake_database")
    # table = get_environ_variable("datalake_table_stock")

    # query = build_stock_query(database, table)

    # result = run_athena_query(query)

    get_athena_result("a2d67a1e-7aca-4168-b7ae-cab6b9bb8d73")

    # print(query)

import time
import stripe
from google.cloud import storage, bigquery
from airflow import DAG, models
from airflow.hooks.base import BaseHook
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.operators.dagrun_operator import TriggerDagRunOperator
from airflow.utils.trigger_rule import TriggerRule

STRIPE_ENDPOINTS = [
    {
        'task_name': 'subscriptions',
        'timestamp_sql': "SELECT MAX(CAST(created AS INT64)) FROM `{project}.{dataset}.subscriptions`",
        'endpoint': stripe.Subscription,
        'params': {'limit': 100, 'status': 'all', 'created': {'gte': 0}},
        'filename': 'subscriptions/subscriptions.json'
    },
    {
        'task_name': 'subscription_updates',
        'timestamp_sql': "SELECT MAX(CAST(created AS INT64)) FROM `{project}.{dataset}.subscription_updates`",
        'endpoint': stripe.Event,
        'params': {'type': 'customer.subscription.*', 'limit': 100, 'created': {'gte': 0}},
        'filename': 'subscription_updates/subscription_updates.json'
    },
    {
        'task_name': 'invoices',
        'timestamp_sql': "SELECT MAX(CAST(created AS INT64)) FROM `{project}.{dataset}.invoices`",
        'endpoint': stripe.Invoice,
        'params': {'limit': 100, 'status': 'paid', 'created': {'gte': 0}},
        'filename': 'invoices/invoices.json'
    }
]

# Get Stripe API key from Airflow connections
stripe_conn = BaseHook.get_connection('stripe_connection_data')
stripe.api_key = stripe_conn.password


def get_latest_timestamp(query_sql: str, project_id: str, dataset: str) -> int:
    """
    Query the target table for the latest record's corresponding timestamp (~ created),
    if no records are found, or the table does not exist, return 0.
    """
    bq_client = bigquery.Client()
    final_query = query_sql.format(project=project_id, dataset=dataset)
    default_value = 0

    try:
        last_timestamp_query = bq_client.query(final_query).result()
        last_timestamp = next(last_timestamp_query)[0]
    except Exception as e:
        print(f"Error querying timestamp: {e}")
        last_timestamp = default_value

    return last_timestamp or default_value


def pull_stripe_endpoint(endpoint, params: dict, max_retries: int = 50) -> list:
    """
    Make the call to the defined Stripe endpoint with auto pagination and return
    the results of the call as a list.
    This handles rate limit errors with exponential backoff
    """
    result = []

    iterator = endpoint.list(**params).auto_paging_iter()

    while True:
        retry_count = 0

        while retry_count <= max_retries:
            try:
                item = next(iterator)
                result.append(item)
                break

            ## capture the end of the iterator
            except StopIteration:
                print(f"Successfully collected {len(result)} items")
                return result

            ## in case we hit the rate limit
            ## wait with a backoff strategy
            except stripe.error.RateLimitError as e:
                retry_count += 1

                if retry_count > max_retries:
                    print(f"Rate limit exceeded after {max_retries} retries.")
                    raise

                delay = 2 ** (retry_count - 1)
                print(f"Rate limit hit during pagination. Retry {retry_count}/{max_retries} after {delay}s delay.")
                time.sleep(delay)
            except Exception as e:
                print(f"Unexpected error: {e}.")
                raise


def write_to_bucket(bucket_name: str, blob_name: str, list_of_json_rows: list) -> None:
    """
    Writes data to a Google Storage bucket. Please note: Big Query expects
    Newline-delimited JSON!
    """
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(blob_name)

    with blob.open("w") as f:
        for i in list_of_json_rows:
            f.write(i + '\n')


def sync_stripe(config: dict, project_id: str, dataset: str, bucket_name: str) -> None:
    """
    The main function that syncs data from Stripe to GCS.
    """

    last_timestamp = get_latest_timestamp(
        config['timestamp_sql'],
        project_id,
        dataset
    )

    params = config['params'].copy()
    params['created'] = {'gte': last_timestamp}

    raw_data = pull_stripe_endpoint(config['endpoint'], params)

    if raw_data:
        write_to_bucket(bucket_name, config['filename'], raw_data)
    else:
        print(f"No new data. for {config['task_name']}")


with models.DAG(
        "stripe_sync_data",
        schedule_interval="30 1 * * *",
        default_args={
            'owner': 'airflow',
            "catchup": False,
        },
        max_active_tasks=1,
        catchup=False
) as dag:
    begin = EmptyOperator(task_id='begin_stripe_data_sync')

    separate_endpoint_calls = []
    for endpoint_config in STRIPE_ENDPOINTS:
        separate_endpoint_calls.append(
            PythonOperator(
                task_id=endpoint_config['task_name'],
                python_callable=sync_stripe,
                op_kwargs={
                    'timestamp_sql': endpoint_config['timestamp_sql'],
                    'params': endpoint_config['params'],
                    'endpoint': endpoint_config['endpoint'],
                    'filename': endpoint_config['filename']
                }
            )
        )

    ## After load all data to GCS, trigger the update DAG
    ## to load data to BQ from the external tables
    trigger_update = TriggerDagRunOperator(
        trigger_dag_id="stripe_update_data",
        task_id="trigger_update_stripe_data",
        trigger_rule=TriggerRule.ALL_SUCCESS,
    )

    begin >> separate_endpoint_calls >> trigger_update

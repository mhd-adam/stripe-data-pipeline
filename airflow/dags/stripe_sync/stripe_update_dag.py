from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator

from composer_utils.utils import error_callback_func

with DAG(
        dag_id='stripe_update_data',
        default_args={
            'owner': 'airflow',
            "catchup": False
            ## other args
        },
        description='Trigger by stripe_sync_data',
        schedule_interval=None,
) as dag:
    start = DummyOperator(task_id="start")

    ## this is a demo command
    ## like `cd /opt/airflow/dbt/ && dbt run --models staging.*`
    ## replace according to the deployment style
    update_staging_data = BashOperator(
        task_id='update_staging_data',
        bash_command='<COMMAND TO RUN DBT IN THE TARGET ENVIRONMENT>',
        on_failure_callback=error_callback_func,
    )

    update_curated_data = BashOperator(
        task_id='update_curated_data',
        bash_command='<COMMAND TO RUN DBT IN THE TARGET ENVIRONMENT>',
        on_failure_callback=error_callback_func,
    )

    update_marts_data = BashOperator(
        task_id='update_marts_data',
        bash_command='<COMMAND TO RUN DBT IN THE TARGET ENVIRONMENT>',
        on_failure_callback=error_callback_func,
    )

    end = DummyOperator(task_id="end")

    start >> update_staging_data >> update_curated_data >> update_marts_data >> end

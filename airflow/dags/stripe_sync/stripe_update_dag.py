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
    ## like `cd /opt/airflow/dbt && dbt run`
    ## replace according to the deployment style
    update_stripe_data = BashOperator(
        task_id='update_data',
        bash_command='<COMMAND TO RUN DBT IN THE TARGET ENVIRONMENT>',
        on_failure_callback=error_callback_func,
    )
    end = DummyOperator(task_id="end")

    start >> update_stripe_data >> end

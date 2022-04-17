from openlineage.airflow import DAG
from airflow.operators.postgres_operator import PostgresOperator
from airflow.utils.dates import days_ago
from great_expectations_provider.operators.great_expectations import (
    GreatExpectationsOperator,
)

default_args = {
    "owner": "datascience",
    "depends_on_past": False,
    "start_date": days_ago(1),
    "email_on_failure": False,
    "email_on_retry": False,
    "email": ["datascience@example.com"],
}

dag = DAG(
    "sum_orders",
    schedule_interval="*/5 * * * *",
    catchup=False,
    is_paused_upon_creation=False,
    default_args=default_args,
    description="DAG that sums the total of ingested orders values.",
)

t1 = PostgresOperator(
    task_id="if_not_exists",
    postgres_conn_id="example_db",
    sql="""
    CREATE TABLE IF NOT EXISTS summed_orders (
      value INTEGER
    );""",
    dag=dag,
)

t2 = PostgresOperator(
    task_id="total_sum",
    postgres_conn_id="example_db",
    sql="""
    INSERT INTO summed_orders (value)
        SELECT SUM(orders.value) FROM raw_orders AS orders;
    """,
    dag=dag,
)

t1 >> t2

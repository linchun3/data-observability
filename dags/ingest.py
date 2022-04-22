import random

from airflow import DAG
from airflow.operators.postgres_operator import PostgresOperator
from airflow.utils.dates import days_ago

default_args = {
    "owner": "datascience",
    "depends_on_past": False,
    "start_date": days_ago(1),
    "email_on_failure": False,
    "email_on_retry": False,
    "email": ["datascience@example.com"],
}

dag = DAG(
    "ingest_orders",
    schedule_interval="*/1 * * * *",
    catchup=False,
    is_paused_upon_creation=False,
    default_args=default_args,
    description="DAG that ingests orders from source system.",
)

t1 = PostgresOperator(
    task_id="if_not_exists",
    postgres_conn_id="example_db",
    sql="""
    CREATE TABLE IF NOT EXISTS raw_orders (
      id SERIAL PRIMARY KEY,
      value INTEGER
    );""",
    dag=dag,
)

t2 = PostgresOperator(
    task_id="ingest_orders",
    postgres_conn_id="example_db",
    sql="""
    INSERT INTO raw_orders (value)
         VALUES (%(value)s)
    """,
    parameters={"value": random.randint(1, 10)},
    dag=dag,
)

t1 >> t2

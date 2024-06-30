from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.utils.dates import days_ago

default_args = {
    'owner': 'airflow',
    'retries': 1,
}

with DAG(
    'example_datalake_dag',
    default_args=default_args,
    description='A simple DAG interacting with the Data Lake',
    schedule_interval='0 * * * *',
    start_date=days_ago(1),
    tags=['example'],
) as dag:

    create_datalake_table = PostgresOperator(
        task_id='create_datalake_table',
        postgres_conn_id='datalake_postgres',
        sql="""
        CREATE TABLE IF NOT EXISTS datalake_table (
            id SERIAL PRIMARY KEY,
            data VARCHAR(50)
        );
        """
    )

    insert_into_datalake = PostgresOperator(
        task_id='insert_into_datalake',
        postgres_conn_id='datalake_postgres',
        sql="""
        INSERT INTO datalake_table (data) VALUES
        ('example_data');
        """
    )

    create_datalake_table >> insert_into_datalake

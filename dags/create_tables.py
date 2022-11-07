from datetime import datetime
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.decorators import task, dag
from dags.conf import DEFAULT_ARGS



@dag(dag_id='create_tables',
     schedule_interval='@once',
     default_args=DEFAULT_ARGS,
     tags=['project'],
     start_date=datetime(2022,9,1,0,0,0)
     )
def CreateTables():
    create_tables = PostgresOperator(
        task_id='create_tables',
        postgres_conn_id='project_pg',
        sql="""
        CREATE TABLE IF NOT EXISTS arxiv (title varchar NOT NULL, doi varchar);
        """,
        autocommit=True,
    )
    create_tables

dag = CreateTables()

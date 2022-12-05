from datetime import datetime
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.decorators import task, dag
from conf import DEFAULT_ARGS

# Drops existing dimension tables and fact tables and creates new ones (once).

@dag(dag_id='create_tables',
     schedule_interval='@once',
     default_args=DEFAULT_ARGS,
     tags=['project'],
     start_date=datetime(2022,9,1,0,0,0)
     )
def CreateTables():
    drop_tables_task = PostgresOperator(
        task_id='drop_tables_task',
        postgres_conn_id='project_pg',
        sql='sql/drop_tables.sql',
        autocommit=True,
    )
    create_tables_task = PostgresOperator(
        task_id='create_tables_task',
        postgres_conn_id='project_pg',
        sql='sql/create_tables.sql',
        autocommit=True,
    )
    drop_tables_task >> create_tables_task

dag = CreateTables()
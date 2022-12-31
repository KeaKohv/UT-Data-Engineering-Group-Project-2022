from datetime import datetime
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.decorators import task, dag
from conf import DEFAULT_ARGS

# This is a DAG that:
# Drops existing dimension tables and fact tables and creates new ones (once).
# Drops existing staging tables and creates new ones (once).

@dag(dag_id='create_tables',
     schedule_interval='@once',
     default_args=DEFAULT_ARGS,
     tags=['project'],
     start_date=datetime(2022,9,1,0,0,0)
     )
def CreateTables():

    DWH_tables_task = PostgresOperator(
        task_id='create_tables_task',
        postgres_conn_id='project_pg',
        sql='sql/DWH_tables.sql',
        autocommit=True,
    )

    staging_tables_task = PostgresOperator(
        task_id='staging_tables_task',
        postgres_conn_id='project_pg',
        sql='sql/staging_tables.sql',
        autocommit=True,
    )
    
    DWH_tables_task >> staging_tables_task

dag = CreateTables()
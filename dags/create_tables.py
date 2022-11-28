from datetime import datetime
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.decorators import task, dag
from conf import DEFAULT_ARGS



@dag(dag_id='create_tables',
     schedule_interval='@once',
     default_args=DEFAULT_ARGS,
     tags=['project'],
     start_date=datetime(2022,9,1,0,0,0)
     )
def CreateTables(): # VAJAB UUENDAMIST, võiks olla üldsegi enne andmebaasi sisestamist
    create_tables = PostgresOperator(
        task_id='create_tables',
        postgres_conn_id='project_pg',
        sql="""
        CREATE TABLE IF NOT EXISTS arxiv (doi varchar,
                                          arxiv_id varchar NOT NULL UNIQUE,
                                          title varchar NOT NULL, 
                                          latest_version_nr varchar NOT NULL,
                                          author_id varchar NOT NULL);

        CREATE TABLE IF NOT EXISTS arxiv_authors (author_id varchar NOT NULL UNIQUE,
                                          first_name varchar NOT NULL, 
                                          last_name varchar NOT NULL,
                                          affiliations varchar);
        """,
        autocommit=True,
    )
    create_tables

dag = CreateTables()
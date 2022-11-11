# This DAG is designated for populating the DWH

import os
from datetime import datetime

from airflow.decorators import task, dag
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.sensors.filesystem import FileSensor

from conf import DEFAULT_ARGS, DATA_FOLDER, ARXIV_FILE_NAME, ARXIV_AUTHORS_FILE_NAME


@dag(
    dag_id='csv_to_db',
    schedule_interval='*/1 * * * *',
    start_date=datetime(2022,9,1,0,0,0),
    catchup=False,
    tags=['project'],
    template_searchpath=DATA_FOLDER,
    default_args=DEFAULT_ARGS
)

def CSVToDB():

    @task(task_id='csvs_to_db')
    def csv_to_db(folder, input_file_main, input_file_authors):
        postgres_hook = PostgresHook(postgres_conn_id="project_pg")
        conn = postgres_hook.get_conn()
        cur = conn.cursor()
        with open(os.path.join(folder, input_file_main), "r") as file:
            cur.copy_expert(
                "COPY arxiv FROM STDIN WITH CSV HEADER DELIMITER AS ',' QUOTE '\"'",
                file,
            )
        with open(os.path.join(folder, input_file_authors), "r") as file:
            cur.copy_expert(
                "COPY arxiv_authors FROM STDIN WITH CSV HEADER DELIMITER AS ',' QUOTE '\"'",
                file,
            )
        conn.commit()
        os.remove(os.path.join(folder, input_file_main))
        os.remove(os.path.join(folder, input_file_authors))

    waiting_for_main_cvs = FileSensor(
    task_id="check__main_file", 
    filepath=ARXIV_FILE_NAME,
    fs_conn_id='file_sensor_connection',
    poke_interval=5,
    timeout=100,
    exponential_backoff=True,
    )

    waiting_for_authors_cvs = FileSensor(
    task_id="check__authors_file", 
    filepath=ARXIV_AUTHORS_FILE_NAME,
    fs_conn_id='file_sensor_connection',
    poke_interval=5,
    timeout=100,
    exponential_backoff=True,
    )

    waiting_for_main_cvs >> waiting_for_authors_cvs >> csv_to_db(folder=DATA_FOLDER, input_file_main=ARXIV_FILE_NAME, input_file_authors = ARXIV_AUTHORS_FILE_NAME)

dag = CSVToDB()
import requests
import os
import orjson
from datetime import datetime
import pandas as pd

from airflow.decorators import task, dag
from airflow.providers.postgres.hooks.postgres import PostgresHook

from dags.conf import DEFAULT_ARGS, API_URL, DATA_FOLDER, ARXIV_FILE_NAME
from dags.transforms import clean_dataframe

from enrich import enrich


@dag(
    dag_id='api_to_db',
    schedule_interval='*/1 * * * *',
    start_date=datetime(2022,9,1,0,0,0),
    catchup=False,
    tags=['project'],
    template_searchpath=DATA_FOLDER,
    default_args=DEFAULT_ARGS
)

def ApiToDB():
    @task(task_id = 'fetch_and_clean')
    def fetch_and_clean(url, folder, file):
        # Get data from API
        r = requests.get(url=url)
        json: [dict] = orjson.loads(r.content)
        df = pd.json_normalize(json, ["result"])

        df = clean_dataframe(df)
        df.to_csv(os.path.join(folder, file), index=False)

    @task(task_id = 'enrich')
    def enrich(url, folder, file):
        df = pd.read_csv(os.path.join(folder, file))
        df = enrich(df)
        df.to_csv(os.path.join(folder, file), index=False)

    @task(task_id='csv_to_db')
    def csv_to_db(folder, input_file):
        postgres_hook = PostgresHook(postgres_conn_id="project_pg")
        conn = postgres_hook.get_conn()
        cur = conn.cursor()
        with open('/tmp/data/arxiv.csv', "r") as file:
            cur.copy_expert(
                "COPY arxiv FROM STDIN WITH CSV HEADER DELIMITER AS ',' QUOTE '\"'",
                file,
            )
        conn.commit()
        os.remove(os.path.join(folder, input_file))

    fetch_and_clean(url=API_URL, params={}, folder=DATA_FOLDER, file=ARXIV_FILE_NAME) >> enrich(url=API_URL, folder=DATA_FOLDER, file=ARXIV_FILE_NAME)

dag = ApiToDB()
import requests 
import os
from datetime import datetime
import pandas as pd

from airflow.decorators import task, dag
from airflow.providers.postgres.hooks.postgres import PostgresHook

from dags.conf import DEFAULT_ARGS, API_URL, DATA_FOLDER, ARXIV_FILE_NAME


def transform(dataframe):
    # Drop abstract
    df = dataframe.drop(['abstract'], axis=1)
    return df

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
    @task(task_id = 'fetch_data')
    def fetch_data(url, params, folder, file):
        # Get data from API
        r = requests.get(url=url)

        # Convert json to dataframe
        j = r.json()
        df = pd.json_normalize(j, ["result"])

        df = transform(df)

        # Save dataframe to csv
        df[['title', 'doi']].to_csv(os.path.join(folder, file), index=False)

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

    fetch_data(url=API_URL, params={}, folder=DATA_FOLDER, file=ARXIV_FILE_NAME) >> csv_to_db(folder=DATA_FOLDER, input_file=ARXIV_FILE_NAME)

dag = ApiToDB()
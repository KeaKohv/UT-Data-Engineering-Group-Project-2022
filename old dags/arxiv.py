import requests
import re
import os
from datetime import datetime, timedelta
import json
import pandas as pd
import csv

from airflow import DAG 
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator 
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.sensors.filesystem import FileSensor
from airflow.operators.sql import BranchSQLOperator

#### General pipeline: ###
# Take first 200k from the big json file and break into 50K splits
# https://plainenglish.io/blog/split-big-json-file-into-small-splits
# reqdoc:'You can select, e.g., 200K records. Furthermore, split them into partitions e.g.,
# 50K each and then feed them on iterations to the pipeline' - kuidas seda teha?

# Read the arxiv json file and transform it
# Save the transformed data into csv file
# Enrich the data using Apache Avro (?)
# Load the data into postgres tables and Graph DB


###################
# The following should be run on Postgres beforehand.
# We create a new database, and in the new database, create new tables.
# Note: you may need to open a new connection to the newly created database before creating the tables.
#
# CREATE DATABASE project;
# 
# CREATE TABLE arxiv (
# 	id serial primary key,
# 	title text not null,
# 	doi text not null,
# 	created_at timestamp not null
# );
#
#
# The following should be done in Airflow UI beforehand.
# Admin->Connections->Add a new record
#
# Connection Id: file_sensor_connection
# Connection Type: File (path)
# Extra: {"path": "/tmp/data/"}
# 
# Add a new record
#
# Connection Id: project_pg
# Connection Type: Postgres
# Host: postgres
# Schema: project
# Username: airflow
# Password: airflow
# Port: 5432


DEFAULT_ARGS = {
    'owner': 'Project',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

API_URL = 'http://65.108.50.112:5000/data/arxiv'
API_PARAMS = {
    'results': 50,
    'format': 'csv',
    'inc': 'title,doi'
}

DATA_FOLDER = '/tmp/data'
ARXIV_FILE_NAME = 'arxiv.csv'
SQL_FILE_NAME = 'insert_arxiv.sql'


# FIRST DAG: Read data from API, transform and save into cvs

arxiv_generator_dag = DAG(
    dag_id='arxiv_generator',
    schedule_interval='* * * * *',
    start_date=datetime(2022,9,1,0,0,0),
    catchup=False,
    tags=['project'],
    template_searchpath=DATA_FOLDER,
    default_args=DEFAULT_ARGS
)


def remove_withdrawn_articles(dataframe: pd.DataFrame) -> pd.DataFrame:
    p = re.compile('\s+(This|The) (paper|submission|manuscript) (has been|is being|is) withdrawn')
    not_withdrawn = dataframe['abstract'].apply(p.match).isnull()
    return dataframe.loc[not_withdrawn]


def transform(dataframe):
    
    # Drop abstract
    df = dataframe.drop(['abstract'], axis=1)
    return df


def arxiv_generator_method(url, params, folder, file):

    # Get data from API
    r = requests.get(url=url, params=params)

    # Convert json to dataframe
    j = r.json()
    df = pd.json_normalize(j, ["result"])

    df = transform(df)

    # Save dataframe to csv
    df.to_csv(os.path.join(folder, file), index=False)


arxiv_task = PythonOperator(
    task_id='fetch_arxiv_data',
    dag=arxiv_generator_dag,
    trigger_rule='none_failed',
    python_callable=arxiv_generator_method,
    op_kwargs={
        'url': API_URL,
        'params': API_PARAMS,
        'folder': DATA_FOLDER,
        'file': ARXIV_FILE_NAME,
    }
)

# SECOND DAG: Read data from CSV and commit to postgres (only title and doi atm), delete the CSV

arxiv_postgres_dag = DAG(
    dag_id='cvs_to_postgres',
    schedule_interval='* * * * *',
    start_date=datetime(2022,9,1,0,0,0),
    catchup=False,
    tags=['project'],
    template_searchpath=DATA_FOLDER,
    default_args=DEFAULT_ARGS
)

waiting_for_arxiv = FileSensor(
    task_id='waiting_for_arxiv_csv',
    dag=arxiv_postgres_dag,
    filepath=ARXIV_FILE_NAME,
    fs_conn_id='file_sensor_connection',
    poke_interval=5,
    timeout=100,
    exponential_backoff=True,
)


def prepare_insert(folder, input_file, output_file):

    df = pd.read_csv(os.path.join(folder,input_file))  
    
    sql_statement = """INSERT INTO arxiv
    (title, doi, created_at)
    """

    for idx, row in df.iterrows():
        if idx==0:
            continue
        if idx>1:
            sql_statement += 'UNION ALL\n'

        sql_statement += f"""
            SELECT '{row['title']}' as title
            , '{row['doi']}' as doi
            , CURRENT_TIMESTAMP
            """
    
    with open(os.path.join(folder,output_file), 'w') as output_f:
        output_f.writelines(sql_statement)

    os.remove(os.path.join(folder,input_file))


prep_sql_task = PythonOperator(
    task_id='prepare_insert_statement',
    dag=arxiv_postgres_dag,
    trigger_rule='none_failed',
    python_callable=prepare_insert,
    op_kwargs={
        'folder': DATA_FOLDER,
        'input_file': ARXIV_FILE_NAME,
        'output_file': SQL_FILE_NAME,
    },
) 

waiting_for_arxiv >> prep_sql_task


sql_insert_task = PostgresOperator(
    task_id='insert_to_db',
    dag=arxiv_postgres_dag,
    postgres_conn_id='project_pg',
    sql=SQL_FILE_NAME,
    trigger_rule='none_failed',
    autocommit=True,
)

prep_sql_task >> sql_insert_task


### VANA ###
# def get_metadata():
#     CUR_DIR = os.path.abspath(os.path.dirname(__file__))
#     lines = 50
#     with open(f"{CUR_DIR}/arxiv-metadata-oai-snapshot.json", 'r') as f:
#         for line in f:
#             if lines == 0:
#                 break
#             lines -= 1
#             yield line

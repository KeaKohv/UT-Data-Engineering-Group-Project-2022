import requests
import os
from datetime import datetime
import pandas as pd
import numpy as np

from airflow.decorators import task, dag
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.neo4j.operators.neo4j import Neo4jOperator
from airflow.providers.neo4j.hooks.neo4j import Neo4jHook


from transforms import clean_dataframe
from conf import DEFAULT_ARGS, API_URL, DATA_FOLDER, ARXIV_FILE_NAME

from enrich import enrich

def clean_title(t):
    return t.replace('\\', '\\\\').replace('"', '\\"')

def row_to_neo4j(r):
    queries = []
    title = clean_title(r['title'])
    piece_properties = "{title: \"" + title + "\""
    if not np.isnan(r['published-year']):
        piece_properties += f""", year: {int(r['published-year'])}"""
    piece_properties += "}"
    piece = f"""MERGE (:Piece {piece_properties})"""
    queries.append(piece)

    q = f"MATCH (p:Piece {piece_properties}) MERGE (a: Author "
    for author in r['authors_merged']:
        author_properties = f'{{ family: "{author["family"]}", given: "{author["given"]}" }}'
        queries.append(q + f'{author_properties})-[:AUTHORS]->(p);')

    if r['reference'] is None:
        return queries

    # references
    for ref in r['reference']:
        if ref['title'] is None:
            continue
        ref_title = clean_title(ref['title'])
        ref_properties = "{title: \"" + ref_title + "\""
        if not np.isnan(r['published-year']):
            ref_properties += f""", year: {int(r['published-year'])}"""
        ref_properties += "}"
        q = f"MATCH (p:Piece {piece_properties}) MERGE (p)-[:REFERENCES]->(r: Piece {ref_properties})"
        queries.append(q)

    return queries



def neo4j_query():
    with open(os.path.join(DATA_FOLDER, 'neo4j.cyp'), 'r') as f:
        return ''.join(f.readlines())


@dag(
    dag_id='api_to_db',
    schedule_interval='*/2 * * * *',
    start_date=datetime(2022,9,1,0,0,0),
    catchup=False,
    tags=['project'],
    template_searchpath=DATA_FOLDER,
    default_args=DEFAULT_ARGS
)
def ApiToDB():
    @task(task_id = 'fetch_and_clean')
    def fetch_and_clean(url, folder, file, **kwargs):
        # Get data from API
        r = requests.get(url=url)
        df = pd.json_normalize(r.json(), ["result"])

        df = clean_dataframe(df)
        df.to_json(os.path.join(folder, file))

    @task(task_id = 'enrich_data')
    def enrich_data(folder, input_file, output_file, **kwargs):
        df = pd.read_json(os.path.join(folder, input_file))
        df = enrich(df)
        df.to_json(os.path.join(folder, output_file))

    @task(task_id = 'json_to_neo4j_query')
    def json_to_neo4j_query(folder, input_file, output_file, **kwargs):
        df = pd.read_json(os.path.join(folder, input_file))
        neo4j_hook = Neo4jHook(conn_id='neo4j')
        for i, r in df.iterrows():
            queries = row_to_neo4j(r)
            for q in queries:
                print(q)
                neo4j_hook.run(q)


    @task(task_id='csv_to_db')
    def csv_to_db(folder, input_file, **kwargs):
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

    fetch_and_clean(url=API_URL, params={}, folder=DATA_FOLDER, file=ARXIV_FILE_NAME) >> \
    enrich_data(folder=DATA_FOLDER, input_file=ARXIV_FILE_NAME, output_file=ARXIV_FILE_NAME) >> \
    json_to_neo4j_query(folder=DATA_FOLDER, input_file=ARXIV_FILE_NAME, output_file=ARXIV_FILE_NAME)

dag = ApiToDB()
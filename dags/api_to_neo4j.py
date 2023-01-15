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
from conf import DEFAULT_ARGS, API_URL, DATA_FOLDER, ARXIV_FILE_NAME, MAIN_FILE_NAME, AUTHORS_FILE_NAME

from enrich import enrich

def clean_title(t):
    if t is None:
        return 'UNKNOWN'
    return t.replace('\\', '\\\\').replace('"', '\\"')

def row_to_neo4j(r):
    print(r)
    queries = []
    title = clean_title(r['title'])
    piece_properties = "{title: \"" + title + "\""
    if not np.isnan(r['published-year']):
        piece_properties += f""", year: {int(r['published-year'])}"""

    piece_properties += f", subject: \"{r['subject']}\""
    piece_properties += "}"
    piece = f"""MERGE (p:Piece {piece_properties})"""
    queries.append(piece)

    venue = f"{{title: \"{clean_title(r['container-title'])}\", publisher: \"{clean_title(r['publisher'])}\", type: \"{clean_title(r['type'])}\"}}"

    q = f"MERGE (v:Venue {venue})"
    queries.append(q)
    q = f"MERGE (v)-[:PUBLICATION]-(p)"
    queries.append(q)


    for i, author in enumerate(r['authors_merged']):
        queries.append(
            f'MERGE (a{i}:Author {{family: \"{author["family"]}\", given: \"{author["given"]}\" }})'
        )
        queries.append(
            f'MERGE (a{i})-[:AUTHORSHIP]-(p)'
        )

        if author["affiliation"] is not None and len(author["affiliation"]):
            print(author["affiliation"])
            queries.append(
                f'MERGE (i{i}:Institution {{name: \"{author["affiliation"]}\"}})'
            )
            queries.append(
               f'MERGE (a{i})-[:AFFILIATION]-(i{i})'
            )

    if r['reference'] is None:
        return queries

    # references
    for i, ref in enumerate(r['reference']):
        if ref['title'] is None:
            continue
        ref_title = clean_title(ref['title'])
        ref_properties = "{title: \"" + ref_title + "\""
        if not np.isnan(r['published-year']):
            ref_properties += f""", year: {int(r['published-year'])}"""
        ref_properties += "}"

        queries.append(
            f"MERGE (r{i}: Piece {ref_properties})"
        )
        queries.append(
            f"MERGE (p)-[:REFERENCES]->(r{i})"
        )

    queries = ['\n'.join(queries)]

    return queries


def neo4j_query():
    with open(os.path.join(DATA_FOLDER, 'neo4j.cyp'), 'r') as f:
        return ''.join(f.readlines())


@dag(
    dag_id='api_to_neo4j',
    schedule_interval='*/1 * * * *',
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
        success, failure = enrich(df)
        success.to_json(os.path.join(folder, output_file))
        with open(os.path.join(folder, 'failures.json'), 'a') as f:
            if len(failure):
                string = failure.to_json(None)
                f.writelines(string)
                f.write('\n')

    @task(task_id = 'prepare_for_staging')
    def prepare_for_staging(folder, input_file, output_file_main, output_file_authors, **kwargs):
        df = pd.read_json(os.path.join(folder, input_file))
        df = df.replace("'", "\'")

        authors_df = df[['id','authors_merged']].copy()
        explded = authors_df.explode("authors_merged")
        authors_df_normalized = pd.json_normalize(explded['authors_merged'])
        authors_df_normalized['id'] = explded['id'].tolist()
        authors_df_normalized = authors_df_normalized.fillna(value="Unknown")
        authors_df_normalized.replace(to_replace="None", value="Unknown", inplace=True)

        for i, row in authors_df_normalized.iterrows():
            if type(row['affiliation']) == list:
                row['affiliation'] = row['affiliation'][0]

        # Creates df_main.csv and df_authors.csv that will be used for data staging and DWH insertion
        df[['published-year','subject','type','container-title','publisher','id','doi','title','versions','is-referenced-by-count']].to_csv(os.path.join(folder, output_file_main), index = False)
        authors_df_normalized.to_csv(os.path.join(folder, output_file_authors), index = False)

    @task(task_id = 'json_to_neo4j_query')
    def json_to_neo4j_query(folder, input_file, output_file, **kwargs):
        df = pd.read_json(os.path.join(folder, input_file))
        neo4j_hook = Neo4jHook(conn_id='neo4j')
        for i, r in df.iterrows():
            queries = row_to_neo4j(r)
            for q in queries:
                print(q)
                neo4j_hook.run(q)

    fetch_and_clean(url=API_URL, params={}, folder=DATA_FOLDER, file=ARXIV_FILE_NAME) >> \
    enrich_data(folder=DATA_FOLDER, input_file=ARXIV_FILE_NAME, output_file=ARXIV_FILE_NAME) >> \
    prepare_for_staging(folder=DATA_FOLDER, input_file=ARXIV_FILE_NAME, output_file_main=MAIN_FILE_NAME, output_file_authors=AUTHORS_FILE_NAME) >> \
    json_to_neo4j_query(folder=DATA_FOLDER, input_file=ARXIV_FILE_NAME, output_file=ARXIV_FILE_NAME)

dag = ApiToDB()
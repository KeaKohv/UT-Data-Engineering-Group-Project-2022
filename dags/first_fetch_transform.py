# This DAG is designated for fetching the ArXiv data from an API, transforming it and saving to CSV

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


import requests 
import os
from datetime import datetime
from operator import itemgetter
import pandas as pd
import numpy as np
import re

from airflow.decorators import task, dag
from airflow.providers.postgres.hooks.postgres import PostgresHook

from conf import DEFAULT_ARGS, API_URL, DATA_FOLDER, ARXIV_FILE_NAME, ARXIV_AUTHORS_FILE_NAME



def affiliations(authors_parsed):
    '''
    Takes the 'authors_parsed' field and returns a list of the paper authors' affiliations.
    '''
    affiliations = []

    for p in authors_parsed:
        try:
            end = p.index("")
            affiliations.append(list(filter(len, p[end:])))
        except ValueError:
            affiliations.append([])
    return affiliations


def authors_to_df(authors_parsed_list):
    '''
    Takes the entire 'authors_parsed' column and returns a dataframe with authors' ids, first names and
    last names. Each row per paper, not exploded.
    '''
    first_names = []
    last_names = []
    ids = []
    
    for line in authors_parsed_list:
        first_name = [e[1] for e in line]
        last_name = [e[0] for e in line]
        id = [e[0] + "_" + e[1] + "_" for e in line]
        id = [i.replace(' ' ,'_') for i in id]
        first_names.append(first_name)
        last_names.append(last_name)
        ids.append(id)

    df = pd.DataFrame()
    df['author_id'] = ids
    df['first_name'] = first_names
    df['last_name'] = last_names

    return df


def transform(df):
    '''
    Performs the necessary transformations to prepare the data for the enrichment phase.
    '''
    # Dropping the license, report-no, comments, journal-ref, submitter, categories
    df.drop(['license', 'report-no', 'comments', 'journal-ref', 'submitter', 'categories'],
                   axis=1, inplace=True)

    # Rename the id column to arxiv_id
    df = df.rename(columns={"id": "arxiv_id"})

    # Dropping withdrawn papers using information from the ‘abstract’ column after 
    # which ‘abstract’ column is dropped.
    p = re.compile('\s+(This|The) (paper|submission|manuscript) (has been|is being|is) withdrawn')
    df = df.loc[df['abstract'].apply(p.match).isnull()]
    df.drop(['abstract'], axis=1, inplace=True)

    # Dropping papers with empty authors
    df.dropna(subset=['authors'])

    # Removing records where both title and authors are duplicates.
    # Prefer to keep the records with DOIs; if no DOIs, prefer newer update dates.
    # After duplicates are removed, drop the authors and update_date columns as they are no longer needed.
    df = df.sort_values(by=['doi','update_date']).drop_duplicates(subset=(['title','authors']), 
                                                                  keep='last')
    df.drop(['authors'], axis=1, inplace=True)
    df.drop(['update_date'], axis=1, inplace=True)

    # Getting the latest version number from versions field and dropping the original ‘versions’ column
    df['latest_version'] = df['versions'].apply(itemgetter(-1))
    df['latest_version_nr'] = [d.get('version') for d in df.latest_version]
    df.drop(['versions'], axis=1, inplace=True)
    df.drop(['latest_version'], axis=1, inplace=True)

    # Separating authors and authors’ affiliations from the authors_parsed field into a new dataframe
    authors = authors_to_df(df['authors_parsed'])
    authors['affiliations'] = df['authors_parsed'].map(affiliations).values

    # Add author_ids into the original dataframe and drop 'authors_parsed' column
    df['author_id'] = authors['author_id'].values
    df.drop(['authors_parsed'], axis=1, inplace=True)

    # Explode the authors table so that there is one row per author
    authors = authors.apply(pd.Series.explode)

    # Returns main dataframe that has columns 'doi', 'arxiv_id', 'title', 'latest_version_nr', 'author_id'.
    # The main dataframe is not exploded. The 'author_id' field has a list of values.
    # Authors dataframe has columns 'author_id', 'first_name', 'last_name', 'affiliations'.
    # Authors dataframe is exploded, it has one row per author.
    # 'first_name' field can contain initial(s) with dot(s) instead of a first name.
    # 'affiliations' column is mostly empty because affiliations are rarely included in the dataset.
    return df, authors

@dag(
    dag_id='fetch_and_transform',
    schedule_interval='*/1 * * * *',
    start_date=datetime(2022,9,1,0,0,0),
    catchup=False,
    tags=['project'],
    template_searchpath=DATA_FOLDER,
    default_args=DEFAULT_ARGS
)
def FetchAndTransform():
    @task(task_id = 'fetch_transform')
    def fetch_transform(url, params, folder, main_file, authors_file):
        # Get data from API
        r = requests.get(url=url)

        # Convert json to dataframe
        j = r.json()
        df = pd.json_normalize(j, ["result"])

        # Transformations
        main_df, authors_df = transform(df)

        # Save dataframe to csv
        main_df.to_csv(os.path.join(folder, main_file), index=False)
        authors_df.to_csv(os.path.join(folder, authors_file), index=False)

    fetch_transform(url=API_URL, params={}, folder=DATA_FOLDER, main_file=ARXIV_FILE_NAME, authors_file = ARXIV_AUTHORS_FILE_NAME)

dag = FetchAndTransform()
import requests
import os
from datetime import datetime
from metrics import hindex, gindex
import pandas as pd
import numpy as np

from airflow.decorators import task, dag
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.neo4j.operators.neo4j import Neo4jOperator
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.providers.neo4j.hooks.neo4j import Neo4jHook

from conf import DEFAULT_ARGS, API_URL, DATA_FOLDER, ARXIV_FILE_NAME
   
   
@dag(
    dag_id='data_staging_and_DWH_insert',
    schedule_interval='*/10 * * * *',
    start_date=datetime(2022,9,1,0,0,0),
    catchup=False,
    tags=['project'],
    template_searchpath=DATA_FOLDER,
    default_args=DEFAULT_ARGS
)
def StageAndDWH():

    # Wait for the tables DAG to finish successfully - ei tööta miskipärast.
    # wait_for_create_tables = ExternalTaskSensor(
    #     task_id="wait_for_create_tables",
    #     external_dag_id="create_tables",
    #     external_task_id = "staging_tables_task",
    #     poke_interval=3,
    #     timeout=180,
    #     soft_fail=False,
    #     retries=4,
    #     check_existence = True,
    # )


   # Data staging
    @task(task_id = 'json_to_staging_tables')
    def json_to_staging(folder, input_file, output_file, **kwargs):

        # Clean staging tables
        sql_statement = """DELETE FROM staging_main;
                           DELETE FROM staging_authors;
                           DELETE FROM staging_affiliations;
        """
        postgres_hook = PostgresHook(postgres_conn_id="project_pg")
        conn = postgres_hook.get_conn()
        cur = conn.cursor()
        cur.execute(sql_statement)
        conn.commit()

        #df = pd.read_json(os.path.join(folder, input_file))
        
        # Mock data
        df = pd.read_csv(os.path.join(folder, 'mock_main.csv'))

        # Insert data into main staging table

        sql_statement = """INSERT INTO staging_main
        (publication_year, scientific_domain, type_name, pub_venue, publisher, arxiv_ID, doi, title, latest_version_nr, citaton_count)
        """

        for idx, row in df.iterrows():

            if idx!=0:
                sql_statement += 'UNION ALL\n'

            sql_statement += f"""
                SELECT CAST('{row['published-year']}' AS INT) as publication_year
                , '{row['subject']}' as scientific_domain
                , '{row['type']}' as type_name
                , '{row['container-title']}' as pub_venue
                , '{row['publisher']}' as publisher
                , '{row['id']}' as arxiv_ID
                , '{row['doi']}' as doi
                , '{row['title']}' as title
                , '{row['versions']}' as latest_version_nr
                , CAST('{row['is-referenced-by-count']}' AS INT) as citaton_count
                """
    
        cur.execute(sql_statement)

        # Prepare data for authors staging table

        # authors_df = df[['id','authors_merged']].copy()
        # explded = authors_df.explode("authors_merged")
        # authors_df_normalized = pd.json_normalize(explded['authors_merged'])
        # authors_df_normalized['id'] = explded['id'].tolist()


        # Mock data
        authors_df_normalized = pd.read_csv((os.path.join(folder, 'mock_authors.csv')))

        # Insert data into authors staging table

        sql_statement = """INSERT INTO staging_authors
        (arxiv_ID, full_name, gender)
        """

        for idx, row in authors_df_normalized.iterrows():

            if idx!=0:
                sql_statement += 'UNION ALL\n'

            sql_statement += f"""
                SELECT '{row['id']}' as arxiv_ID
                , '{row['full_name']}' as full_name
                , '{row['gender']}' as gender
                """

        cur.execute(sql_statement)

        # Insert data into affiliations staging table
        affiliation_df = authors_df_normalized[['id','affiliation']].copy()
        
        sql_statement = """INSERT INTO staging_affiliations
        (arxiv_ID, affiliation_name)
        """

        for idx, row in affiliation_df.iterrows():

            if idx!=0:
                sql_statement += 'UNION ALL\n'

            sql_statement += f"""
                SELECT '{row['id']}' as arxiv_ID
                , '{row['affiliation']}' as affiliation_name
                """
    
        cur.execute(sql_statement)
        conn.commit()


    #### Data insertion to DWH ###
    @task(task_id = 'from_staging_to_DWH_tables')
    def staging_to_tables(**kwargs):

        postgres_hook = PostgresHook(postgres_conn_id="project_pg")
        conn = postgres_hook.get_conn()
        cur = conn.cursor()

        # Insert data into the dimensions and store the keys in the staging table for creation of the fact table rows
        # Then create the fact table, author group bridge and affiliations group bridge
        sql_statement = """
                           UPDATE staging_main 
                           SET publication_year_key = (
                                SELECT year_key
                                FROM dim_year
                                WHERE staging_main.publication_year = dim_year.publication_year
                            );
        
                           INSERT INTO dim_domain (scientific_domain)
                           SELECT DISTINCT scientific_domain FROM staging_main
                           ON CONFLICT DO NOTHING;

                           UPDATE staging_main 
                           SET scientific_domain_key = (
                                SELECT domain_key
                                FROM dim_domain
                                WHERE staging_main.scientific_domain = dim_domain.scientific_domain
                            );

                           INSERT INTO dim_type (type_name)
                           SELECT DISTINCT type_name FROM staging_main
                           ON CONFLICT DO NOTHING;

                           UPDATE staging_main 
                           SET type_key = (
                                SELECT type_key
                                FROM dim_type
                                WHERE staging_main.type_name = dim_type.type_name
                            );

                           INSERT INTO dim_venue (pub_venue, publisher)
                           SELECT DISTINCT pub_venue, publisher FROM staging_main
                           ON CONFLICT DO NOTHING;

                           UPDATE staging_main 
                           SET venue_key = (
                                SELECT venue_key
                                FROM dim_venue
                                WHERE staging_main.pub_venue = dim_venue.pub_venue AND staging_main.publisher = dim_venue.publisher
                            );

                           INSERT INTO dim_author (full_name, gender)
                           SELECT DISTINCT full_name, gender FROM staging_authors
                           WHERE NOT EXISTS (SELECT full_name from dim_author
                                             WHERE dim_author.full_name = staging_authors.full_name);

                           UPDATE staging_authors 
                           SET author_key = (
                                SELECT author_key
                                FROM dim_author
                                WHERE staging_authors.full_name = dim_author.full_name
                            );
                            
                           INSERT INTO dim_affiliation (affiliation_name)
                           SELECT DISTINCT affiliation_name FROM staging_affiliations
                           WHERE NOT EXISTS (SELECT affiliation_name from dim_affiliation
                                             WHERE dim_affiliation.affiliation_name = staging_affiliations.affiliation_name);

                           UPDATE staging_affiliations
                           SET affiliation_key = (
                                SELECT affiliation_key
                                FROM dim_affiliation
                                WHERE staging_affiliations.affiliation_name = dim_affiliation.affiliation_name
                           );

                           INSERT INTO paper_fact (year_key
                                                   , domain_key
                                                   , type_key
                                                   , venue_key
                                                   , arxiv_ID
                                                   , doi
                                                   , title
                                                   , latest_version_nr
                                                   , citaton_count)
                           SELECT publication_year_key
                                  , scientific_domain_key
                                  , type_key
                                  , venue_key
                                  , arxiv_ID
                                  , doi
                                  , title
                                  , latest_version_nr
                                  , citaton_count
                                  FROM staging_main
                           ON CONFLICT DO NOTHING;

                           UPDATE staging_authors 
                           SET author_group_key = (
                                SELECT author_group_key
                                FROM paper_fact
                                WHERE staging_authors.arxiv_ID = paper_fact.arxiv_ID
                            );

                           INSERT INTO bridge_author_group (author_group_key, author_key)
                           SELECT DISTINCT author_group_key, author_key FROM staging_authors
                           ON CONFLICT DO NOTHING;

                           UPDATE staging_affiliations
                           SET affiliation_group_key = (
                                SELECT affiliation_group_key
                                FROM paper_fact
                                WHERE staging_affiliations.arxiv_ID = paper_fact.arxiv_ID
                            );

                           INSERT INTO bridge_affiliation_group (affiliation_group_key, affiliation_key)
                           SELECT DISTINCT affiliation_group_key, affiliation_key FROM staging_affiliations
                           ON CONFLICT DO NOTHING;


        """

        cur.execute(sql_statement)
        conn.commit()


        ### Update h-index and g-index for the authors of the added papers ###
    @task(task_id = 'update_h_and_g_index')
    def update_h_and_g_index(**kwargs):

        postgres_hook = PostgresHook(postgres_conn_id="project_pg")
        conn = postgres_hook.get_conn()
        cur = conn.cursor()

        # Get the paper citation counts for all authors that are in the staging_authors
        sql_statement = """SELECT full_name FROM staging_authors;"""
        cur.execute(sql_statement)
        full_names = [r[0] for r in cur.fetchall()]

        for name in full_names:
            sql_statement = f"""SELECT paper_fact.citaton_count FROM paper_fact
                               WHERE author_group_key IN (
                                SELECT author_group_key FROM bridge_author_group
                                WHERE author_key IN (
                                    SELECT author_key FROM dim_author
                                    WHERE full_name = CAST('{name}' as varchar)
                                ));
            """
        
            cur.execute(sql_statement)
            citation_counts = [r[0] for r in cur.fetchall()]
            print(f'{name} papers citation counts: {citation_counts}')

            h_index = hindex(np.array(citation_counts))
            g_index = gindex(np.array(citation_counts))

            sql_statement = f""" 
                           UPDATE dim_author
                           SET h_index = {h_index}, g_index = {g_index}
                           WHERE full_name = CAST('{name}' as varchar);
            """

            cur.execute(sql_statement)

        conn.commit()


    # Vaja lisada wait for tables ja wait for json file/enrichment.
    json_to_staging(folder=DATA_FOLDER, input_file=ARXIV_FILE_NAME, output_file=ARXIV_FILE_NAME) >> \
    staging_to_tables() >> update_h_and_g_index()

dag = StageAndDWH()
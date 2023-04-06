# 1. Introduction

This is a group project for the Data Engineering Course UT 2022. The ArXiv dataset, consisting of metadata about research papers in physics, mathematics, computer science, quantitative biology, quantitative finance, statistics, electrical engineering and systems science, and economics, is available at Kaggle [1].

This README describes the Data Warehouse (DWH) schema, data update policy, Business Intelligence (BI) queries that the schema answers, Graph Database (DB) nodes, edges, queries, and analytics. In addition, the different processing steps of the ETL (Extract, Transform, and Load) pipeline are described, and a guide with commands on starting the pipeline is included. Finally, query examples for the DWH and Graph DB are given. In this project, the pipeline DAGs (Directed Acyclic Graphs) are implemented using Airflow. The DWH is set up using Postgres. The Graph DB is implemented using Neo4j. 

# 2. Data Warehouse

The DWH, built using Postgres, follows the technique of dimensional modelling. We use the star schema to store the data about research papers.

## 2.1 Schema

This is the DWH schema:

![image](https://user-images.githubusercontent.com/55859977/230319496-8dc9fd13-817e-4fbd-917c-3b7481b77905.png)

Figure 1.

There is a Paper Fact table which holds facts and metrics (citation count) about the research paper. In the fact table, there are foreign keys (FK-s) for the 6 dimensions. These foreign keys are for the publication year, scientific domain, paper type (e.g. article or book), publishing venue, author group, and authors’ affiliations group.

The authors and affiliations are linked to the fact table using bridge tables as one paper can have multiple authors and multiple affiliations (per paper). Holding all these in the fact table would result in an overly large fact table.

## 2.2 Updates policy

The updates to the DWH are based on Type 1 slowly changing dimensions. The existing data is overwritten with the new value. History is not maintained because the old values hold no significance in this project. It is also simpler and faster than Type 2 and will keep the dimension tables from growing too big. Therefore, the information in the fact table and dimensions tables is current.

Citation counts are added to the fact table at the time of insertion of the paper into the DWH. Updates to the authors’ h-index and g-index are made each time new papers are inserted into the DWH, updates are done only for the authors of the newly inserted papers.

## 2.3 Business Intelligence Queries

These are the Business Intelligence (BI) queries that DWH schema answers:
*	ranking authors in a scientific domain
    *	by the total number of papers;
    *	by the total number of citations of their papers;
    *	by H-index;
    *	by G-index;
*	ranking papers by citation count
*	ranking affiliations:
    *	by the total number of papers from affiliated authors;
    *	by the number of papers published this century by affiliated authors;
    *	by the total number of paper citations from affiliated authors;
    *	by the average number of citations per author.
*	ranking publication venues:
    *	by the total number of published papers;
    *	by the total number of paper citations;
    *	by the average number of citations per paper;
    *	by the number of published math papers to see the top math publication venues;
*	finding years with the most published papers,
*	histograms for the number of papers on a given scientific domain over a given time period (years).

# 3. Graph view
In the Neo4j graph database there is the following graph view:
*	Nodes:
    *	authors;
    *	papers;
    *	publication venues;
    *	affiliations.
*	Edges:
    *	authorship between an author and a paper;
    *	works-for between author and affiliation;
    *	cites relationship among papers;
    *	paper published under a publication venue.
*	Labels:
    *	Scientific domain
    *	Year

In this project, labels are preferred over node properties due to faster querying.

Graph queries and analytics:
*	Finding influential papers using PageRank and the citation relation;
*	Finding influential authors using ArticleRank;
*	Finding influential papers with Betweenness Centrality;
*	Finding similar papers by analysing co-citation;
*	Detecting communities by finding strongly connected components in:
    *	co-authorship relationships, e.g. researcher pairs who have published together most papers;
    *	being a member of the same scientific domain;
    *	having the same affiliations;
    *	authors citing each other.

# 4. Processing steps of the ETL pipeline

In this Chapter, the general overview of the ETL pipeline and more detailed descriptions about each processing step are given.

## 4.1.	General overview
The below scheme illustrates the general flow of the pipeline. 

![image](https://user-images.githubusercontent.com/55859977/230321632-c6e99459-2ff8-4152-b4b9-b801ab0933bb.png)
 
Figure 2. General process

The pipeline takes ArXiv data as input from an API which is set up for this project’s purposes. The API displays metadata of 50 papers at once, changing each time data is requested from the API.

After the records in json format have been fetched, the data is transformed and enriched. After enrichment is done, the data is inserted into the Graph DB Neo4j and into the DWH. The papers that cannot be found by Crossref and OpenAlex API-s are written into a separate file ‘failures.json’.

Postgres tables are created once at the start of the pipeline before any data is inserted into the DWH.

This is achieved with 3 Airflow DAGs. In the following subchapters, the Airflow DAGs and tasks are described in more detail.

## 4.2.	DAGs and tasks

There are three DAGs.

![image](https://user-images.githubusercontent.com/55859977/230321738-f4c3d440-3c11-4a1b-b4b6-ad0fbca2af7b.png)

Figure 3. The “tables” DAG graph view

The first DAG “tables” creates Postgres tables. It consists of two tasks:
1) create_tables_task which drops (if they exist) and creates dimension and fact tables. The year dimension table is also populated in this task;
2) staging_tables_task which drops (if they exist) and creates data staging tables.

![image](https://user-images.githubusercontent.com/55859977/230321782-67d8938a-6b6a-47ca-9c1e-f8f429babea1.png)

Figure 4. The “api_to_neo4j” DAG graph view

The second DAG “api_to_neo4j” fetches data from the API, transforms and enriches the data, prepares the DWH CSV-s, and loads data into Neo4j. It consists of 4 tasks:
1.	fetch_and_clean requests data from the API, receives it in json format, transforms the data and saves it in json format. Json format was picked over csv as it simplifies enrichment in the next task. The transformations are described in more detail under Chapter 5.3.
2.	enrich_data reads in the json file with transformed data, enriches it and saves it again as a json file by overwriting it. The papers that could not be found by Crossref and OpenAlex (the two main enrichment sources) are written into a separate file, called ‘failures.json’. The enrichments are described in more detail under Chapter 5.4.
3.	json_to_neo4j_query takes the enriched json file as input and loads the data into Neo4j. The Neo4j data import logic is described under Chapter 5.6.
4.	prepare_for_staging reads in the enriched json file, separates the per author data (e.g. author full_name, gender) from the per paper data (e.g. title, venue) and outputs two csv files that are ready for Postgres data staging. In the enriched json file, there was one record per paper, including authors’ data as a list of dictionaries. In the output main csv file, there is one row per paper. In the output authors csv file, there is one row per author. The DWH data import logic is described in more detail under Chapter 5.5.

![image](https://user-images.githubusercontent.com/55859977/230322000-449ec57e-4fac-448f-8b49-ceb689c27915.png)

Figure 5. The “DWH_staging_and_insert” DAG graph view

The third DAG “DWH_staging_and_insert” loads the data into Postgres. It uses 5 tasks. These are:
1)	waiting_for_main_csv is a file sensor that waits for the enriched main csv (paper data) file to appear in the data folder. This is necessary because without the csv files, the third task would fail.
2)	waiting_for_authors_csv is a file sensor that waits for the enriched authors csv (author data) file to appear in the data folder.
3)	json_to_staging is dedicated to loading the data into Postgres staging tables. First it deletes any existing data from the staging tables. Then it escapes some characters (e.g. single quote) and inserts the enriched data from the csv files into the Postgres staging tables. There are 3 staging tables: the main staging table, the authors data staging table and the affiliations data staging table. The connection of an author and an affiliation to a paper is maintained by the ArxivID business key. The DWH load logic is described in more detail under Chapter 5.6.
4)	staging_to_tables inserts the data from the 3 staging tables to the DWH dimension tables, gathers the foreign keys and creates a fact table row for each paper. A value is inserted to a dimension table only if the value does not exist there already. A dimension table does not have any duplicate values. Staging table stores the primary key of the inserted (or already existing) dimension table value. New fact table rows are created after all the foreign keys for the dimension table values have been gathered. The author group key and affiliation group key are created with the fact table row, gathered to the staging tables and then inserted into the author group bridge table and the affiliation group bridge table.
5)	update_h_and_g_index calculates and updates the authors h-index and g-index in the DWH author dimension. The updates are done for only the authors who wrote the papers that were added to the DWH in this iteration. This update logic is described in more detail under Chapter 5.6. This task also deletes the 2 CSV files.

## 4.3.	Transformations

The task responsible for data transformations is fetch_and_clean in the DAG “api_to_neo4j” (see Chapter 5.2). The functions that transform the data are imported into the DAG file from transforms.py which is located also in the dags folder.

The input for the transformations is the raw data from the API. The transformed data output is in json format and it is stored in the data folder. In this format, the later querying of Crossref is easier.

The below scheme illustrates the data transformation process.

![image](https://user-images.githubusercontent.com/55859977/230322252-be833859-7f28-4344-9ff9-cdc23a57e304.png)

Figure 6. Data transformation steps

The transformation steps in more detail are the following:
1)	Regex is used to find paper records where the abstract contains information that the paper has actually been withdrawn. The regex is: “(This|The) (paper|submission|manuscript|work) (has been|is being|is) withdrawn')”. Only not withdrawn papers are kept in the pandas dataframe.
2)	The dataframe columns 'comments', 'abstract', 'license', 'update_date', 'report-no' are dropped as they are not needed.
3)	Using itemgetter, the paper’s latest version number is extracted from the ‘version’ field and the ‘version’ column is overwritten with the new value.
4)	Duplicates (same title and authors) are removed.
5)	The ‘/n’ is removed from the ‘title’, ‘authors’ and ‘journal-ref’ fields.
6)	Authors’ given name, family name and affiliation are extracted from the ‘authors_parsed’ field. The field is overwritten with these extracted values in a dictionary form to make later Crossref and OpenAlex querying easier.

An example of data before transformations:
![image](https://user-images.githubusercontent.com/55859977/230322314-0d0ce73c-0c40-47d5-8fa1-92a67cf11bf1.png)

An example of data after transformations:
![image](https://user-images.githubusercontent.com/55859977/230322343-adfc0ad8-0367-4ac1-89eb-180c87ca8034.png)

An example of ‘authors_parsed’ field after transformations:

![image](https://user-images.githubusercontent.com/55859977/230322405-2b7232b3-2465-4b4c-aed0-f7c629f849df.png)

Dropping one-word titles was suggested in the project requirements document but it was not included as a transformation because there seems no reason to drop these records. The fact that a paper has a one-word title does not decrease its value.

## 4.4.	Enrichment
The following enrichment sources are used:
* OpenAlex API [2] and Crossref API [3] for title, doi, scientific domain (subject), authors names, publication venue and publisher names, the type of publication, publication year, references and citations count;
* Gender_guesser package [4] for finding the authors’ gender.

We tried other enrichment sources (e.g. scholarly and Google Scholar, AMiner Gender API), as well. However, other enrichment sources that we considered had either the problem of being not free, too slow or prone to blocking.

The task responsible for data enrichment is enrich_data in the DAG “api_to_neo4j” (see Chapter 5.2). The functions that enrich the data are imported into the DAG file from enrich.py which is located also in the dags folder. Enrich.py also uses functions from openalex.py located in the dags folder.

The input to the enrichment task is the transformed data in a json format. The output of the enrichment task is enriched data in json format. The json file is overwritten.

The below scheme illustrates the enrichment steps.

![image](https://user-images.githubusercontent.com/55859977/230322562-01ffa503-cfef-441d-beee-8dd62b54b556.png)

Figure 7. Enrichment steps

The enrichment steps in more detail are the following:
1)	First OpenAlex API query is made using doi or title if the doi is missing. OpenAlex is used to extract information about the papers title, doi, type, publishing year and month, publication venue and publisher, authors’ names and affiliations, citation counts, scientific domain and references.
2)	If OpenAlex cannot find the paper, a similar query is made to Crossref API.
3)	The papers that could not be found by the OpenAlex and Crossref APIs are written to a file failures.json.
4)	The enriched data that could be gathered from OpenAlex or Crossref is merged with the original data.
5)	The gender_guesser package is used to determine the authors’ gender. This package was preferred over API-s because it is much faster. The gender_guesser returns a guess: unknown/female/male/mostly_female/mostly_male/androgynous. However, for simplicity’s sake, mostly_female modified to be female, mostly_male to be male and androgynous as unknown.
6)	Enriched data is written to a json file overwriting the previous json file that had the transformed data.

Continuing the same example given under the previous subchapter for the transformations, this is an example of data after enrichment:
![image](https://user-images.githubusercontent.com/55859977/230322635-a6485023-e328-466e-b11b-9b788ec5eaa0.png)

 
As can be seen from this example, the data now has fields that were not there before, e.g. information about the type, publisher, publication venue (container-title), citations count (is-referenced-by-count), scientific domain (subject), published year, references and addition information about the author in the field ‘authors_merged’.

This is an example of the enriched authors’ data:
![image](https://user-images.githubusercontent.com/55859977/230322660-1f4228e1-e031-423b-bc41-d1c01075f95d.png)
 
If the original dataset contains no information about a specific field and no information is found using enrichment, this field value is treated as ‘unknown’. For example, the authors’ affiliations field for the specific paper is most difficult to determine and is sometimes left as ‘unknown’.

## 4.5.	Data Warehouse

In Chapter 2, the DWH schema, updates policy and BI queries were described. Here, a more detailed overview is given about loading the enriched data into the DWH. This includes a description of how the enriched data is processed to finally land in the dimension tables and the fact table.

The below scheme illustrates how the data is loaded into the DWH at each iteration:

![image](https://user-images.githubusercontent.com/55859977/230323007-6a1177cd-7757-44ef-83e2-870f9a7c4187.png)

Figure 7. Loading data into the DWH

The Postgres staging tables, dimension tables and the fact table are created using a separate DAG ‘tables’. There the year dimension is also populated using (generate_series(1980,2030)). The value 0 is also inserted for papers where the year cannot be determined.

These steps in more detail are the following:
1)	In the DAG “api_to_neo4j” the task prepare_for_staging takes in the enriched data in json format, separates the ArxivID and the merged authors’ data from the main dataframe (df) to a new authors df, normalises the authors df, and saves the main df and authors df in the csv format. All the next steps will take place using another DAG, the DWH_staging_and_insert DAG.
2)	The DAG DWH_staging_and_insert waits for the 2 CSV files with the enriched data to appear in the data folder.
3)	If the CSV files have appeared, the Postgres staging tables are emptied of existing data and populated with the new data. The affiliations data is separated from the authors data while the affiliations’ connection to the paper is kept. Before inserting data into Postgres, some modifications to the data are made, e.g. escaping single quotes, treating null values. There are 3 staging tables: main (for paper data), authors (for per author data), affiliations (for affiliations data). Each has the ArxivID to maintain the connection to the paper.
4)	At first, the staging tables have null values for the dimension values’ keys. The dimension values from the staging tables are inserted into the dimension tables if they do not exist there already. Then the new or existing dimension values’ keys’ are fetched from the dimension tables and stored in the staging tables.

An example of filled main staging table:
![image](https://user-images.githubusercontent.com/55859977/230323086-0e6b2c24-a095-48cb-93e6-8bd8afe172c5.png)


5)	The new fact table records are created from the main staging table.
6)	The newly generated authors group key and the affiliations group key are fetched from the added fact table rows and stored in the authors staging table and the affiliations staging table.

An example of a filled authors staging table:
![image](https://user-images.githubusercontent.com/55859977/230323121-78522e85-01ea-4352-b0f5-d2bad8d3c2b7.png)

An example of a filled affiliations staging table (as many rows as there are authors):
![image](https://user-images.githubusercontent.com/55859977/230323175-7c9e3794-632c-4e13-b78a-87ac3246c579.png)

7)	Insert new rows to the authors bridge table and the affiliations bridge table using the author key and author group key stored in the authors staging table (see steps 4 and 6) and the affiliation key and affiliations group key stored in the affiliations staging table.
8)	Update the h-index and g-index of authors whose papers were just added to the DWH. For this, the DWH is queried for the papers’ citation counts of all the authors in the authors staging table. The citation counts are the citation counts of all the papers the author has written that are in the DWH. The new h-index and g-index are calculated using the scholarmetrics package metrics.py and updates are made in the author dimension.
9)	The CSV files are deleted as the DWH loading is now complete for this iteration.

An example of the fact table records:
![image](https://user-images.githubusercontent.com/55859977/230323264-bd74b87e-234c-4948-948a-17316312fb06.png)

An example of the publishing year dimension rows:

![image](https://user-images.githubusercontent.com/55859977/230323319-478278ad-d371-461d-9937-3a734082f9db.png)
 
An example of the publication venue dimension rows:

![image](https://user-images.githubusercontent.com/55859977/230323367-316ae59f-64cb-40ef-91af-301b55eb2790.png)

An example of the author dimension rows:

![image](https://user-images.githubusercontent.com/55859977/230323425-cc66b754-15c3-40bb-82e5-71dae60436d4.png)

An example of the author group bridge rows:

![image](https://user-images.githubusercontent.com/55859977/230323468-8e1d9773-2d2a-4ebb-a239-fd536bb68f0d.png)

An example of the affiliation dimension rows:

![image](https://user-images.githubusercontent.com/55859977/230323505-5ee97812-c0ca-4dc0-826b-df6e9b6df2c9.png)

An example of the affiliation group bridge table rows:

![image](https://user-images.githubusercontent.com/55859977/230323545-ae1f7042-81ce-4e5e-b529-352fe8ef7c55.png)


## 4.6.	Neo4J

The main processing steps for Neo4J involve turning the enriched data into Neo4j queries, after which the queries can be used to insert data into the graph database.
As this work is largely combining together different strings, we will present as an example 1 paper and its references turned into Neo4j queries. We believe this is easier to understand compared to a detailed description of each of the steps.

MERGE (p:Piece {title: "Graph manifolds, left-orderability and amalgamation", year: 2013, subject: "Geometry and Topology"})

MERGE (v:Venue {title: "Algebraic &amp; Geometric Topology", publisher: "Mathematical Sciences Publishers", type: "journal-article"})

MERGE (v)-[:PUBLICATION]-(p)

MERGE (a0:Author {family: "Clay", given: "Adam" })

MERGE (a0)-[:AUTHORSHIP]-(p)

MERGE (a1:Author {family: "Pinsky", given: "Tali" })

MERGE (a1)-[:AUTHORSHIP]-(p)

MERGE (r0: Piece {title: "Spherical space forms and Dehn filling", year: 2013})

MERGE (p)-[:REFERENCES]->(r0)

MERGE (r1: Piece {title: "Conjugacy in lattice-ordered groups and right ordered groups", year: 2013})

MERGE (p)-[:REFERENCES]->(r1)

MERGE (r2: Piece {title: "Word problems, embeddings, and free products of right-ordered groups with amalgamated subgroup", year: 2013})

MERGE (p)-[:REFERENCES]->(r2)

MERGE (r3: Piece {title: "On L-spaces and left-orderable fundamental groups", year: 2013})

MERGE (p)-[:REFERENCES]->(r3)

MERGE (r4: Piece {title: "Orderable 3-manifold groups", year: 2013})

MERGE (p)-[:REFERENCES]->(r4)

MERGE (r5: Piece {title: "Exceptional surgery on knots", year: 2013})

MERGE (p)-[:REFERENCES]->(r5)

MERGE (r6: Piece {title: "Laminations and groups of homeomorphisms of the circle", year: 2013})

MERGE (p)-[:REFERENCES]->(r6)

MERGE (r7: Piece {title: "Right orderability and graphs of groups", year: 2013})

MERGE (p)-[:REFERENCES]->(r7)

MERGE (r8: Piece {title: "Non-Left-Orderable 3-Manifold Groups", year: 2013})

MERGE (p)-[:REFERENCES]->(r8)

# 5.	Guide and commands on starting the pipeline
## 5.1.	Preparations

These are the preparation steps:

1)	Make sure Docker Desktop is installed and running. CLI docs: https://docs.docker.com/engine/reference/commandline/cli/
2)	Download this project’s repository. In the command prompt navigate to the downloaded project’s directory and run docker compose up -d to start up all the services in detached mode.
3)	After all services are started, Airflow UI should be available on localhost port 8080, Postgres UI should be available on localhost port 5432 and Neo4j UI should be available on localhost port 7474.
4)	In Airflow UI, to log in, the username is airflow and password is airflow. In Postgres Admin, the username is airflow and password is airflow. In Neo4j UI, the username is neo4j and password is password.
5)	In Postgres Admin, a new server should be added. For this, add a new server with your preferred name, host postgres, username airflow, password airflow.
6)	In Neo4j UI, a server connection needs to be established by triggering :server connect. Use bolt://localhost:7687, username neo4j, password password.

## 5.2.	Running Airflow

In Airflow UI, the connections should be set up first and then the DAG-s can be activated. In the following two subchapters, the Airflow connections set-up and the DAG activation order is described.

### 5.2.1.	Airflow connections

The following connections should be set up in Airflow UI beforehand.

Admin->Connections->Add a new record

Connection Id: file_sensor_connection
Connection Type: File (path)
Extra: {"path": "/tmp/data/"}

Admin->Connections->Add a new record

Connection Id: project_pg
Connection Type: Postgres
Host: postgres
Schema: project
Login: airflow
Password: airflow
Port: 5432

Admin->Connections->Add a new record
Connection Id: neo4j
Connection Type: Neo4j
Host: neo4j
Login: neo4j
Password: password
Port: 7687

### 5.2.2.	DAG activation

After the connections have been added, the DAGs can be activated in the following order:
1)	First, activate the “tables” DAG. This DAG runs once and creates the Postgres tables. This DAG needs to run successfully before the other two DAGs can be activated;
2)	Second, activate the other two DAGs.

## 5.3.	Clean-up

To stop and remove the containers run:
docker compose down

# 6.	Query examples

In this Chapter, examples in the form of screenshots are given for the graph view and DWH view queries. The queries were listed above in Chapters 2.3 and 3.

## 6.1.	DWH view

Below are examples of these queries run in Postgres Admin using real data from the pipeline.

Examples:
*	Ranking authors in a scientific domain (Physics in this case) by the total number of papers;
![image](https://user-images.githubusercontent.com/55859977/230323959-c5f462f2-9c70-40e8-9603-eae0e3f9ea4f.png)

*	ranking authors in a scientific domain (Physics in this case) by the total number of citations of their papers;
![image](https://user-images.githubusercontent.com/55859977/230323998-1ff4c5a2-2e8f-41c2-91f4-3451cb46a20b.png)

*	ranking authors in a scientific domain (Physics in this case) by H-index;
 ![image](https://user-images.githubusercontent.com/55859977/230324053-4ae8101a-8677-4a33-ab81-99f4922c39df.png)

*	ranking authors in a scientific domain (Physics in this case) by G-index;
![image](https://user-images.githubusercontent.com/55859977/230324106-25304344-f490-4010-98ad-0793d38a234f.png)

*	ranking papers by citation count
![image](https://user-images.githubusercontent.com/55859977/230324163-eff66272-dc66-4f29-bfd1-14a1991c110e.png)

*	ranking affiliations by the total number of papers from affiliated authors;
![image](https://user-images.githubusercontent.com/55859977/230324210-d693fd28-bb1a-4e1a-aefc-0196f8ac68d5.png)

*	ranking affiliations by the number of papers published this century by affiliated authors;
![image](https://user-images.githubusercontent.com/55859977/230324261-4299767f-9198-4331-8421-ae2edda00816.png)

*	ranking affiliations by the total number of paper citations from affiliated authors;
![image](https://user-images.githubusercontent.com/55859977/230324315-86897310-415c-4ae8-a107-76b1cdf88819.png)

*	ranking affiliations by the average number of citations per author;
![image](https://user-images.githubusercontent.com/55859977/230324358-b353b629-46cf-4a5d-93a2-a10d5415c32a.png)

*	ranking publication venues by the total number of published papers;
 ![image](https://user-images.githubusercontent.com/55859977/230324417-f7f20153-6feb-4c26-9e33-1ef8cba02deb.png)

*	ranking publication venues by the total number of paper citations;
 ![image](https://user-images.githubusercontent.com/55859977/230324457-e315c04a-e24d-4bbb-97ad-17aa4523d91e.png)

*	ranking publication venues by the average number of citations per paper;
 ![image](https://user-images.githubusercontent.com/55859977/230324503-3504bdcc-6be5-44b8-8bf7-d4fca8779626.png)

*	ranking publication venues by the number of published math papers to see the top math publication venues;
 ![image](https://user-images.githubusercontent.com/55859977/230324537-a1f1ceaf-ab9b-478c-b17b-7610e23f7a93.png)

*	finding years with the most published papers;

 ![image](https://user-images.githubusercontent.com/55859977/230324600-9c2e7235-e2fd-403e-bfa4-9a61959a4359.png)

*	histograms for the number of papers on a given scientific domain (math in this case) over given years (2011-2022 in this case).
![image](https://user-images.githubusercontent.com/55859977/230324675-0488fd36-1b71-4b1a-bb68-cd279d8f4cca.png)

## 6.2.	Graph view

Below are examples of these queries run in Neo4j UI using real data from the pipeline.

PageRank on articles
![image](https://user-images.githubusercontent.com/55859977/230324751-ba8f1c70-a4fe-4fe9-80eb-1aa80bc0e0c9.png)

Author Articlerank
![image](https://user-images.githubusercontent.com/55859977/230324796-bcf1e7bb-81e8-47cc-bfc9-1e4c31621ee5.png)

Community Detection 
![image](https://user-images.githubusercontent.com/55859977/230324838-a97bdfc7-a116-4431-9dc6-521a7179e4fa.png)


# References

[1] https://www.kaggle.com/datasets/Cornell-University/arxiv?resource=download

[2] https://openalex.org/ 

[3] https://habanero.readthedocs.io/en/latest/modules/crossref.html 

[4] https://pypi.org/project/gender-guesser/ 


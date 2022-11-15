DEFAULT_ARGS = {
    'owner': 'Project',
    'depends_on_past': False,
    'retries': 0,
}
API_URL = 'http://65.108.50.112:5000/data/arxiv'
API_PARAMS = {
    'results': 50,
    'format': 'csv',
    'inc': 'title,doi'
}
DATA_FOLDER = '/tmp/data'
ARXIV_FILE_NAME = 'arxiv.json'
SQL_FILE_NAME = 'insert_arxiv.sql'

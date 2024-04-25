from datetime import datetime
import pytz
import requests
import pandas as pd
import psycopg2
from psycopg2.extras import execute_batch
from dotenv import load_dotenv
import os

load_dotenv()

# Constants
USER_DATA_API_URL = 'https://data.bl.ch/api/explore/v2.1/monitoring/datasets/ods-api-monitoring/exports/json'
DATASETS_DATA_API_URL = 'https://data.bl.ch/api/explore/v2.1/monitoring/datasets/ods-datasets-monitoring/exports/json'

def get_db_connection():
    '''
    Establishes a connection to the database using credentials from Airflow Variables.

    Returns:
        A psycopg2 connection object.
    '''
    return psycopg2.connect(
        host=os.getenv('ogd_analytics_host'),
        database=os.getenv('ogd_analytics_db'),
        user=os.getenv('ogd_analytics_user'),
        password=os.getenv('ogd_analytics_password'),
        port=os.getenv('ogd_analytics_port')
    )

def get_latest_timestamp(table, field):
    '''
    Fetches the latest timestamp from a specified table and field.

    Parameters:
        table (str): The database table name.
        field (str): The field name to query for the latest timestamp.

    Returns:
        datetime: The latest timestamp.
    '''

    with get_db_connection() as conn, conn.cursor() as cur:
        cur.execute(f'SELECT {field} FROM {table} ORDER BY {field} DESC LIMIT 1')
        return cur.fetchone()[0]

def download_data(url, params=None):
    '''
    Downloads data from a specified URL with optional query parameters.

    Parameters:
        url (str): The API URL to download data from.
        params (dict, optional): Query parameters for the request.

    Returns:
        DataFrame: A pandas DataFrame containing the downloaded data.
    '''

    headers = {
        'Accept': 'application/json',
        'Authorization': f'Apikey {os.getenv("ogd_opendatasoft_token")}'
    }

    proxies = {
        'http'  : os.getenv("https_proxy"), 
        'https' : os.getenv("https_proxy"),     
    }
    
    response = requests.get(url, headers=headers, proxies=proxies ,params=params)

    return pd.DataFrame.from_records(response.json())

def download_user_data(start_date, end_date):
    '''
    Downloads user data for a specified date range.

    Parameters:
        start_date (str): The start date in YYYY-MM-DD format.
        end_date (str): The end date in YYYY-MM-DD format.

    Returns:
        DataFrame: A pandas DataFrame containing the user data.
    '''
    params = {
        'lang': 'de',
        'qv1': f'(timestamp:[{start_date} TO {end_date}])',
        'timezone': 'Europe/Berlin'
    }
    return download_data(USER_DATA_API_URL, params=params)

def download_datasets_data():
    '''
    Downloads datasets data.

    Returns:
        DataFrame: A pandas DataFrame containing the datasets data.
    '''
    params = {'lang': 'de', 'timezone': 'Europe/Berlin'}
    return download_data(DATASETS_DATA_API_URL, params=params)

def to_timestamp(x):
    '''
    Converts a string to a timestamp, handling different date formats.

    Parameters:
        x (str): The date string to convert.

    Returns:
        datetime: The converted datetime object.
    '''
    try:
        x = datetime.strptime(x, '%Y-%m-%dT%H:%M:%S.%f%z').replace(microsecond=0)
    except ValueError:
        x = datetime.strptime(x, '%Y-%m-%dT%H:%M:%S%z').replace(microsecond=0)

    return x.replace(tzinfo=pytz.utc).astimezone(pytz.timezone('Europe/Berlin')).replace(tzinfo=None)

def extract_dataset(list):
    '''
    Extracts the dataset from a list, handling cases where it might not be present. The opendatasoft monitoring api returns the dataset_id as a list with one id in it.

    Parameters:
        list: The list to extract the dataset from.

    Returns:
        The extracted dataset, or None if not extractable.
    '''
    try:
        return list[0]
    except:
        return None

def upload_data(df, table):
    '''
    Uploads data from a DataFrame to a specified database table.

    Parameters:
        df (DataFrame): The pandas DataFrame containing the data to upload.
        table (str): The database table to upload the data to.
    '''
    with get_db_connection() as conn, conn.cursor() as cur:
        # Lock the table in SHARE ROW EXCLUSIVE MODE
        cur.execute(f"LOCK TABLE {table} IN SHARE ROW EXCLUSIVE MODE;")

        # Prepare and execute the INSERT query
        cols = ', '.join(df.columns)
        vals_placeholder = ', '.join(['%s'] * len(df.columns))
        query = f'INSERT INTO {table}({cols}) VALUES ({vals_placeholder});'
        execute_batch(cur, query, df.values.tolist(), page_size=2000)
        conn.commit()


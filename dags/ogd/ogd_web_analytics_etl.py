dag_doc_md = """
### OGD Web Analytics ETL DAG

This DAG is responsible for updating user actions and datasets analytics data. It starts by determining the last update dates for user actions and datasets. Depending on the freshness of the data, it may proceed to update these datasets by fetching the latest data from the Opendatasoft API and uploading it to a PostgreSQL database.

The workflow includes tasks to check the up-to-dateness of data, download new data, and update the database accordingly.
"""

### importing the required libraries
from airflow import DAG
from airflow.operators.python_operator import PythonOperator, ShortCircuitOperator

from pendulum import duration
from datetime import datetime, timedelta

from dotenv import load_dotenv
import os

load_dotenv()

from ogd.ogd_web_analytics_utilities import *


get_update_dates_doc = """
**Task ID:** `get_update_dates`

**Description:** Fetches the latest update dates for user actions and datasets from the PostgreSQL database and determines if the data needs updating based on the current date.
"""
def get_update_dates():
    latest_user_date = get_latest_timestamp('ogd_analytics.user_actions', 'timestamp').strftime('%Y-%m-%d')
    latest_datasets_date = get_latest_timestamp('ogd_analytics.datasets', 'timestamp').strftime('%Y-%m-%d')
    today = (datetime.now()).strftime('%Y-%m-%d')

    return latest_user_date, latest_datasets_date, today


is_user_data_up_to_date_doc = """
**Task ID:** `is_user_data_up_to_date`

**Description:** Evaluates whether the user actions data in the database is up-to-date. If the data is older than 2 days, it proceeds to update it.
"""
def is_user_data_up_to_date(ti):
    latest_user_date, _, today = ti.xcom_pull(task_ids='get_update_dates')

    if datetime.strptime(latest_user_date, '%Y-%m-%d').date() < (datetime.strptime(today, '%Y-%m-%d') - timedelta(days=1)).date():
        return True
    else:
        return False
    

is_datasets_data_up_to_date_doc = """
**Task ID:** `is_datasets_data_up_to_date`

**Description:** Checks if the datasets analytics data is current. The task triggers the update process if the data is one or more days old.
"""
def is_datasets_data_up_to_date(ti):
    _, latest_datasets_date, today = ti.xcom_pull(task_ids='get_update_dates')

    if datetime.strptime(latest_datasets_date, '%Y-%m-%d').date() < (datetime.strptime(today, '%Y-%m-%d')).date():
        return True
    else:
        return False


update_user_data_doc = """
**Task ID:** `update_user_data`

**Description:** Downloads and processes the latest user actions data from the Opendatasoft API, filtering it for relevance and formatting before uploading it to the PostgreSQL database.
"""
def update_user_data(ti):
    latest_user_date, latest_datasets_date, today = ti.xcom_pull(task_ids='get_update_dates')

    # Downloading data

    # The Opendatasoft API can't really be trusted with the time selection. We therefore download data from
    # one day before the last update and today and afterwards we filter the data ourself.
    # In the end we only use data between the most recent date in the database and yesterday.
    query_update_start = (datetime.strptime(latest_user_date, '%Y-%m-%d') - timedelta(days=1)).strftime('%Y-%m-%d')

    df = download_user_data(query_update_start, today)
    
    # Cleansing
    columns = ['timestamp', 'user_ip_addr', 'user_id', 'dataset_id', 'api_type', 'mobile', 'action', 'attributes', 'filename', 'query_text', 'domain_id', 'query_string', 'format', 'user_agent', 'referer', 'geo_coordinates', 'hostname']
    df = df[columns].reset_index(drop=True)
    
    df['timestamp'] = df['timestamp'].apply(to_timestamp)
    df = df[(df['timestamp'].dt.date > datetime.strptime(latest_user_date, '%Y-%m-%d').date()) & (df['timestamp'].dt.date < datetime.strptime(today, '%Y-%m-%d').date())].reset_index(drop=True)
    df['dataset_id'] = df['dataset_id'].apply(extract_dataset)

    for col in [col for col in columns if col not in ['timestamp', 'dataset_id']]:
        df[col] = df[col].astype(str)


    # Upload data
    latest_user_date = get_latest_timestamp('ogd_analytics.user_actions', 'timestamp')
    if latest_user_date.date() < (datetime.now() - timedelta(days=1)).date():
        upload_data(df, 'ogd_analytics.user_actions')
    else:
        print('User data already updated, probably by a concurrently running job')
    

update_datasets_data_doc = """
**Task ID:** `update_datasets_data`

**Description:** Fetches the latest datasets analytics data, processes it, and updates the PostgreSQL database.
"""
def update_datasets_data(ti):
    _, _, today = ti.xcom_pull(task_ids='get_update_dates')

    df = download_datasets_data()
    df['timestamp'] = datetime.now()
    columns = ['timestamp', 'dataset_id', 'title', 'modified', 'publisher', 'license', 'keyword', 'theme', 'api_call_count', 'download_count', 'records_count', 'visibility']

    df = df[columns].reset_index(drop=True)

    latest_datasets_date = get_latest_timestamp('ogd_analytics.datasets', 'timestamp')
    if latest_datasets_date.date() < datetime.now().date():
        upload_data(df, 'ogd_analytics.datasets')
    else:
        print('Datasets data already updated, probably by a concurrently running job')


# DAG definition
with DAG(
    dag_id='ogd_web_analytics_etl',
    start_date=datetime(2023, 1, 1),
    schedule_interval='0 5 * * *',
    catchup=False,
    tags=['ogd'],
    default_args={
        'owner': 'ogd',
        'depends_on_past': False,
        'email': os.getenv('ogd_alert_mails'),
        'email_on_failure': True,
        'email_on_retry': False,
        'retries': 2,
        'retry_delay': duration(minutes=4*60)
    },
    doc_md=dag_doc_md
) as dag:

    get_update_dates_task = PythonOperator(
        task_id='get_update_dates',
        python_callable=get_update_dates,
        doc_md=get_update_dates_doc
    )

    is_user_data_up_to_date_task = ShortCircuitOperator(
        task_id='is_user_data_up_to_date',
        python_callable=is_user_data_up_to_date,
        doc_md=is_user_data_up_to_date_doc
    )

    is_datasets_data_up_to_date_task = ShortCircuitOperator(
        task_id='is_datasets_data_up_to_date',
        python_callable=is_datasets_data_up_to_date,
        doc_md=is_datasets_data_up_to_date_doc
    )

    update_user_data_task = PythonOperator(
        task_id='update_user_data',
        python_callable=update_user_data,
        doc_md=update_user_data_doc
    )

    update_datasets_data_task = PythonOperator(
        task_id='update_datasets_data',
        python_callable=update_datasets_data,
        doc_md=update_datasets_data_doc
    )

    #get_update_dates_task fetches the most current timestamps in our databases and today's date. It then triggers is_user_data_up_to_date_task and is_datasets_data_up_to_date_task
    # is_user_data_up_to_date_task and is_datasets_data_up_to_date_task are short circuit operators that succeed if the timestamps from our databases are not up to date. If they succeed, they trigger the respective update task
    # update_user_data_task queries the user actions data from the Opendatasoft api and uploads it to our database
    # update_datasets_data_task queries the current analytics metadata for each dataset from Opendatasoft and uploads it to our database

    get_update_dates_task >> [is_user_data_up_to_date_task, is_datasets_data_up_to_date_task]
    is_user_data_up_to_date_task >> update_user_data_task
    is_datasets_data_up_to_date_task >> update_datasets_data_task

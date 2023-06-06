from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
import requests
import psycopg2
from airflow.models import Variable
import logging

DATABASE_NAME = Variable.get("DATABASE_NAME")
DATABASE_USER = Variable.get("DATABASE_USER")
DATABASE_PASS = Variable.get("DATABASE_PASS")
DATABASE_HOST = "google_proxy"


def fetch_hacker_news():
    try:
        response = requests.get('https://api.hnpwa.com/v0/news/1.json')
        news_data = response.json()[:10]
        return news_data
    except Exception as e:
        logging.error("Error Hacker news api: %s", str(e))
        return []


def clean_news_data(**context):
    """
    This function will clean the data and only get the related data from the api response.
    :param context: it will fetch the data using xcom from hacker news api dag
    :return: -> dict
    """
    news_data = context['ti'].xcom_pull(task_ids='fetch_hacker_news_task')
    if not news_data:
        return []
    cleaned_data = []

    for news_item in news_data:
        cleaned_item = {
            'title': news_item['title'],
            'url': news_item['url'],
            'author': news_item['user'],
            'points': news_item['points']
        }
        cleaned_data.append(cleaned_item)

    return cleaned_data


def store_news_in_database(**context):
    """
    Data fetched from other flow will be stored in database.
    :param context: it will fetch the data using xcom from other dag
    :return: -> None
    """
    cleaned_data = context['ti'].xcom_pull(task_ids='clean_news_data_task')
    if not cleaned_data:
        return
    conn = psycopg2.connect(
        host=DATABASE_HOST,
        port=3308,
        database=DATABASE_NAME,
        user=DATABASE_USER,
        password=DATABASE_PASS
    )
    cursor = conn.cursor()
    """
    Create the table in database if not exists other wise it will not trigger.
    """
    # Define the insert query with conflict resolution
    query = '''
        INSERT INTO news_airflow (title, url, author, points)
        VALUES (%s, %s, %s, %s)
        ON CONFLICT (title) DO UPDATE SET
            url = EXCLUDED.url,
            author = EXCLUDED.author,
            points = EXCLUDED.points
    '''

    # Prepare the data for bulk insert
    data = [(news_item['title'], news_item['url'], news_item['author'], news_item['points']) for news_item in
            cleaned_data]

    # Execute the bulk insert
    try:
        cursor.executemany(query, data)
        conn.commit()  # Commit the changes
        print("Bulk insert successful!")
    except Exception as e:
        conn.rollback()  # Rollback the transaction in case of an error
        logging.error("Error inserting data: %s", str(e))

    conn.commit()
    cursor.close()
    conn.close()
    logging.info("Data inserted successfully")


default_args = {
    'owner': 'ML-Sense',
    'start_date': datetime(2023, 1, 1),
    'catchup': False
}

with DAG('hacker_news_dag', default_args=default_args, schedule_interval="@daily", catchup=False) as dag:
    """
    This is the main dag which will start the airflow tasks 
    """
    fetch_task = PythonOperator(
        task_id='fetch_hacker_news_task',
        python_callable=fetch_hacker_news
    )

    clean_task = PythonOperator(
        task_id='clean_news_data_task',
        python_callable=clean_news_data,
        provide_context=True
    )

    store_task = PythonOperator(
        task_id='store_news_in_database_task',
        python_callable=store_news_in_database,
        provide_context=True
    )
    # Set task dependencies e.g 1 -> 2 -> 3 task
    fetch_task >> clean_task >> store_task # noqa

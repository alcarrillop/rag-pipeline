from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from datetime import datetime, timedelta
import requests
import pandas as pd
from sqlalchemy import create_engine
from psycopg2.extras import execute_values
import logging
import time
import hashlib
import json

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 6, 1, 0, 0),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

dag = DAG(
    'etl_api_stackoverflow',
    default_args=default_args,
    description='ETL DAG for Stack Overflow questions',
    schedule_interval='0 * * * *',  # Ejecutar cada hora
    catchup=False
)

client_id = '29225'
access_token = 'yy9cBgqxQnCE04sArgr57A))'
key = '*Jo5bSsNWYzx0kB)pvj6tA(('

# Simple cache dictionary
cache = {}

def generate_cache_key(url, params):
    key = f"{url}:{json.dumps(params, sort_keys=True)}"
    return hashlib.md5(key.encode('utf-8')).hexdigest()

def extract_data(tag, fromdate, todate):
    url = 'https://api.stackexchange.com/2.3/search/advanced'
    params = {
        'order': 'desc',
        'sort': 'activity',
        'tagged': tag,
        'site': 'stackoverflow',
        'filter': 'withbody',
        'key': key,
        'access_token': access_token,
        'fromdate': fromdate,
        'todate': todate
    }
    
    cache_key = generate_cache_key(url, params)
    
    # Check if response is cached
    if cache_key in cache:
        cached_response, timestamp = cache[cache_key]
        if time.time() - timestamp < 60:  # Cache validity period of 60 seconds
            logging.info("Using cached response")
            return cached_response
    
    try:
        response = requests.get(url, params=params)
        response.raise_for_status()
        data = response.json()
        
        # Handle dynamic throttling
        if 'backoff' in data:
            backoff = data['backoff']
            logging.info(f"Backoff received. Sleeping for {backoff} seconds.")
            time.sleep(backoff)
        
        # Cache the response
        cache[cache_key] = (data['items'], time.time())
        
        return data['items']
    
    except requests.exceptions.RequestException as e:
        logging.error(f"Error during API request: {e}")
        return []

def transform_data(questions):
    question_answers = []

    for question in questions:
        try:
            question_id = question['question_id']
            question_content = f"{question['title']} {question.get('body', '')} Tags: {' '.join(question['tags'])}"

            # Fetch the answers for the question
            answer_url = f"https://api.stackexchange.com/2.3/questions/{question_id}/answers"
            answer_params = {
                'order': 'desc',
                'sort': 'votes',
                'site': 'stackoverflow',
                'filter': 'withbody',
                'key': key,
                'access_token': access_token
            }
            
            answer_response = requests.get(answer_url, params=answer_params)
            answer_response.raise_for_status()
            answers = answer_response.json().get('items', [])

            if answers:
                question_answers.append({
                    'questionid': question_id,
                    'questioncontent': question_content,
                    'answerbody': answers[0].get('body', ''),  # Take the first answer (highest voted)
                    'tags': ' '.join(question['tags'])
                })
            # Handle rate limiting
            if 'backoff' in answer_response.json():
                backoff = answer_response.json()['backoff']
                logging.info(f"Backoff received. Sleeping for {backoff} seconds.")
                time.sleep(backoff)
        except requests.exceptions.RequestException as e:
            logging.error(f"Error fetching answers for question {question_id}: {e}")
            continue

    df = pd.DataFrame(question_answers)
    return df.to_dict(orient='records')

def load_data(records):
    if not records:
        logging.info("No records to load.")
        return

    engine = create_engine('postgresql+psycopg2://postgres:123456@host.docker.internal:5432/postgres')
    
    df = pd.DataFrame(records)
    
    if df.empty:
        logging.info("DataFrame is empty. No records to load into the database.")
        return
    
    records = df.to_records(index=False).tolist()
    
    insert_query = """
    INSERT INTO stackoverflow (questionid, questioncontent, answerbody, tags)
    VALUES %s
    ON CONFLICT (questionid) DO NOTHING
    """
    
    try:
        with engine.connect() as conn:
            with conn.connection.cursor() as cur:
                execute_values(cur, insert_query, records)
            conn.connection.commit()
    except Exception as e:
        logging.error(f"Error inserting records into the database: {e}")

def extract_task(tag, **kwargs):
    fromdate = kwargs['execution_date'] - timedelta(hours=1)
    todate = kwargs['execution_date']
    fromdate_unix = int(fromdate.timestamp())
    todate_unix = int(todate.timestamp())
    return extract_data(tag, fromdate_unix, todate_unix)

def transform_task(ti, **kwargs):
    task_id = kwargs['task_instance'].task_id
    questions = ti.xcom_pull(task_ids=f'extract_{task_id.split("_")[1]}')
    return transform_data(questions)

def load_task(ti, **kwargs):
    task_id = kwargs['task_instance'].task_id
    records = ti.xcom_pull(task_ids=f'transform_{task_id.split("_")[1]}')
    load_data(records)

# Definir las tareas del DAG
start = DummyOperator(task_id='start', dag=dag)

tags = ['pyspark', 'python']
for tag in tags:
    extract_operator = PythonOperator(
        task_id=f'extract_{tag}',
        python_callable=extract_task,
        op_kwargs={'tag': tag},
        provide_context=True,
        dag=dag,
    )

    transform_operator = PythonOperator(
        task_id=f'transform_{tag}',
        python_callable=transform_task,
        provide_context=True,
        dag=dag,
    )

    load_operator = PythonOperator(
        task_id=f'load_{tag}',
        python_callable=load_task,
        provide_context=True,
        dag=dag,
    )

    start >> extract_operator >> transform_operator >> load_operator
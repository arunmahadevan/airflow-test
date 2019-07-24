import json
import requests
import time
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import os

LIVY_URL = 'http://spark-test-livy.spark-test/batches'

def checkStatus(**kwargs):
    batchId = kwargs['ti'].xcom_pull(task_ids='submitJob')
    print('Waiting for batch id: ' + batchId + ' to complete')
    while True:
        time.sleep(5)
        r = requests.get(url = LIVY_URL + "/" + batchId)
        data = r.json()
        #print(data)
        status = data['state']
        if status == 'success':
            break
        print('Status: ' + status)
    print('Done')
    return batchId

def logPi(**kwargs):
    batchId = kwargs['ti'].xcom_pull(task_ids='checkStatus')
    r = requests.get(url = LIVY_URL + "/" + batchId + "/log")
    data = r.json()
    for line in data['log']:
        if line.startswith('Pi is'):
            print(line)

def submitJob():
    payload = {"file": "local:///opt/spark/examples/jars/spark-examples_2.11-2.4.2.3.1.3.0-32.jar", "className": "org.apache.spark.examples.SparkPi", "args": ["1000"]}
    r = requests.post(url = LIVY_URL, data=json.dumps(payload), headers={"Content-Type": "application/json"})
    data = r.json()
    print(data)
    id=data['id']
    return str(id)


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2019, 5, 28),
    'email': ['arunm@cloudera.com'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 1,
    'retry_delay': timedelta(minutes=1)
}

dag = DAG(
    'spark-test',
    default_args=default_args,
    description='Spark test DAG',
    schedule_interval='*/2 * * * *',
    catchup=False
)

t1 = PythonOperator(
    task_id='submitJob',
    python_callable=submitJob,
    dag=dag,
)

t2 = PythonOperator(
    task_id='checkStatus',
    python_callable=checkStatus,
    provide_context=True,
    dag=dag,
)

t3 = PythonOperator(
    task_id='getResult',
    python_callable=logPi,
    provide_context=True,
    dag=dag,
)

t1 >> t2 >> t3

import json
import requests
import time
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import os
import re

# TODO: merge spark-test and airflow-test into a single chart
#  and export LIVY_HOSTNAME (i.e. RELEASE_NAME-livy) in the env via chart config map and use it. 
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

def logResult(**kwargs):
    batchId = kwargs['ti'].xcom_pull(task_ids='checkStatus')
    r = requests.get(url = LIVY_URL + "/" + batchId + "/log?size=1000000")
    data = r.json()
    for line in data['log']:
        print(line)

def getNextQueryId():
    r = requests.get(url = LIVY_URL)
    data = r.json()
    if data['sessions']:
        batchId = str(data['sessions'][-1]['id'])
        r = requests.get(url = LIVY_URL + "/" + batchId + "/log?size=1000000")
        data = r.json()
        for line in data['log']:
            m = re.search("To:\s(\d+)", line)
            if m:
                return int(m.group(1)) % 99
    return 0

def submitJob():
    qid = getNextQueryId()
    payload = {"file": "local:///opt/hadoop/jars/tpcds-test-0.1.jar", "className": "com.example.cluster.TcpdsTest", "args": [qid, qid + 1]}
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
    'tpcds',
    default_args=default_args,
    description='tpcds DAG',
    schedule_interval='*/15 * * * *',
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
    python_callable=logResult,
    provide_context=True,
    dag=dag,
)

t1 >> t2 >> t3

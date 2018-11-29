
from airflow import utils
from airflow import DAG
from datetime import datetime, timedelta
from airflow.contrib.operators.livy_spark_operator import LivySubmitRunOperator

now = datetime.now()
now_to_the_hour = (now - timedelta(0, 0, 0, 0, 0, 3)).replace(minute=0, second=0, microsecond=0)
START_DATE = now_to_the_hour
DAG_NAME = 'livy_operator_test1'

default_args = {
    'owner': 'test',
    'depends_on_past': True,
    'start_date': utils.dates.days_ago(2)
}
dag = DAG(DAG_NAME, schedule_interval='*/10 * * * *', default_args=default_args)
json = {"file": "/spark/pi.py"}
livy_conn = "http://ap-xxxxx-b.rds.aliyuncs.com:8998"
job1 = LivySubmitRunOperator(task_id='livy_python_task1', json=json, livy_conn=livy_conn, dag=dag)
job2 = LivySubmitRunOperator(task_id='livy_python_task2', json=json, livy_conn=livy_conn, dag=dag)
job2.set_upstream(job1)

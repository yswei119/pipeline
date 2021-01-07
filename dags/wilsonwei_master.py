
from datetime import timedelta

from airflow.models import DAG

from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago

args = {
    'owner': 'Airflow',
    'start_date': days_ago(2),
}

dag = DAG(
    dag_id='wilsonwei_master',
    default_args=args,
    schedule_interval='0 0 * * *',
    dagrun_timeout=timedelta(minutes=60),
    tags=['example']
)

def func_a():
    print(1)
    
def func_b():
    print(2)
    
dag_func_a = PythonOperator(task_id='master_1',
              python_callable=func_a,
              dag=dag)

dag_func_b = PythonOperator(task_id='master_2',
              python_callable=func_b,
              dag=dag)
dag_func_a>>dag_func_b
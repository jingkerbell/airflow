from airflow import DAG
import datetime
import pendulum
from airflow.operators.python import PythonOperator
from common.common_func import regist

with DAG(
    dag_id="dags_python_task_decorator",
    schedule="30 6 * * *",
    start_date=pendulum.datetime(2024, 4, 18, tz="Asia/Seoul"),
    catchup=False,
) as dag:
    
    regist_t1 = PythonOperator(
        task_id="regist_t1",
        python_callable=regist,
        op_args=['hjkim','man','kr','seoul']
    )

regist_t1
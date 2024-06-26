from airflow import DAG
import datetime
import pendulum
from airflow.operators.python import PythonOperator
from airflow.decorators import task


with DAG(
    dag_id="dags_python_template",
    schedule="10 0 * * *",
    start_date=pendulum.datetime(2024, 4, 9, tz="Asia/Seoul"),
    catchup=False
) as dag:
    def python_function1(start_dt, end_dt, **kwargs):
        print(start_dt)
        print(end_dt)

    python_t1 = PythonOperator(
        task_id="python_t1",
        python_callable=python_function1,
        op_kwargs={'start_dt':'{{data_interval_start | ds}}','end_dt':'{{data_interval_end | ds}}'}
    )

    @task(task_id='python_t2')
    def python_function2(**kwargs):
        print(kwargs)
        print('ds:'+ kwargs['ds'])
        print('ts:'+ kwargs['ts'])
        print('data_interval_start:'+ str(kwargs['data_interval_start']))
        print('data_interval_end:'+ str(kwargs['data_interval_end']))
        print('task_instance:'+ str(kwargs['ti']))


    python_t1 >> python_function2()
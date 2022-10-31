from airflow import DAG
from datetime import datetime,timedelta

from airflow.operators.bash import BashOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator

with DAG('j_spark_trigger',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 0,
    },
    schedule=timedelta(days=1),
    start_date=datetime(2022, 10, 21),
    catchup = False,


) as dag:


    load_data = PostgresOperator(task_id ='load_data',
    sql="sql/01-create-data.sql", 
    postgres_conn_id='conn_postgres')

    transform_data_1 = BashOperator(task_id ='transform_data_1',
    bash_command='/opt/spark/bin/spark-submit /mnt/script-1.py'
    )

    transform_data_2 = BashOperator(task_id ='transform_data_2',
    bash_command='/opt/spark/bin/spark-submit /mnt/script-2.py'
    )

    load_data >> transform_data_1
    load_data >> transform_data_2

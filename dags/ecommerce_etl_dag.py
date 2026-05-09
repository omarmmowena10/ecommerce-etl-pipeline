from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='ecommerce_etl_pipeline',
    default_args=default_args,
    description='Ecommerce ETL Pipeline - Daily',
    schedule_interval='@daily',
    start_date=datetime(2024, 1, 1),
    catchup=False,
) as dag:

    extract = BashOperator(
        task_id='extract',
        bash_command='docker exec spark-jupyter spark-submit --master yarn --conf spark.driver.host=172.30.1.13 /tmp/e.py',
    )

    transform = BashOperator(
        task_id='transform',
        bash_command='docker exec spark-jupyter spark-submit --master yarn --conf spark.driver.host=172.30.1.13 /tmp/t.py',
    )

    load = BashOperator(
        task_id='load',
        bash_command='docker exec spark-jupyter spark-submit --master yarn --conf spark.driver.host=172.30.1.13 --jars /opt/spark/jars/spark-snowflake_2.12-2.12.0-spark_3.3.jar,/opt/spark/jars/snowflake-jdbc-3.13.30.jar /tmp/l.py',
    )

    extract >> transform >> load
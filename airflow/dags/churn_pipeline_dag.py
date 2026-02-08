from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

default_args = {
    "owner": "abhay",
    "start_date": datetime(2025, 1, 1),
    "retries": 1,
}

with DAG(
    dag_id="churn_stream_batch_pipeline",
    default_args=default_args,
    schedule_interval="@daily",
    catchup=False
) as dag:

    run_spark_batch = BashOperator(
        task_id="run_spark_batch_job",
        bash_command="""
        /home/abhay/spark-3.5.7-bin-hadoop3/bin/spark-submit \
        --master local[*] \
        /media/abhay/768270A98270700B/bigdata/churn_streaming_project/spark/batch_processing.py
        """
    )

    run_spark_batch

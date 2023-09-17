from datetime import timedelta
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.utils.dates import days_ago

default_args = {
    "owner": "dibimbing",
    "retry_delay": timedelta(minutes=5),
}

spark_dag = DAG(
    dag_id="assignment_day_15",
    default_args=default_args,
    schedule_interval=None,
    dagrun_timeout=timedelta(minutes=60),
    description="Assignment Day 15",
    start_date=days_ago(1),
)

# menambahkan driver_class_path dan jars untuk mengkoneksikan spark submit ke postgres
Extract = SparkSubmitOperator(
     application="/spark-scripts/spark-assignment15.py",
     conn_id="spark_tgs",
     task_id="spark_submit_task",
     dag=spark_dag,
     driver_class_path="/spark-scripts/postgresql-42.6.0.jar",
     jars="/spark-scripts/postgresql-42.6.0.jar"
 )

Extract

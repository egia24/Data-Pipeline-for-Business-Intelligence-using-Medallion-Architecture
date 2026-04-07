"""
dag_olist_pipeline.py
Full pipeline Olist: ingest → bronze_to_silver → silver_to_gold
Jadwal: setiap hari jam 06:00
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago

default_args = {
    "owner":            "egg",
    "retries":          2,
    "retry_delay":      timedelta(minutes=5),
    "email_on_failure": False,
}

SPARK_SUBMIT = (
    "docker exec spark_master_olist /opt/spark/bin/spark-submit "
    "--master spark://spark-master:7077 "
    "--packages org.postgresql:postgresql:42.6.0 "
    "--py-files /opt/project/config.py "
    "/app/jobs/{script}"
)

with DAG(
    dag_id="olist_pipeline",
    description="Full pipeline: CSV → Bronze → Silver (star schema) → Gold",
    default_args=default_args,
    start_date=days_ago(1),
    schedule_interval="0 6 * * *",
    catchup=False,
    tags=["olist", "etl", "spark"],
) as dag:

    ingest = BashOperator(
        task_id="ingest_csv_to_bronze",
        bash_command="docker exec pyworker_olist python ingestion/ingest.py",
    )

    bronze_to_silver = BashOperator(
        task_id="bronze_to_silver",
        bash_command="""
docker exec -i spark_master_olist \
/opt/spark/bin/spark-submit \
--master spark://spark-master:7077 \
--packages org.postgresql:postgresql:42.7.3 \
/app/jobs/bronze_to_silver.py
""",
    )

    silver_to_gold = BashOperator(
        task_id="silver_to_gold",
        bash_command="""
docker exec -i spark_master_olist \
/opt/spark/bin/spark-submit \
--master spark://spark-master:7077 \
--packages org.postgresql:postgresql:42.7.3 \
/app/jobs/silver_to_gold.py
""",
    )

    ingest >> bronze_to_silver >> silver_to_gold
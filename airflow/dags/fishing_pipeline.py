from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2026, 3, 1),
    "email_on_failure": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    dag_id="fishing_pipeline",
    default_args=default_args,
    description="End-to-end Fishing Analytics: Kafka to Spark ETL",
    schedule="@daily",
    catchup=False,
) as dag:

    start = EmptyOperator(task_id="start")

    # Generates our data and sends to Kafka topic "fishing_records"
    produce_events = BashOperator(
        task_id="produce_order_events",
        bash_command="python /opt/project/kafka/producer.py --bootstrap-servers kafka:29092 --num-events 500",
    )

    # Updated Bronze Task (Streaming from Kafka)
    run_bronze_stream = BashOperator(
        task_id="run_bronze_stream",
        bash_command=(
            "spark-submit "
            "--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 "
            "/opt/project/spark/stream_consumer.py "
            "--bootstrap-servers kafka:29092 --duration 120"
        ),
    )

    # Updated Gold Task (Aggregates)
    run_silver_gold_batch = BashOperator(
        task_id="run_silver_gold_batch",
        bash_command="spark-submit /opt/project/spark/batch_df_etl.py",
    )

    end = EmptyOperator(task_id="end")

    (
        start
        >> produce_events
        >> run_bronze_stream
        >> run_silver_gold_batch
        >> end
    )

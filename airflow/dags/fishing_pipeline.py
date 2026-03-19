from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

# Default arguments for the DAG
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

    # STEP 3: Generate data into Kafka
    produce_events = BashOperator(
        task_id="produce_order_events",
        bash_command="python /opt/project/kafka/producer.py --bootstrap-servers kafka:29092 --num-events 500",
    )

    # STEP 4: Consume stream and write raw Parquet (Using internal Kafka:29092)
    run_streaming_consumer = BashOperator(
        task_id="run_streaming_consumer",
        bash_command=(
            "spark-submit "
            "--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 "
            "/opt/project/spark/stream_consumer.py "
            "--bootstrap-servers kafka:29092 --duration 120"
        ),
    )

    # STEP 5: RDD-based Batch ETL (Using /opt/project path)
    run_batch_rdd_etl = BashOperator(
        task_id="run_batch_rdd_etl",
        bash_command="spark-submit /opt/project/spark/batch_rdd_etl.py",
    )

    # STEP 6: DataFrame-based Batch ETL (Using /opt/project path)
    run_batch_df_etl = BashOperator(
        task_id="run_batch_df_etl",
        bash_command="spark-submit /opt/project/spark/batch_df_etl.py",
    )

    end = EmptyOperator(task_id="end")

    # --- DAG Structure ---
    (
        start
        >> produce_events
        >> run_streaming_consumer
        >> [run_batch_rdd_etl, run_batch_df_etl]
        >> end
    )

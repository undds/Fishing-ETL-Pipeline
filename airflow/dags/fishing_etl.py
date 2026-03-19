# from airflow import DAG
# from airflow.operators.bash import BashOperator
# from datetime import datetime, timedelta

# default_args = {
#     "owner": "data_engineer",
#     "retries": 1,
#     "retry_delay": timedelta(minutes=1),
# }

# with DAG(
#     dag_id="fishing_etl_pipeline",
#     default_args=default_args,
#     description="Kafka → Spark → Batch ETL Pipeline",
#     schedule_interval=None,  # manual trigger
#     start_date=datetime(2024, 1, 1),
#     catchup=False,
#     tags=["kafka", "spark", "etl"],
# ) as dag:

#     # Step 1: Run Producer
#     run_producer = BashOperator(
#         task_id="run_kafka_producer",
#         bash_command="python /opt/project/Kafka/producer.py",
#     )

#     # Step 2: Wait for data
#     wait_for_data = BashOperator(
#         task_id="wait_for_data",
#         bash_command="sleep 20",
#     )

#     # Step 3: Run RDD ETL
#     run_rdd_etl = BashOperator(
#         task_id="run_rdd_etl",
#         bash_command="python /opt/project/batch_rdd_etl.py",
#     )

#     # Step 4: Run DataFrame ETL
#     run_df_etl = BashOperator(
#         task_id="run_df_etl",
#         bash_command="python /opt/project/batch_df_etl.py",
#     )

#     run_producer >> wait_for_data >> run_rdd_etl >> run_df_etl
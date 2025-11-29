from airflow import DAG
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from datetime import datetime

default_args = {"owner": "arseni", "start_date": datetime(2025, 11, 20), "retries": 0}

with DAG(
    "snowflake_retail_project_dag",
    default_args=default_args,
    schedule=None,
    catchup=False,
) as dag:
    load_raw_data = SQLExecuteQueryOperator(
        task_id="1_load_csv_to_stage",
        conn_id="snowflake_default",
        sql="""
            COPY INTO RETAIL_DWH.STAGE.RAW_ORDERS
            FROM @RETAIL_DWH.UTILS.MY_INTERNAL_STAGE/orders.csv
            FILE_FORMAT = (FORMAT_NAME = RETAIL_DWH.UTILS.MY_CSV_FORMAT)
            ON_ERROR = 'CONTINUE'; 
        """,
    )

    transform_data = SQLExecuteQueryOperator(
        task_id="2_transform_to_core",
        conn_id="snowflake_default",
        sql="CALL RETAIL_DWH.CORE.PROCESS_ORDERS();",
        autocommit=True,
    )

    load_raw_data >> transform_data

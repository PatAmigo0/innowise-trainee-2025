import os
import re
from datetime import datetime

import pandas as pd
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.python import BranchPythonOperator
from airflow.providers.standard.sensors.filesystem import FileSensor
from airflow.sdk import DAG, TaskGroup, task
from dag_constants import FILE_PATH, PROCESSED_FILE_PATH, processed_dataset


def _check_file_empty(file_path):
    if os.path.exists(file_path) and os.path.getsize(file_path) > 0:
        return "processing_group._load_and_clean_nulls"
    else:
        return "log_empty_file"


@task
def _load_and_clean_nulls(file_path):
    df = pd.read_json(file_path)
    df.replace("null", "-", inplace=True)
    print("Заменены 'null' на '-'")
    return df


@task
def _sort_by_date(df: pd.DataFrame):
    df["created_date"] = pd.to_datetime(df["created_date"])
    df.sort_values(by="created_date", inplace=True)
    print("Данные отсортированы по 'created_date'")
    return df


@task
def _clean_content(df: pd.DataFrame):
    def clean_text(text):
        # Оставляем буквы (включая кириллицу), цифры, пробелы и основные знаки препинания
        return re.sub(r"[^\w\s.,!?-]", "", text, flags=re.UNICODE)

    df["content"] = df["content"].apply(clean_text)
    print("Колонка 'content' очищена")
    return df


@task(outlets=[processed_dataset])
def _save_processed_data(df: pd.DataFrame, output_path):
    df.to_json(output_path, orient="records", date_format="iso")
    print(f"Обработанные данные сохранены в {output_path}")


with DAG(
    dag_id="dag_1_data_processing",
    start_date=datetime(2025, 10, 23),
    schedule=None,
    catchup=False,
    tags=["task_7", "processing"],
) as dag:
    wait_for_file = FileSensor(
        task_id="wait_for_file",
        filepath=FILE_PATH,
        poke_interval=10,
        timeout=300,
        mode="poke",
    )

    check_file_empty = BranchPythonOperator(
        task_id="check_file_empty",
        python_callable=_check_file_empty,
        op_args=[FILE_PATH],
    )

    log_empty_file = BashOperator(
        task_id="log_empty_file",
        bash_command=f"echo 'Файл {FILE_PATH} пустой или не найден...'",
    )

    with TaskGroup("processing_group") as processing_group:
        df_cleaned_nulls = _load_and_clean_nulls(FILE_PATH)
        df_sorted = _sort_by_date(df_cleaned_nulls)
        df_cleaned_content = _clean_content(df_sorted)

        _save_processed_data(df_cleaned_content, PROCESSED_FILE_PATH)

        # df_cleaned_nulls >> df_sorted >> df_cleaned_content >> _save_processed_data

    wait_for_file >> check_file_empty >> [log_empty_file, processing_group]

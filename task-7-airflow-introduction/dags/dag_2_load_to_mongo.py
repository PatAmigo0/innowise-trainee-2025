from datetime import datetime

import pandas as pd
from airflow.models.dag import DAG
from airflow.providers.mongo.hooks.mongo import MongoHook
from airflow.sdk import task
from dag_constants import MONGO_CONN_ID, PROCESSED_FILE_PATH, processed_dataset


@task
def _load_data_to_mongo():
    df = pd.read_json(PROCESSED_FILE_PATH)
    data_to_insert = df.to_dict("records")

    print(f"Подключение к MongoDB (Conn ID: {MONGO_CONN_ID})...")
    hook = MongoHook(mongo_conn_id=MONGO_CONN_ID)
    client = hook.get_conn()

    db = client["airflow_db"]
    collection = db["airflow_data"]

    collection.delete_many({})

    if data_to_insert:
        print(f"Вставка {len(data_to_insert)} записей в 'airflow_db.airflow_data'...")
        collection.insert_many(data_to_insert)
        print("Данные успешно загружены в MongoDB")
    else:
        print("Нет данных для вставки ;(")

    client.close()


with DAG(
    dag_id="dag_2_load_to_mongo",
    start_date=datetime(2025, 11, 10),
    schedule=[processed_dataset],
    catchup=False,
    tags=["task_7", "mongo", "load"],
) as dag:
    _load_data_to_mongo()

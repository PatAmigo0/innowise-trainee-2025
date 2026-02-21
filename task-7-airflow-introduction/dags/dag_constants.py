from airflow.datasets import Dataset

base_path = "mongodb://data/processed"
FILE_PATH = "/opt/airflow/data/airflow_data.json"
PROCESSED_FILE_PATH = "/opt/airflow/data/processed_data.json"
processed_dataset = Dataset(base_path)
MONGO_CONN_ID = "mongo_default"

import os
import sys

from pyspark.shell import spark
from dotenv import load_dotenv

load_dotenv()

jdbc_url = os.getenv("JDBC_URL")
db_user = os.getenv("DB_USER")
db_password = os.getenv("DB_PASSWORD")
pg_driver = os.getenv("PG_DRIVER")

def load_table(table_name):
    try:
        return spark.read.format("jdbc") \
            .option("url", jdbc_url) \
            .option("dbtable", table_name) \
            .option("user", db_user) \
            .option("password", db_password) \
            .option("driver", pg_driver) \
            .load()
    except Exception as e:
        print(f"Ошибка при загрузке таблицы {table_name}: {e}")
        sys.exit(1)
import os
import psycopg2 as sql
from  ..decorators.db_decorators import check_connection
from ..abstraction.basic_db import DBManager
from ..queries.basic_queries import save_query_template, insert_query_template
from ..queries.postgres_queries import headers_check_query_template, get_all_tables_query

class PostgresDB(DBManager):
    def __init__(self):
        super().__init__()
        self.cursor = None
        self.connection = None

    def __del__(self):
        self.disconnect()

    def connect(self, host="localhost", port=5432, dbname="postgres", user="postgres", password=""):
        try:
            self.connection = sql.connect(host=host, port=port, dbname=dbname, user=user, password=password)
            self.cursor = self.connection.cursor()
        except sql.OperationalError as e:
            print(f"Не удалось подключиться к БД: {e}")
            raise

    def connect_from_env(self):
        self.connect(
            host=os.environ.get("DB_HOST", "localhost"),
            port=int(os.environ.get("DB_PORT", 5432)),
            dbname=os.environ.get("DB_NAME", "postgres"),
            user=os.environ.get("DB_USER", "postgres"),
            password=os.environ.get("DB_PASSWORD", "")
        )

    def disconnect(self):
        if self.cursor:
            self.cursor.close()
            self.cursor = None
        if self.connection:
            self.connection.close()
            self.connection = None

    @check_connection
    def query(self, query_template, params=None):
        try:
            self.cursor.execute(query_template, params)
            return self.cursor.fetchall()
        except Exception as e:
            print(f"Ошибка при работе запроса: {e}")
            self.connection.rollback()
            return None

    @check_connection
    # В отличии от query возвращает больше данных
    def execute_query(self, query: str, params: tuple | None = None) -> tuple[list[tuple], list[str]] | tuple[list[None], list[None]]:
        try:
            self.cursor.execute(query, params or ())
            data = self.cursor.fetchall()

            if not self.cursor.description: return [], []

            headers = [desc[0] for desc in self.cursor.description]
            return data, headers

        except (Exception, sql.DatabaseError) as e:
            print(f"Ошибка при работе запроса: {e}")
            self.connection.rollback()
            return [], []

    @check_connection
    def check_table_exists(self, table_name):
        self.cursor.execute(get_all_tables_query)
        return table_name in (t[0] for t in self.cursor.fetchall())

    @check_connection
    def get_table_headers(self, table_name: str) -> list[str]:
        try:
            self.cursor.execute(headers_check_query_template.format(table_name))
            return [desc[0] for desc in self.cursor.description]
        except Exception as e:
            print(f"Не получилось получить колонки для {table_name}: {e}")
            self.connection.rollback()
            return []

    @check_connection
    def get_all_data(self, table_name: str) -> list[tuple]:
        query = save_query_template.format(table_name)
        data = self.query(query)
        return data or []

    @check_connection
    def insert_data_from_list(self, table_name: str, data_list: list[dict]):
        if not data_list:
            print("Нечего вставлять")
            return

        try:
            table_format = self.get_table_headers(table_name)
            if not table_format:
                print(f"Не удалось получить информацию о '{table_name}'")
                return

            table_format_set = set(table_format)
            str_table_format = ", ".join(f'"{v}"' for v in table_format)

            insert_query = insert_query_template.format(
                table_name,
                str_table_format,
                ", ".join("%s" for _ in range(len(table_format))),
            )

            print(f"Загружаю {len(data_list)} элементов в '{table_name}'...")
            items_inserted = 0
            for item in data_list:
                if not table_format_set.issubset(item.keys()): continue

                values = [item[key] for key in table_format]
                self.cursor.execute(insert_query, values)
                items_inserted += 1

            self.connection.commit()

        except Exception as e:
            print(f"Ошибка при вставке в таблицу '{table_name}': {e}")
            self.connection.rollback()
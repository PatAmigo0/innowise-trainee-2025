import os
import sys
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from xml.etree.ElementTree import ElementTree
from python_workspace.core.decorators.basic_decorators import isdir
from dotenv import load_dotenv
import json
import datetime
import xml.etree.ElementTree as ET
from core.systems.postgresql_db import PostgresDB
from core.queries.task_queries import *

rooms_path = r".\data\rooms.json"
students_path = r".\data\students.json"
output_folder_path = r".\data\output"
output_format = "json"


def init():
    load_dotenv("safe.env")
    load_dotenv("unsafe.env", override=True)
    print("Загрузил env!")


def fix_type(v):
    if isinstance(v, datetime.date):
        return v.isoformat()
    return v


def transform_data(obj: list[tuple[...]], headers: list[str], oformat=output_format, **kwargs) -> str | ElementTree | None:
    if oformat == "json":
        dict_list = [{headers[idx]: fix_type(v) for idx, v in enumerate(t)} for t in obj]
        return json.dumps(dict_list, indent=4)

    elif oformat == "xml":
        root_name = str(kwargs.get("name", "data"))
        root = ET.Element(root_name)

        for t in obj:
            row_element = ET.SubElement(root, "row")
            for i, v in enumerate(t):
                header_name = headers[i]
                prop_element = ET.SubElement(row_element, header_name)
                prop_element.text = str(fix_type(v))
        ET.indent(root, space="  ")
        return ET.ElementTree(root)
    else:
        print(f"Формат '{oformat}' не поддерживается!")
        return None


def write_in_file(obj_to_write: str | ET.ElementTree, f, **kwargs):
    if isinstance(obj_to_write, str):
        f.write(obj_to_write)
    elif isinstance(obj_to_write, ET.ElementTree):
        obj_to_write.write(
            kwargs.get("path", f.name), encoding="utf-8", xml_declaration=True
        )
    else: print("Объект не поддерживается для записи")


def import_from_json(db: PostgresDB, table_name: str, filepath: str):
    print(f"\nИмпортирую' {table_name}' из '{filepath}' ---")
    try:
        with open(filepath, "r", encoding="utf-8") as f:
            parsed_data: list[dict] = list(json.load(f))
        if not parsed_data: return

        if not db.check_table_exists(table_name):
            print(f"[ Error ]: таблица '{table_name}' не найдена, скип")
            return

        db.insert_data_from_list(table_name, parsed_data)

    except FileNotFoundError:
        print(f"[ Error ]: файл не найден ({filepath})")
    except json.JSONDecodeError:
        print(f"[ Error ]: ошибка чтения json ({filepath})")
    except Exception as e:
        print(f"Нежданчик: {e}")


@isdir(path=output_folder_path)
def export_table_to_file(db: PostgresDB, table_name: str, output_file=None):
    filepath = (
            output_file or os.path.join(output_folder_path, f"{table_name}_exported.{output_format}")
    )

    try:
        headers = db.get_table_headers(table_name)
        data = db.get_all_data(table_name)

        if not data or not headers: return
        transformed_data = transform_data(data, headers, name=table_name)

        if transformed_data is None: return

        with open(filepath, "w", encoding="utf-8") as f:
            write_in_file(transformed_data, f, path=filepath)

        print(f"Успех экспорта в {filepath}")

    except Exception as e:
        print(f"Нежданчик: {e}")


@isdir(path=output_folder_path)
def export_query_to_file(db: PostgresDB, query: str, report_name: str):
    filepath = os.path.join(output_folder_path, f"{report_name}.{output_format}")

    try:
        data, headers = db.execute_query(query)
        transformed_data = transform_data(data, headers, oformat=output_format, name=report_name)

        if transformed_data is None: return

        with open(filepath, "w", encoding="utf-8") as f:
            write_in_file(transformed_data, f, path=filepath)

    except Exception as e:
        print(f"Ошибка: {e}")


def main():
    init()

    with PostgresDB() as db:
        db.connect_from_env()

        import_from_json(db, "rooms", rooms_path)
        import_from_json(db, "students", students_path)

        export_table_to_file(db, "rooms")
        export_table_to_file(db, "students")

        export_query_to_file(db, query_student_count_by_room, "report_student_count_by_room")
        export_query_to_file(db, query_rooms_lowest_avg_age, "report_rooms_lowest_avg_age")
        export_query_to_file(db, query_rooms_max_age_difference, "report_rooms_max_age_difference")
        export_query_to_file(db, query_rooms_mixed_gender, "report_rooms_mixed_gender")


if __name__ == "__main__":
    main()

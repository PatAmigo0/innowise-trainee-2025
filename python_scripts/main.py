import os
import psycopg2 as pstg
from dotenv import load_dotenv, dotenv_values
import json
import datetime
import xml.etree.ElementTree as ET

# info
rooms_path = ".\\data\\rooms.json"
students_path = ".\\data\\students.json"
output_folder_path = ".\\data"
output_format = "xml"

# queries
get_all_tables_query = """
SELECT table_name 
FROM information_schema.tables
WHERE table_schema = 'public';
"""

headers_check_query_template = """
SELECT *
FROM {}
LIMIT 0;
"""

insert_query_template = """
INSERT INTO public.{}({})
VALUES ({})
ON CONFLICT(id) DO NOTHING;
"""

save_query_template = """
SELECT * FROM public.{};
"""


# functions
def get_connection():
    conn = pstg.connect(
        host=os.environ["DB_HOST"],
        dbname=os.environ["DB_NAME"],
        user=os.environ["DB_USER"],
        password=os.environ["DB_PASSWORD"],
        port=os.environ["DB_PORT"],
    )
    cur = conn.cursor()
    return conn, cur


def init():
    load_dotenv()


def get_headers(table_name: str, cur):
    cur.execute(headers_check_query_template.format(table_name))
    return [desc[0] for desc in cur.description]


def check_for_table(table_name: str, cur):
    cur.execute(get_all_tables_query)
    return table_name in [t[0] for t in cur.fetchall()]


def import_into_table(table_name: str, filepath: str):
    with open(filepath, "r") as f:
        conn, cur = get_connection()

        if not check_for_table(table_name, cur):
            print(f"No table {table_name} was found")
            return

        parsed_data: list[dict] = list(json.load(f))

        table_format = get_headers(table_name, cur)
        table_format_set = set(table_format)

        str_table_format = ", ".join(f"{v}" for v in table_format)

        insert_query = insert_query_template.format(
            table_name,
            str_table_format,
            ", ".join("%s" for _ in range(len(table_format))),
        )

        # loading into the table
        for dict_table in parsed_data:
            if not table_format_set.issubset(dict_table):
                pass
            cur.execute(insert_query, [dict_table[k] for k in table_format])

        conn.commit()
        cur.close()
        conn.close()


def is_dir(dir_path: str):
    return os.path.isdir(dir_path)


def fix_type(v: any):
    return isinstance(v, datetime.date) and v.isoformat() or v


def transform_data(obj: list[tuple[any, ...]], headers: list[str], oformat=output_format, **kwargs):
    if oformat == "json":
        return json.dumps(
            obj=[{headers[idx]: fix_type(v) for idx, v in enumerate(t)} for t in obj],
            indent=len(headers),
        )
    elif oformat == "xml":
        name = str(kwargs.get("name", "table"))
        table = ET.Element(name)
        table = ET.SubElement(table, name)

        for t in obj:
            row = ET.SubElement(table, "row")
            for i, v in enumerate(t):
                propertie = ET.SubElement(row, headers[i])
                propertie.text = str(fix_type(v))

        return ET.ElementTree(table)
    else:
        print("Format isn't supported")


def write_in_file(obj_to_write: str | ET.ElementTree, f, **kwargs):
    if isinstance(obj_to_write, str):
        f.write(obj_to_write)
    elif isinstance(obj_to_write, ET.ElementTree):
        obj_to_write.write(
            kwargs.get("path", f.name), encoding="utf-8", xml_declaration=True
        )
    else:
        print("Not supported.")


def export_from_table(table_name: str, output_file=None):
    if not is_dir(output_folder_path):
        return

    filepath = (
        output_file or output_folder_path + f"\\{table_name}_exported.{output_format}"
    )
    with open(
        filepath,
        "w",
    ) as f:
        conn, cur = get_connection()
        headers = get_headers(table_name, cur)
        cur.execute(save_query_template.format(table_name))

        write_in_file(transform_data(cur.fetchall(), headers), f)
        cur.close()
        conn.close()


def main():
    init()
    import_into_table("rooms", rooms_path)
    export_from_table("rooms")


if __name__ == "__main__":
    main()

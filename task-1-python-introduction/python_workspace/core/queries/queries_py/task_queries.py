import glob
import os


def load_sql_queries(folder_path: str, extension: str = "sql") -> dict[str, str]:
    print(f"\nЗагружаю запросы из'{folder_path}'...")
    queries_dict = {}

    file_pattern = os.path.join(folder_path, f"*.{extension}")
    filepaths = glob.glob(file_pattern)

    if not filepaths:
        print(f"[ Warning ] '.{extension}' не найдены {folder_path}.")
        return queries_dict

    for filepath in filepaths:
        try:
            query_name = os.path.splitext(os.path.basename(filepath))[0]

            with open(filepath, "r", encoding="utf-8") as f:
                query_content = f.read()
                queries_dict[query_name] = query_content
                print(f"Загрузил запрос: {query_name}")

        except Exception as e:
            print(f"Ошибка чтения {filepath}: {e}")

    return queries_dict


if __name__ != "__main__":
    adir = os.path.dirname(os.path.dirname(__file__)) # ../..
    basic_queries = load_sql_queries(os.path.abspath(f"{adir}/queries_sql"))

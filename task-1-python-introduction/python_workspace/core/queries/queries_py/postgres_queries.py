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
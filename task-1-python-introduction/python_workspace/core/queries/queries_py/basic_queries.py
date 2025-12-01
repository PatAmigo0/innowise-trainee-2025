insert_query_template = """
INSERT INTO public.{}({})
VALUES ({})
ON CONFLICT(id) DO NOTHING;
"""

save_query_template = """
SELECT * FROM public.{};
"""
FROM apache/airflow:slim-3.1.3-python3.13

RUN pip install --no-cache-dir \
    pandas \
    pymongo \
    asyncpg \
    psycopg2-binary \
    flask-appbuilder \
    flask-session \
    "connexion[flask]<3.0.0" \
    apache-airflow-providers-mongo \
    apache-airflow-providers-standard \
    apache-airflow-providers-fab
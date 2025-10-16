FROM apache/airflow:slim-3.1.0-python3.12

COPY ./requirements.txt .
USER airflow
RUN pip install -r requirements.txt
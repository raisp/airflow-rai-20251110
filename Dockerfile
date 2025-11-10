FROM apache/airflow:2.8.1

USER airflow
COPY requirements.txt /opt/airflow/.
RUN pip install -r requirements.txt

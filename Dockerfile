FROM puckel/docker-airflow:latest

COPY airflow/airflow.cfg ${AIRFLOW_HOME}/airflow.cfg

COPY requirements.txt /requirements.txt
RUN pip install -r /requirements.txt



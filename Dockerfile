FROM apache/airflow:2.7.2-python3.11

COPY ./requirements.txt ./requirements.txt
RUN pip install -r requirements.txt

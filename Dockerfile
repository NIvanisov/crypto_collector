FROM apache/airflow:2.9.2-python3.12

COPY requirements.txt .

RUN pip install --no-cache-dir "apache-airflow==2.9.2" -r requirements.txt --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-2.9.2/constraints-3.12.txt"

COPY . /opt/airflow/

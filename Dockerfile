FROM apache/airflow:2.9.2-python3.12

# Установим зависимости
COPY requirements.txt .

# В Airflow крайне важно устанавливать пакеты с использованием их официального "constraints" файла,
# иначе pip может обновить базовые библиотеки (например, SQLAlchemy), что ломает внутренности Airflow.
RUN pip install --no-cache-dir "apache-airflow==2.9.2" -r requirements.txt --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-2.9.2/constraints-3.12.txt"

# Копируем исходный код проекта
COPY . /opt/airflow/

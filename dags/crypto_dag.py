from datetime import datetime, timedelta
import os
import sys

# Добавляем корневую папку проекта в sys.path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from airflow import DAG
from airflow.operators.python import PythonOperator
from storage import get_engine, ensure_database_exists, ensure_table_exists
from collector import extract, transform, load

default_args = {
    'owner': 'crypto_admin',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'retries': 2,
    'retry_delay': timedelta(seconds=15),
}

with DAG(
    'crypto_etl_pipeline_v2',
    default_args=default_args,
    description='Разделенный ETL пайплайн (Extract -> Transform -> Load)',
    schedule_interval='* * * * *',
    catchup=False,
    max_active_runs=1,
    tags=['crypto', 'etl']
) as dag:

    def task_extract(**kwargs):
        """Шаг 1: Скачивает сырые данные (JSON) с бирж"""
        raw_data = extract()
        return raw_data  # Данные уйдут в XCom

    def task_transform(**kwargs):
        """Шаг 2: Очистка формата и сведение в таблицу"""
        ti = kwargs['ti']
        raw_data = ti.xcom_pull(task_ids='extract_raw_data')
        
        if not raw_data:
            raise ValueError("Нет сырых данных от шага скачивания.")
            
        clean_records = transform(raw_data)
        return clean_records

    def task_load(**kwargs):
        """Шаг 3: Сохранение результатов в Postgres"""
        ti = kwargs['ti']
        records = ti.xcom_pull(task_ids='transform_data')
        
        if not records:
            print("Нет очищенных данных для загрузки.")
            return
            
        ensure_database_exists()
        engine = get_engine()
        ensure_table_exists(engine)
        
        count = load(engine, records)
        print(f"Успешно обработано и сохранено: {count} торговых пар.")
        engine.dispose()

    extract_op = PythonOperator(
        task_id='extract_raw_data',
        python_callable=task_extract,
    )

    transform_op = PythonOperator(
        task_id='transform_data',
        python_callable=task_transform,
    )

    load_op = PythonOperator(
        task_id='load_to_postgres',
        python_callable=task_load,
    )

    # Идемпотентный пайплайн в стиле Data Engineering
    extract_op >> transform_op >> load_op

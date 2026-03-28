import os

BATCH_SIZE_KRAKEN = 10
SLEEP_BETWEEN_BATCHES = 1
SLEEP_BETWEEN_CYCLES = 10  # no longer strictly needed in airflow context, but kept

DB_NAME = os.getenv("DB_NAME", "crypto_prices")
DB_USER = os.getenv("DB_USER", "postgres")
DB_PASSWORD = os.getenv("DB_PASSWORD", "1234")
DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = int(os.getenv("DB_PORT", 5432))
TABLE_NAME = "prices"
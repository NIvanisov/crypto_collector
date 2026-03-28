# storage.py

import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

from config import DB_NAME, DB_USER, DB_PASSWORD, DB_HOST, DB_PORT


def ensure_database_exists():
    engine = create_engine(
        f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/postgres",
        isolation_level="AUTOCOMMIT"
    )

    with engine.connect() as conn:
        exists = conn.execute(
            text("SELECT 1 FROM pg_database WHERE datname = :name"),
            {"name": DB_NAME}
        ).fetchone()

        if not exists:
            conn.execute(text(f'CREATE DATABASE "{DB_NAME}"'))

    engine.dispose()


def get_engine() -> Engine:
    return create_engine(
        f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}",
        pool_pre_ping=True
    )


def ensure_table_exists(engine: Engine):
    sql = """
    CREATE TABLE IF NOT EXISTS prices (
        pair TEXT PRIMARY KEY,
        ts TIMESTAMPTZ NOT NULL,

        k_bid DOUBLE PRECISION,
        k_ask DOUBLE PRECISION,
        k_mid DOUBLE PRECISION,

        o_bid DOUBLE PRECISION,
        o_ask DOUBLE PRECISION,
        o_mid DOUBLE PRECISION
    );
    """
    with engine.begin() as conn:
        conn.execute(text(sql))


def upsert_prices(engine: Engine, df: pd.DataFrame):
    sql = """
    INSERT INTO prices (
        pair, ts,
        k_bid, k_ask, k_mid,
        o_bid, o_ask, o_mid
    )
    VALUES (
        :pair, :ts,
        :k_bid, :k_ask, :k_mid,
        :o_bid, :o_ask, :o_mid
    )
    ON CONFLICT (pair) DO UPDATE SET
        ts = EXCLUDED.ts,
        k_bid = EXCLUDED.k_bid,
        k_ask = EXCLUDED.k_ask,
        k_mid = EXCLUDED.k_mid,
        o_bid = EXCLUDED.o_bid,
        o_ask = EXCLUDED.o_ask,
        o_mid = EXCLUDED.o_mid;
    """

    with engine.begin() as conn:
        conn.execute(text(sql), df.to_dict(orient="records"))

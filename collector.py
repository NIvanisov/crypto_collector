# collector.py
from datetime import datetime, timezone
import time
import pandas as pd
from api import get_kraken_pairs, get_kraken_tickers, get_okx_pairs, get_okx_tickers
from normalizer import normalize_kraken, normalize_okx
from storage import upsert_prices
from config import BATCH_SIZE_KRAKEN, SLEEP_BETWEEN_BATCHES

def extract() -> dict:
    """ETL - Task 1: Fetch raw data from APIs (Бронзовый слой)"""
    kraken_raw = get_kraken_pairs()
    okx_raw = get_okx_pairs()

    kraken_map = {normalize_kraken(p): p for p in kraken_raw}
    okx_map = {normalize_okx(p): p for p in okx_raw}

    # Ищем пересечения пар на обеих биржах
    pairs = sorted(set(kraken_map) & set(okx_map))

    # Fetch Kraken
    kraken_data = {}
    targets = [kraken_map[p] for p in pairs]
    for i in range(0, len(targets), BATCH_SIZE_KRAKEN):
        tickers = get_kraken_tickers(targets[i:i + BATCH_SIZE_KRAKEN])
        kraken_data.update(tickers)
        time.sleep(SLEEP_BETWEEN_BATCHES)

    # Fetch OKX
    okx_data = get_okx_tickers()

    return {
        "kraken": kraken_data,
        "okx": okx_data,
        "pairs_intersection": pairs,
        "timestamp": datetime.now(timezone.utc).isoformat()
    }

def transform(raw_data: dict) -> list:
    """ETL - Task 2: Normalize and calculate mid prices (Серебряный слой)"""
    kraken_data = raw_data["kraken"]
    okx_data = raw_data["okx"]
    pairs = raw_data["pairs_intersection"]
    timestamp = raw_data["timestamp"]

    df = pd.DataFrame(index=pairs)
    df.index.name = "pair"
    df["ts"] = timestamp # Сохраняем строку, так как Timestamp ломает JSON XCom

    # Process Kraken
    for raw, info in kraken_data.items():
        norm = normalize_kraken(raw)
        if norm in df.index:
            try:
                bid, ask = float(info["b"][0]), float(info["a"][0])
                df.loc[norm, ["k_bid", "k_ask", "k_mid"]] = [bid, ask, (bid + ask) / 2]
            except (KeyError, IndexError, ValueError):
                continue

    # Process OKX
    for item in okx_data:
        norm = normalize_okx(item["instId"])
        if norm in df.index:
            try:
                bid, ask = float(item["bidPx"]), float(item["askPx"])
                df.loc[norm, ["o_bid", "o_ask", "o_mid"]] = [bid, ask, (bid + ask) / 2]
            except (KeyError, ValueError):
                continue

    # Удаляем пары, для которых не удалось рассчитать цены с обеих бирж (Clean Data)
    df = df.dropna(subset=['k_mid', 'o_mid'])
    df.reset_index(inplace=True)
    
    # Airflow XCom лучше всего работает с базовыми типами Python (dict, list)
    return df.to_dict(orient="records")

def load(engine, records: list) -> int:
    """ETL - Task 3: Upsert to PostgreSQL (Золотой слой)"""
    if not records:
        return 0
        
    df = pd.DataFrame(records)
    # Преобразуем строковые даты обратно в datetime для PostgreSQL
    df["ts"] = pd.to_datetime(df["ts"])
    
    upsert_prices(engine, df)
    return len(df)

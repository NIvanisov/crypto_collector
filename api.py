import requests

KRAKEN_PAIRS_URL = "https://api.kraken.com/0/public/AssetPairs"
KRAKEN_TICKER_URL = "https://api.kraken.com/0/public/Ticker"

OKX_PAIRS_URL = "https://www.okx.com/api/v5/public/instruments"
OKX_TICKERS_URL = "https://www.okx.com/api/v5/market/tickers"


def get_kraken_pairs():
    r = requests.get(KRAKEN_PAIRS_URL, timeout=10)
    r.raise_for_status()
    data = r.json()
    if data.get("error"):
        raise Exception(f"Kraken API Error: {data['error']}")
    return list(data["result"].keys())


def get_kraken_tickers(pairs):
    if not pairs:
        return {}
    r = requests.get(
        KRAKEN_TICKER_URL,
        params={"pair": ",".join(pairs)},
        timeout=10
    )
    r.raise_for_status()
    data = r.json()
    if data.get("error"):
        raise Exception(f"Kraken API Error: {data['error']}")
    return data.get("result", {})


def get_okx_pairs():
    r = requests.get(
        OKX_PAIRS_URL,
        params={"instType": "SPOT"},
        timeout=10
    )
    r.raise_for_status()
    return [item["instId"] for item in r.json()["data"]]


def get_okx_tickers():
    r = requests.get(
        OKX_TICKERS_URL,
        params={"instType": "SPOT"},
        timeout=10
    )
    r.raise_for_status()
    return r.json()["data"]
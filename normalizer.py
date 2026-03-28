# normalizer.py

SYMBOL_MAP = {
    "XXBT": "BTC",
    "XBT": "BTC",
    "XXDG": "DOGE",
    "XDG": "DOGE",
    "ZUSD": "USD",
    "ZEUR": "EUR",
    "ZUSDT": "USDT",
    "XETH": "ETH",
    "ZGBP": "GBP",
    "ZCAD": "CAD",
    "ZJPY": "JPY",
    "XXRP": "XRP",
    "XLTC": "LTC"
}

def normalize_kraken(symbol: str) -> str:
    for k, v in SYMBOL_MAP.items():
        symbol = symbol.replace(k, v)
    return symbol

def normalize_okx(symbol: str) -> str:
    return symbol.replace("-", "")

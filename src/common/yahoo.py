import yfinance as yf
from typing import Optional

def _yf_last_close(symbol: str, interval: str = "5m") -> Optional[float]:
    try:
        data = yf.download(symbol, period="1d", interval=interval, progress=False)
        if data is None or data.empty or "Close" not in data:
            return None
        s = data["Close"].dropna()
        if s.empty:
            return None
        return float(s.iloc[-1].item())
    except:
        return None

def stock_usd_price(ticker: str) -> Optional[float]:
    return _yf_last_close(ticker, interval="5m")

def stock_usd_change_5m_pct(ticker: str) -> Optional[float]:
    """
    Aproxima variación 5m usando dos últimas velas 5m del día.
    """
    try:
        data = yf.download(ticker, period="1d", interval="5m", progress=False)
        if data is None or data.empty or "Close" not in data:
            return None
        s = data["Close"].dropna()
        if len(s) < 2:
            return None
        prev = float(s.iloc[-2].item())
        last = float(s.iloc[-1].item())
        if prev <= 0:
            return None
        return (last - prev) / prev * 100.0
    except:
        return None

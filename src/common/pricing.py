from typing import Optional, Dict, Any, List
from src.common.iol import IOLClient, get_last_price
from src.common.yahoo import stock_usd_price
from src.common.sheets import get_all_records


def _find_latest_price_in_sheet(sheet, tab_name: str, ticker: str) -> Optional[float]:
    try:
        rows = get_all_records(sheet, tab_name)
    except Exception:
        return None
    # search from bottom for latest
    for r in reversed(rows):
        if (r.get("ticker") or "").strip().upper() == ticker.upper():
            p = r.get("price_ars") or r.get("price") or r.get("price_ars")
            try:
                if p is None:
                    return None
                if isinstance(p, str):
                    p = p.replace(',', '')
                return float(p)
            except Exception:
                return None
    return None


def get_ars_price(sheet, ticker: str, portfolio_row: Optional[Dict[str, Any]] = None, iol: Optional[IOLClient] = None) -> Optional[float]:
    # 1) portfolio explicit override
    if portfolio_row:
        for key in ("last_price", "last_ars", "last"):
            v = portfolio_row.get(key)
            if v:
                try:
                    if isinstance(v, str):
                        v = v.replace(',', '')
                    return float(v)
                except Exception:
                    pass

    # 2) prices_daily sheet
    try:
        p = _find_latest_price_in_sheet(sheet, "prices_daily", ticker)
        if p:
            return p
    except Exception:
        pass

    # 3) IOL if available
    if iol:
        try:
            q = get_last_price(iol, "bcba", ticker)
            if q:
                return q
        except Exception:
            pass

    return None


def get_usd_price(ticker: str) -> Optional[float]:
    try:
        return stock_usd_price(ticker)
    except Exception:
        return None

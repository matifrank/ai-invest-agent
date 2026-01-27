import os
import json
import time
import requests
import yfinance as yf
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from datetime import date
from typing import Optional, Dict, Any, List, Tuple

# =========================
# CONFIG
# =========================
SPREADSHEET_NAME = "ai-portfolio-agent"

PORTFOLIO_SHEET = "portfolio"              # ticker | tipo | cantidad | ppc | last_price | ratio
WATCHLIST_SHEET = "watchlist"              # ticker | tipo | ratio
PRICES_SHEET = "prices_daily"              # date | ticker | price | source

PORTFOLIO_HISTORY_SHEET = "portfolio_history_v2"
WATCHLIST_HISTORY_SHEET = "watchlist_history_v2"

# Portfolio valuation mode (broker-like)
PORTFOLIO_PRICE_MODE = "mark"  # mark | bid | ask | last

# Costs and thresholds
BROKER_FEE_PCT = 0.5  # per transaction
WATCH_MIN_DIFF_PCT = 1.5  # % threshold vs market CCL
WATCH_MIN_NET_USD_PER_CEDEAR = 0.05  # USD/CEDEAR after estimated fees

IOL_BASE = "https://api.invertironline.com"
IOL_MERCADO = "bcba"  # CEDEARs/acciones locales

# =========================
# SHEETS
# =========================
def connect_sheets():
    scope = [
        "https://spreadsheets.google.com/feeds",
        "https://www.googleapis.com/auth/drive",
    ]
    creds_json = os.environ["PORTFOLIO_GS_CREDS"]
    creds_dict = json.loads(creds_json)
    creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, scope)
    client = gspread.authorize(creds)
    return client.open(SPREADSHEET_NAME)

def ensure_worksheet(sheet, title: str, rows: int = 2000, cols: int = 40, header: Optional[List[str]] = None):
    try:
        ws = sheet.worksheet(title)
    except gspread.exceptions.WorksheetNotFound:
        ws = sheet.add_worksheet(title=title, rows=rows, cols=cols)

    if header:
        values = ws.get_all_values()
        if not values:
            ws.append_row(header)
        else:
            # Ensure row 1 is the expected header; do NOT create new tabs.
            if values[0] != header:
                ws.update("1:1", [header])
    return ws

def get_all_records(sheet, tab_name: str) -> List[Dict[str, Any]]:
    return sheet.worksheet(tab_name).get_all_records()

def append_price_daily(sheet, ticker: str, price: float, source: str):
    ws = sheet.worksheet(PRICES_SHEET)
    ws.append_row([str(date.today()), ticker, price, source])

def update_portfolio_last_price(sheet, ticker: str, last_price: float):
    ws = sheet.worksheet(PORTFOLIO_SHEET)
    cells = ws.findall(ticker)
    for c in cells:
        ws.update_cell(c.row, 5, last_price)  # last_price col (E)

# =========================
# UTILS
# =========================
def safe_float(x) -> Optional[float]:
    try:
        if x is None:
            return None
        if isinstance(x, str) and x.strip() == "":
            return None
        return float(x)
    except:
        return None

# =========================
# YAHOO FALLBACK
# =========================
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

def yahoo_cedear_price_ars(ticker: str) -> Optional[float]:
    return _yf_last_close(f"{ticker}.BA", interval="5m")

def yahoo_stock_price_usd(ticker: str) -> Optional[float]:
    return _yf_last_close(ticker, interval="5m")

# =========================
# IOL CLIENT
# =========================
class IOLClient:
    def __init__(self, username: str, password: str):
        self.username = username
        self.password = password
        self.access_token: Optional[str] = None
        self.refresh_token: Optional[str] = None
        self.expires_at: float = 0.0

    def login_password(self):
        r = requests.post(
            f"{IOL_BASE}/token",
            data={
                "username": self.username,
                "password": self.password,
                "grant_type": "password",
            },
            timeout=15,
        )
        r.raise_for_status()
        j = r.json()
        self.access_token = j.get("access_token")
        self.refresh_token = j.get("refresh_token")
        expires_in = float(j.get("expires_in", 900))
        self.expires_at = time.time() + expires_in - 20
        if not self.access_token:
            raise RuntimeError(f"IOL login sin access_token: {j}")

    def refresh(self):
        if not self.refresh_token:
            self.login_password()
            return
        r = requests.post(
            f"{IOL_BASE}/token",
            data={"refresh_token": self.refresh_token, "grant_type": "refresh_token"},
            timeout=15,
        )
        if r.status_code >= 400:
            self.login_password()
            return
        j = r.json()
        self.access_token = j.get("access_token")
        self.refresh_token = j.get("refresh_token", self.refresh_token)
        expires_in = float(j.get("expires_in", 900))
        self.expires_at = time.time() + expires_in - 20

    def ensure_token(self):
        if not self.access_token or time.time() >= self.expires_at:
            if self.refresh_token:
                self.refresh()
            else:
                self.login_password()

    def headers(self) -> Dict[str, str]:
        self.ensure_token()
        return {"Authorization": f"Bearer {self.access_token}"}

    def get_quote(self, mercado: str, simbolo: str) -> Optional[Dict[str, Any]]:
        url = f"{IOL_BASE}/api/v2/{mercado}/Titulos/{simbolo}/Cotizacion"
        try:
            r = requests.get(url, headers=self.headers(), timeout=15)
            if r.status_code == 401:
                self.refresh()
                r = requests.get(url, headers=self.headers(), timeout=15)
            if r.status_code >= 400:
                return None
            return r.json()
        except:
            return None

def parse_iol_quote(q: Dict[str, Any]) -> Tuple[Optional[float], Optional[float], Optional[float]]:
    last = safe_float(q.get("ultimoPrecio"))
    bid = None
    ask = None
    puntas = q.get("puntas") or []
    if isinstance(puntas, list) and len(puntas) > 0 and isinstance(puntas[0], dict):
        bid = safe_float(puntas[0].get("precioCompra"))
        ask = safe_float(puntas[0].get("precioVenta"))
    return last, bid, ask

# =========================
# CCL / MATH
# =========================
def get_ccl_market() -> Optional[float]:
    try:
        url = "https://dolarapi.com/v1/dolares"
        r = requests.get(url, timeout=10)
        data = r.json()
        for item in data:
            if item.get("casa") == "contadoconliqui":
                return float(item["venta"])
        return None
    except:
        return None

def ccl_implicit(cedear_ars: float, stock_usd: float, ratio: float) -> Optional[float]:
    if not cedear_ars or not stock_usd or not ratio:
        return None
    if stock_usd <= 0 or ratio <= 0:
        return None
    return (cedear_ars * ratio) / stock_usd

def usd_value(qty: float, cedear_ars: float, ccl_impl: float) -> float:
    if not ccl_impl or ccl_impl <= 0:
        return 0.0
    return (qty * cedear_ars) / ccl_impl

def gain_usd(qty: float, ppc_ars: float, current_ars: float, ccl_impl: float) -> float:
    if ppc_ars is None or not ccl_impl or ccl_impl <= 0:
        return 0.0
    return qty * (current_ars - ppc_ars) / ccl_impl

def usd_per_cedear(price_ars: float, ccl_mkt: float) -> Optional[float]:
    if not price_ars or not ccl_mkt or ccl_mkt <= 0:
        return None
    return price_ars / ccl_mkt

def usd_stock_per_cedear(stock_usd: float, ratio: float) -> Optional[float]:
    if not stock_usd or not ratio or ratio <= 0:
        return None
    return stock_usd / ratio

def fee_roundtrip_usd(usd_base: float, fee_pct_per_tx: float) -> Optional[float]:
    if usd_base is None:
        return None
    return usd_base * ((2 * fee_pct_per_tx) / 100.0)

def edges_intuitive(
    bid_ars: float, ask_ars: float, stock_usd: float, ratio: float, ccl_mkt: float, fee_pct_per_tx: float
) -> Optional[Dict[str, float]]:
    """
    Intuitive edges (USD/CEDEAR):
      usd_cedear_ask = ask_ars / ccl_mkt
      usd_cedear_bid = bid_ars / ccl_mkt
      usd_stock_per_cedear = stock_usd / ratio

      BUY edge (cheap CEDEAR):  usd_stock_per_cedear - usd_cedear_ask
      SELL edge (expensive CEDEAR): usd_cedear_bid - usd_stock_per_cedear
    """
    usd_ask = usd_per_cedear(ask_ars, ccl_mkt)
    usd_bid = usd_per_cedear(bid_ars, ccl_mkt)
    usd_stock = usd_stock_per_cedear(stock_usd, ratio)
    if usd_ask is None or usd_bid is None or usd_stock is None:
        return None

    edge_buy_gross = usd_stock - usd_ask
    edge_sell_gross = usd_bid - usd_stock

    fee_buy = fee_roundtrip_usd(usd_ask, fee_pct_per_tx) or 0.0
    fee_sell = fee_roundtrip_usd(usd_bid, fee_pct_per_tx) or 0.0

    edge_buy_net = edge_buy_gross - fee_buy
    edge_sell_net = edge_sell_gross - fee_sell

    return {
        "usd_cedear_ask": usd_ask,
        "usd_cedear_bid": usd_bid,
        "usd_stock_per_cedear": usd_stock,
        "edge_buy_gross": edge_buy_gross,
        "edge_sell_gross": edge_sell_gross,
        "fee_buy_usd_rt": fee_buy,
        "fee_sell_usd_rt": fee_sell,
        "edge_buy_net": edge_buy_net,
        "edge_sell_net": edge_sell_net,
    }

# =========================
# PRICE FETCHERS
# =========================
def get_cedear_quote_ars(ticker: str, iol: Optional[IOLClient]) -> Tuple[Optional[float], Optional[float], Optional[float], str]:
    if iol:
        q = iol.get_quote(IOL_MERCADO, ticker)
        if q:
            last, bid, ask = parse_iol_quote(q)
            if last is not None or bid is not None or ask is not None:
                return last, bid, ask, "IOL"
    y = yahoo_cedear_price_ars(ticker)
    return y, None, None, "YAHOO"

def get_stock_usd_price(ticker: str) -> Optional[float]:
    return yahoo_stock_price_usd(ticker)

def pick_portfolio_price(last: Optional[float], bid: Optional[float], ask: Optional[float]) -> Optional[float]:
    mark = (bid + ask) / 2.0 if (bid is not None and ask is not None) else last
    if PORTFOLIO_PRICE_MODE == "bid" and bid is not None:
        return bid
    if PORTFOLIO_PRICE_MODE == "ask" and ask is not None:
        return ask
    if PORTFOLIO_PRICE_MODE == "last" and last is not None:
        return last
    return mark

# =========================
# TELEGRAM
# =========================
def send_telegram(msg: str):
    token = os.environ["TELEGRAM_TOKEN"]
    chat_id = os.environ["TELEGRAM_CHAT_ID"]
    url = f"https://api.telegram.org/bot{token}/sendMessage"
    r = requests.post(url, json={"chat_id": chat_id, "text": msg}, timeout=10)
    print("📨 Telegram status:", r.status_code)
    print("📨 Telegram response:", r.text)

# =========================
# MAIN
# =========================
def main():
    print("🚀 Iniciando pipeline")
    sheet = connect_sheets()

    ws_port_hist = ensure_worksheet(
        sheet,
        PORTFOLIO_HISTORY_SHEET,
        header=[
            "date", "ticker", "qty", "ppc_ars",
            "mark_ars", "bid_ars", "ask_ars",
            "ratio", "stock_usd", "ccl_impl",
            "usd_value", "gain_usd", "source"
        ],
    )
    ws_watch_hist = ensure_worksheet(
        sheet,
        WATCHLIST_HISTORY_SHEET,
        header=[
            "date","ticker","ratio","stock_usd","bid_ars","ask_ars",
            "ccl_buy","ccl_sell","ccl_mkt","diff_buy_pct","diff_sell_pct",
            "usd_cedear_ask","usd_cedear_bid","usd_stock_per_cedear",
            "edge_buy_gross","edge_sell_gross",
            "fee_buy_usd_rt","fee_sell_usd_rt",
            "edge_buy_net","edge_sell_net",
            "source"
        ],
    )

    portfolio = get_all_records(sheet, PORTFOLIO_SHEET)
    watchlist = get_all_records(sheet, WATCHLIST_SHEET)
    ccl_mkt = get_ccl_market()
    today = str(date.today())

    iol = None
    if os.environ.get("IOL_USERNAME") and os.environ.get("IOL_PASSWORD"):
        iol = IOLClient(os.environ["IOL_USERNAME"], os.environ["IOL_PASSWORD"])

    # ---------- PORTFOLIO ----------
    total_ars = 0.0
    total_usd = 0.0
    dist: Dict[str, Tuple[float, float, float]] = {}  # usd_value, gain_usd, ccl_impl

    for p in portfolio:
        ticker = (p.get("ticker") or "").strip()
        tipo = (p.get("tipo") or "").upper().strip()
        qty = safe_float(p.get("cantidad"))
        ppc = safe_float(p.get("ppc"))
        ratio = safe_float(p.get("ratio")) or 1.0

        if not ticker or not qty or tipo != "CEDEAR":
            continue

        last, bid, ask, src = get_cedear_quote_ars(ticker, iol)
        price = pick_portfolio_price(last, bid, ask)
        if price is None:
            continue

        mark = (bid + ask) / 2.0 if (bid is not None and ask is not None) else last

        stock_usd = get_stock_usd_price(ticker)
        if stock_usd is None:
            continue

        ccl_impl_now = ccl_implicit(price, stock_usd, ratio)
        if not ccl_impl_now:
            continue

        usd_val = usd_value(qty, price, ccl_impl_now)
        gain = gain_usd(qty, ppc, price, ccl_impl_now)

        total_ars += qty * price
        total_usd += usd_val
        dist[ticker] = (usd_val, gain, ccl_impl_now)

        update_portfolio_last_price(sheet, ticker, price)
        append_price_daily(sheet, ticker, price, src)

        ws_port_hist.append_row([
            today, ticker, qty, ppc,
            mark if mark is not None else "",
            bid if bid is not None else "",
            ask if ask is not None else "",
            ratio, stock_usd, ccl_impl_now,
            usd_val, gain, src
        ])

    # ---------- WATCHLIST ----------
    watch_opps: List[str] = []

    for w in watchlist:
        ticker = (w.get("ticker") or "").strip()
        tipo = (w.get("tipo") or "").upper().strip()
        ratio = safe_float(w.get("ratio")) or 1.0
        if not ticker or tipo != "CEDEAR":
            continue

        last, bid, ask, src = get_cedear_quote_ars(ticker, iol)
        stock_usd = get_stock_usd_price(ticker)
        if stock_usd is None:
            continue

        if not ccl_mkt or bid is None or ask is None:
            ws_watch_hist.append_row([
                today, ticker, ratio, stock_usd,
                bid if bid is not None else "",
                ask if ask is not None else "",
                "", "", ccl_mkt if ccl_mkt else "",
                "", "", "", "", "", "", "", "", "", "", src
            ])
            continue

        ccl_buy = ccl_implicit(ask, stock_usd, ratio)
        ccl_sell = ccl_implicit(bid, stock_usd, ratio)
        if not ccl_buy or not ccl_sell:
            continue

        diff_buy = (ccl_buy - ccl_mkt) / ccl_mkt * 100
        diff_sell = (ccl_sell - ccl_mkt) / ccl_mkt * 100

        pack = edges_intuitive(bid, ask, stock_usd, ratio, ccl_mkt, BROKER_FEE_PCT)
        if not pack:
            continue

        usd_ask = pack["usd_cedear_ask"]
        usd_bid = pack["usd_cedear_bid"]
        usd_stock = pack["usd_stock_per_cedear"]
        edge_buy_gross = pack["edge_buy_gross"]
        edge_sell_gross = pack["edge_sell_gross"]
        fee_buy = pack["fee_buy_usd_rt"]
        fee_sell = pack["fee_sell_usd_rt"]
        edge_buy_net = pack["edge_buy_net"]
        edge_sell_net = pack["edge_sell_net"]

        # BUY opportunity
        if diff_buy <= -WATCH_MIN_DIFF_PCT and edge_buy_net >= WATCH_MIN_NET_USD_PER_CEDEAR:
            watch_opps.append(
                f"⚡ {ticker} COMPRA (ASK) diff {diff_buy:+.1f}% | "
                f"usd ask {usd_ask:.2f} vs stock {usd_stock:.2f} | "
                f"bruto {edge_buy_gross:.2f} | fees {fee_buy:.2f} | neto {edge_buy_net:.2f} USD/CEDEAR"
            )

        # SELL opportunity
        if diff_sell >= WATCH_MIN_DIFF_PCT and edge_sell_net >= WATCH_MIN_NET_USD_PER_CEDEAR:
            watch_opps.append(
                f"⚡ {ticker} VENTA (BID) diff {diff_sell:+.1f}% | "
                f"usd bid {usd_bid:.2f} vs stock {usd_stock:.2f} | "
                f"bruto {edge_sell_gross:.2f} | fees {fee_sell:.2f} | neto {edge_sell_net:.2f} USD/CEDEAR"
            )

        ws_watch_hist.append_row([
            today, ticker, ratio, stock_usd, bid, ask,
            ccl_buy, ccl_sell, ccl_mkt,
            diff_buy, diff_sell,
            usd_ask, usd_bid, usd_stock,
            edge_buy_gross, edge_sell_gross,
            fee_buy, fee_sell,
            edge_buy_net, edge_sell_net,
            src
        ])

    # ---------- TELEGRAM ----------
    msg = (
        "📊 AI Portfolio Daily - Broker Mode\n\n"
        f"Valor ARS: ${total_ars:,.0f}\n"
        f"Valor USD real (CCL implícito por activo): ${total_usd:,.2f}\n\n"
        "Distribución principal:\n"
    )

    for t, (usd_val, gain, ccl_impl_now) in sorted(dist.items(), key=lambda kv: kv[1][0], reverse=True)[:3]:
        msg += f"- {t}: ${usd_val:,.2f} ({gain:+.2f} USD, CCL {ccl_impl_now:.0f})\n"

    msg += "\n🚨 Ganancia / pérdida cartera:\n"
    for t, (_, gain, _) in sorted(dist.items(), key=lambda kv: kv[1][0], reverse=True):
        msg += f"{'📈' if gain >= 0 else '📉'} {t}: {gain:+.2f} USD\n"

    msg += f"\n👀 Watchlist oportunidades (umbral {WATCH_MIN_DIFF_PCT:.1f}% | fee {BROKER_FEE_PCT:.1f}%/tx):\n"
    msg += ("\n".join(watch_opps) + "\n") if watch_opps else "(sin oportunidades relevantes hoy)\n"
    msg += "\nPipeline funcionando 🤖"

    send_telegram(msg)

if __name__ == "__main__":
    main()

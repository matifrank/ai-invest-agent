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

PORTFOLIO_SHEET = "portfolio"
WATCHLIST_SHEET = "watchlist"
PRICES_SHEET = "prices_daily"

# Use NEW sheets to avoid old headers messing columns
PORTFOLIO_HISTORY_SHEET = "portfolio_history_v2"
WATCHLIST_HISTORY_SHEET = "watchlist_history_v2"

PORTFOLIO_PRICE_MODE = "mark"  # mark | bid | ask | last

BROKER_FEE_PCT = 0.5
WATCH_MIN_DIFF_PCT = 1.5
WATCH_MIN_NET_USD_PER_CEDEAR = 0.05

IOL_BASE = "https://api.invertironline.com"
IOL_MERCADO = "bcba"

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
            # if header mismatch, do not append into wrong schema
            if values[0] != header:
                # create a new sheet with timestamp-like suffix to avoid corruption
                # (simple deterministic suffix based on date)
                new_title = f"{title}_{str(date.today()).replace('-', '')}"
                try:
                    ws2 = sheet.worksheet(new_title)
                except gspread.exceptions.WorksheetNotFound:
                    ws2 = sheet.add_worksheet(title=new_title, rows=rows, cols=cols)
                    ws2.append_row(header)
                return ws2
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
            data={"username": self.username, "password": self.password, "grant_type": "password"},
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

def edge_gross(cedear_ars: float, ccl_mkt: float, ccl_impl: float) -> Optional[float]:
    if not ccl_mkt or ccl_mkt <= 0 or not ccl_impl or ccl_impl <= 0:
        return None
    usd_mkt = cedear_ars / ccl_mkt
    usd_impl = cedear_ars / ccl_impl
    return usd_impl - usd_mkt

def edge_net_abs(cedear_ars: float, ccl_mkt: float, ccl_impl: float, fee_pct_per_tx: float) -> Optional[Tuple[float, float, float]]:
    g = edge_gross(cedear_ars, ccl_mkt, ccl_impl)
    if g is None:
        return None
    usd_mkt = cedear_ars / ccl_mkt
    fee_usd = usd_mkt * ((2 * fee_pct_per_tx) / 100.0)
    gross_abs = abs(g)
    net = max(gross_abs - fee_usd, 0.0)
    return gross_abs, fee_usd, net

# =========================
# PRICE FETCHERS
# =========================
def get_cedear_quote_ars(ticker: str, iol: Optional[IOLClient]) -> Tuple[Optional[float], Optional[float], Optional[float], str]:
    if iol:
        q = iol.get_quote(IOL_MERCADO, ticker)
        if q:
            last, bid, ask = parse_iol_quote(q)
            # consider valid if we got at least last or bid/ask
            if last is not None or bid is not None or ask is not None:
                return last, bid, ask, "IOL"
    # fallback
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
        header=["date", "ticker", "qty", "ppc_ars", "mark_ars", "bid_ars", "ask_ars",
                "ratio", "stock_usd", "ccl_impl", "usd_value", "gain_usd", "source"],
    )
    ws_watch_hist = ensure_worksheet(
        sheet,
        WATCHLIST_HISTORY_SHEET,
        header=["date", "ticker", "ratio", "stock_usd", "bid_ars", "ask_ars",
                "ccl_buy", "ccl_sell", "ccl_mkt",
                "diff_buy_pct", "diff_sell_pct",
                "gross_buy_abs", "gross_sell_abs",
                "fee_buy_usd_rt", "fee_sell_usd_rt",
                "net_buy", "net_sell", "source"],
    )

    portfolio = get_all_records(sheet, PORTFOLIO_SHEET)
    watchlist = get_all_records(sheet, WATCHLIST_SHEET)
    ccl_mkt = get_ccl_market()
    today = str(date.today())

    # IMPORTANT: ensure secrets are passed in workflow
    iol = None
    if os.environ.get("IOL_USERNAME") and os.environ.get("IOL_PASSWORD"):
        iol = IOLClient(os.environ["IOL_USERNAME"], os.environ["IOL_PASSWORD"])

    # ---------- PORTFOLIO ----------
    total_ars = 0.0
    total_usd = 0.0
    dist: Dict[str, Tuple[float, float, float]] = {}

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
            ratio, stock_usd, ccl_impl_now, usd_val, gain, src
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
                "", "", "", "", "", "", "", "", src
            ])
            continue

        ccl_buy = ccl_implicit(ask, stock_usd, ratio)
        ccl_sell = ccl_implicit(bid, stock_usd, ratio)
        if not ccl_buy or not ccl_sell:
            continue

        diff_buy = (ccl_buy - ccl_mkt) / ccl_mkt * 100
        diff_sell = (ccl_sell - ccl_mkt) / ccl_mkt * 100

        buy_pack = edge_net_abs(ask, ccl_mkt, ccl_buy, BROKER_FEE_PCT)
        sell_pack = edge_net_abs(bid, ccl_mkt, ccl_sell, BROKER_FEE_PCT)

        gross_buy_abs, fee_buy, net_buy = buy_pack if buy_pack else (None, None, None)
        gross_sell_abs, fee_sell, net_sell = sell_pack if sell_pack else (None, None, None)

        # BUY opp: diff_buy negative enough + net positive enough
        if diff_buy <= -WATCH_MIN_DIFF_PCT and net_buy is not None and net_buy >= WATCH_MIN_NET_USD_PER_CEDEAR:
            watch_opps.append(
                f"⚡ {ticker} COMPRA (ASK) diff {diff_buy:+.1f}% | bruto {gross_buy_abs:.2f} | fees {fee_buy:.2f} | neto {net_buy:.2f} USD/CEDEAR"
            )

        # SELL opp: diff_sell positive enough + net positive enough
        if diff_sell >= WATCH_MIN_DIFF_PCT and net_sell is not None and net_sell >= WATCH_MIN_NET_USD_PER_CEDEAR:
            watch_opps.append(
                f"⚡ {ticker} VENTA (BID) diff {diff_sell:+.1f}% | bruto {gross_sell_abs:.2f} | fees {fee_sell:.2f} | neto {net_sell:.2f} USD/CEDEAR"
            )

        ws_watch_hist.append_row([
            today, ticker, ratio, stock_usd, bid, ask,
            ccl_buy, ccl_sell, ccl_mkt,
            diff_buy, diff_sell,
            gross_buy_abs if gross_buy_abs is not None else "",
            gross_sell_abs if gross_sell_abs is not None else "",
            fee_buy if fee_buy is not None else "",
            fee_sell if fee_sell is not None else "",
            net_buy if net_buy is not None else "",
            net_sell if net_sell is not None else "",
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

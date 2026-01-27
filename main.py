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
PRICES_SHEET = "prices_daily"              # date | ticker | price

PORTFOLIO_HISTORY_SHEET = "portfolio_history"
WATCHLIST_HISTORY_SHEET = "watchlist_history"

# Broker-like valuation mode for portfolio
PORTFOLIO_PRICE_MODE = "mark"  # mark | bid | ask | last

# Costs and thresholds
BROKER_FEE_PCT = 0.5  # per transaction (e.g., 0.5%)
WATCH_MIN_DIFF_PCT = 1.5  # min |diff| to alert (recommended with 0.5% fee)
WATCH_MIN_EDGE_USD_PER_CEDEAR = 0.05       # gross sanity filter
WATCH_MIN_NET_USD_PER_CEDEAR = 0.05        # net filter (after estimated fees)

IOL_BASE = "https://api.invertironline.com"
IOL_MERCADO = "bcba"  # CEDEARs / acciones locales

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
        elif len(values) == 1 and (values[0] == [""] or values[0] == []):
            ws.clear()
            ws.append_row(header)
        elif values and values[0] != header:
            # no pisamos historial existente
            pass
    return ws

def get_all_records(sheet, tab_name: str) -> List[Dict[str, Any]]:
    return sheet.worksheet(tab_name).get_all_records()

def append_price_daily(sheet, ticker: str, price: float):
    ws = sheet.worksheet(PRICES_SHEET)
    ws.append_row([str(date.today()), ticker, price])

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
            data={
                "refresh_token": self.refresh_token,
                "grant_type": "refresh_token",
            },
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

def parse_iol_quote(q: Dict[str, Any]) -> Dict[str, Optional[float]]:
    """
    JSON real IOL:
      - ultimoPrecio
      - puntas[0].precioCompra (bid)
      - puntas[0].precioVenta (ask)
    """
    last = safe_float(q.get("ultimoPrecio"))
    bid = None
    ask = None
    puntas = q.get("puntas") or []
    if isinstance(puntas, list) and len(puntas) > 0 and isinstance(puntas[0], dict):
        bid = safe_float(puntas[0].get("precioCompra"))
        ask = safe_float(puntas[0].get("precioVenta"))
    return {"last": last, "bid": bid, "ask": ask}

# =========================
# FX / CCL
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

def usd_value_broker_mode(qty: float, cedear_ars: float, ccl_impl: float) -> float:
    if not ccl_impl or ccl_impl <= 0:
        return 0.0
    return (qty * cedear_ars) / ccl_impl

def gain_usd_broker_mode(qty: float, ppc_ars: float, last_ars: float, ccl_impl_now: float) -> float:
    if not ccl_impl_now or ccl_impl_now <= 0 or ppc_ars is None:
        return 0.0
    return qty * (last_ars - ppc_ars) / ccl_impl_now

def edge_usd_per_cedear(cedear_ars: float, ccl_market: float, ccl_impl: float) -> Optional[float]:
    if not ccl_market or ccl_market <= 0 or not ccl_impl or ccl_impl <= 0:
        return None
    usd_mkt = cedear_ars / ccl_market
    usd_impl = cedear_ars / ccl_impl
    return usd_impl - usd_mkt

def usd_at_ccl_mkt(cedear_ars: float, ccl_market: float) -> Optional[float]:
    if not cedear_ars or not ccl_market or ccl_market <= 0:
        return None
    return cedear_ars / ccl_market

def edge_net_usd_per_cedear(cedear_ars: float, ccl_market: float, ccl_impl: float, fee_pct_per_tx: float) -> Optional[Tuple[float, float, float]]:
    """
    Returns: (edge_gross_abs, fee_usd_roundtrip, edge_net)
    edge_gross_abs: |edge gross| USD/CEDEAR
    fee_usd_roundtrip: USD/CEDEAR estimated fees for 2 transactions
    edge_net: max(|edge gross| - fees, 0)
    """
    gross = edge_usd_per_cedear(cedear_ars, ccl_market, ccl_impl)
    if gross is None:
        return None
    usd_mkt = usd_at_ccl_mkt(cedear_ars, ccl_market)
    if usd_mkt is None:
        return None
    round_trip_fee_pct = 2 * fee_pct_per_tx
    fee_usd = usd_mkt * (round_trip_fee_pct / 100.0)
    gross_abs = abs(gross)
    net = max(gross_abs - fee_usd, 0.0)
    return gross_abs, fee_usd, net

# =========================
# PRICE FETCHERS (IOL + fallback)
# =========================
def get_cedear_quote_ars(ticker: str, iol: Optional[IOLClient]) -> Tuple[Optional[float], Optional[float], Optional[float], str]:
    """
    Returns (last, bid, ask, source)
    """
    if iol:
        q = iol.get_quote(IOL_MERCADO, ticker)
        if q:
            px = parse_iol_quote(q)
            return px["last"], px["bid"], px["ask"], "IOL"
    y = yahoo_cedear_price_ars(ticker)
    return y, None, None, "YAHOO"

def get_stock_usd_price(ticker: str) -> Optional[float]:
    return yahoo_stock_price_usd(ticker)

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
        header=["date", "ticker", "qty", "ppc_ars", "mark_ars", "bid_ars", "ask_ars", "ratio", "stock_usd",
                "ccl_impl", "usd_value", "gain_usd", "source"],
    )
    ws_watch_hist = ensure_worksheet(
        sheet,
        WATCHLIST_HISTORY_SHEET,
        header=["date", "ticker", "ratio", "stock_usd", "bid_ars", "ask_ars",
                "ccl_buy", "ccl_sell", "ccl_mkt",
                "diff_buy_pct", "diff_sell_pct",
                "edge_buy_gross_abs", "edge_sell_gross_abs",
                "fee_buy_usd_rt", "fee_sell_usd_rt",
                "edge_buy_net", "edge_sell_net",
                "source"],
    )

    portfolio = get_all_records(sheet, PORTFOLIO_SHEET)
    watchlist = get_all_records(sheet, WATCHLIST_SHEET)
    ccl_mkt = get_ccl_market()

    # IOL client if secrets exist
    iol = None
    if os.environ.get("IOL_USERNAME") and os.environ.get("IOL_PASSWORD"):
        iol = IOLClient(os.environ["IOL_USERNAME"], os.environ["IOL_PASSWORD"])

    today = str(date.today())

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

        last, bid, ask, source = get_cedear_quote_ars(ticker, iol=iol)
        if last is None and bid is None and ask is None:
            print(f"⚠️ Portfolio skip {ticker}: sin precio")
            continue

        # MARK price
        mark = (bid + ask) / 2.0 if (bid is not None and ask is not None) else last
        # Portfolio valuation price according to mode
        valuation_price = mark
        if PORTFOLIO_PRICE_MODE == "bid" and bid is not None:
            valuation_price = bid
        elif PORTFOLIO_PRICE_MODE == "ask" and ask is not None:
            valuation_price = ask
        elif PORTFOLIO_PRICE_MODE == "last" and last is not None:
            valuation_price = last

        if valuation_price is None:
            continue

        # Update sheet + daily prices
        update_portfolio_last_price(sheet, ticker, valuation_price)
        append_price_daily(sheet, ticker, valuation_price)

        stock_usd = get_stock_usd_price(ticker)
        if stock_usd is None:
            print(f"⚠️ Portfolio skip {ticker}: sin stock USD")
            continue

        ccl_impl_now = ccl_implicit(valuation_price, stock_usd, ratio)
        if not ccl_impl_now:
            continue

        usd_val = usd_value_broker_mode(qty, valuation_price, ccl_impl_now)
        gain_usd = gain_usd_broker_mode(qty, ppc, valuation_price, ccl_impl_now)

        total_ars += qty * valuation_price
        total_usd += usd_val
        dist[ticker] = (usd_val, gain_usd, ccl_impl_now)

        ws_port_hist.append_row([
            today, ticker, qty, ppc, mark, bid if bid is not None else "", ask if ask is not None else "",
            ratio, stock_usd, ccl_impl_now, usd_val, gain_usd, source
        ])

    # ---------- WATCHLIST (gross + net, bid/ask) ----------
    watch_opps: List[str] = []
    min_diff_pct = WATCH_MIN_DIFF_PCT

    for w in watchlist:
        ticker = (w.get("ticker") or "").strip()
        tipo = (w.get("tipo") or "").upper().strip()
        ratio = safe_float(w.get("ratio")) or 1.0
        if not ticker or tipo != "CEDEAR":
            continue

        last, bid, ask, source = get_cedear_quote_ars(ticker, iol=iol)
        stock_usd = get_stock_usd_price(ticker)
        if stock_usd is None:
            continue

        # Require bid/ask and ccl market for real opportunities
        if bid is None or ask is None or not ccl_mkt:
            ws_watch_hist.append_row([
                today, ticker, ratio, stock_usd,
                bid if bid is not None else "", ask if ask is not None else "",
                "", "", ccl_mkt if ccl_mkt else "",
                "", "",
                "", "",
                "", "",
                "", "",
                source
            ])
            continue

        # BUY uses ASK
        ccl_buy = ccl_implicit(ask, stock_usd, ratio)
        # SELL uses BID
        ccl_sell = ccl_implicit(bid, stock_usd, ratio)
        if not ccl_buy or not ccl_sell:
            continue

        diff_buy_pct = (ccl_buy - ccl_mkt) / ccl_mkt * 100
        diff_sell_pct = (ccl_sell - ccl_mkt) / ccl_mkt * 100

        buy_pack = edge_net_usd_per_cedear(ask, ccl_mkt, ccl_buy, BROKER_FEE_PCT)
        sell_pack = edge_net_usd_per_cedear(bid, ccl_mkt, ccl_sell, BROKER_FEE_PCT)

        edge_buy_gross_abs, fee_buy_usd_rt, edge_buy_net = (buy_pack if buy_pack else (None, None, None))
        edge_sell_gross_abs, fee_sell_usd_rt, edge_sell_net = (sell_pack if sell_pack else (None, None, None))

        # Alert logic
        # BUY opp: ccl_buy below market (diff negative enough) and net positive enough
        if diff_buy_pct <= -min_diff_pct and edge_buy_net is not None and edge_buy_net >= WATCH_MIN_NET_USD_PER_CEDEAR:
            watch_opps.append(
                f"⚡ {ticker} COMPRA (ASK) diff {diff_buy_pct:+.1f}% | bruto {edge_buy_gross_abs:.2f} | fees {fee_buy_usd_rt:.2f} | neto {edge_buy_net:.2f} USD/CEDEAR"
            )

        # SELL opp: ccl_sell above market (diff positive enough) and net positive enough
        if diff_sell_pct >= min_diff_pct and edge_sell_net is not None and edge_sell_net >= WATCH_MIN_NET_USD_PER_CEDEAR:
            watch_opps.append(
                f"⚡ {ticker} VENTA (BID) diff {diff_sell_pct:+.1f}% | bruto {edge_sell_gross_abs:.2f} | fees {fee_sell_usd_rt:.2f} | neto {edge_sell_net:.2f} USD/CEDEAR"
            )

        # History
        ws_watch_hist.append_row([
            today, ticker, ratio, stock_usd, bid, ask,
            ccl_buy, ccl_sell, ccl_mkt,
            diff_buy_pct, diff_sell_pct,
            edge_buy_gross_abs if edge_buy_gross_abs is not None else "",
            edge_sell_gross_abs if edge_sell_gross_abs is not None else "",
            fee_buy_usd_rt if fee_buy_usd_rt is not None else "",
            fee_sell_usd_rt if fee_sell_usd_rt is not None else "",
            edge_buy_net if edge_buy_net is not None else "",
            edge_sell_net if edge_sell_net is not None else "",
            source
        ])

    # ---------- TELEGRAM MESSAGE ----------
    msg = (
        "📊 AI Portfolio Daily - Broker Mode\n\n"
        f"Valor ARS: ${total_ars:,.0f}\n"
        f"Valor USD real (CCL implícito por activo): ${total_usd:,.2f}\n\n"
        "Distribución principal:\n"
    )

    top3 = sorted(dist.items(), key=lambda kv: kv[1][0], reverse=True)[:3]
    for t, (usd_val, gain_usd, ccl_impl_now) in top3:
        msg += f"- {t}: ${usd_val:,.2f} ({gain_usd:+.2f} USD, CCL {ccl_impl_now:.0f})\n"

    msg += "\n🚨 Ganancia / pérdida cartera:\n"
    for t, (_, gain_usd, _) in sorted(dist.items(), key=lambda kv: kv[1][0], reverse=True):
        icon = "📈" if gain_usd >= 0 else "📉"
        msg += f"{icon} {t}: {gain_usd:+.2f} USD\n"

    msg += f"\n👀 Watchlist oportunidades (umbral {WATCH_MIN_DIFF_PCT:.1f}% | fee {BROKER_FEE_PCT:.1f}%/tx):\n"
    if watch_opps:
        msg += "\n".join(watch_opps) + "\n"
    else:
        msg += "(sin oportunidades relevantes hoy)\n"

    msg += "\nPipeline funcionando 🤖"
    send_telegram(msg)

if __name__ == "__main__":
    main()

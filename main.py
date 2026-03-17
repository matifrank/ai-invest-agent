import os
import json
import time
import math
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
WATCHLIST_SHEET = "watchlist"              # ticker | tipo | ratio | ticker_d (opcional)
PRICES_SHEET = "prices_daily"              # date | ticker | price | source

PORTFOLIO_HISTORY_SHEET = "portfolio_history_v2"
WATCHLIST_HISTORY_SHEET = "watchlist_history_v2"

PORTFOLIO_PRICE_MODE = "mark"  # mark | bid | ask | last

# ---- versión flexible para prueba semanal ----
BROKER_FEE_PCT = float(os.environ.get("BROKER_FEE_PCT", "0.5"))  # por transacción
WATCH_MIN_DIFF_PCT = float(os.environ.get("WATCH_MIN_DIFF_PCT", "1.0"))
WATCH_MIN_NET_USD_PER_CEDEAR = float(os.environ.get("WATCH_MIN_NET_USD_PER_CEDEAR", "0.12"))
TARGET_USD = float(os.environ.get("TARGET_USD", "300"))

# filtros mínimos suaves
MIN_MONTO_OPERADO_ARS = int(os.environ.get("MIN_MONTO_OPERADO_ARS", "0"))
MIN_TOP_QTY_ARS = int(os.environ.get("MIN_TOP_QTY_ARS", "1"))
MIN_TOP_QTY_D = int(os.environ.get("MIN_TOP_QTY_D", "1"))

# si querés volver a ventana horaria estricta:
USE_TIME_WINDOW = os.environ.get("USE_TIME_WINDOW", "0") == "1"

IOL_BASE = "https://api.invertironline.com"
IOL_MERCADO = "bcba"

ALLOWED_WINDOWS = [
    (11, 0, 13, 0),
    (16, 0, 17, 0),
]

WATCHLIST_HISTORY_HEADER = [
    "date", "time_arg", "ticker", "ticker_d", "ratio",
    "bid_ars", "ask_ars", "bid_qty_ars", "ask_qty_ars", "monto_ars", "plazo_ars",
    "bid_d", "ask_d", "bid_qty_d", "ask_qty_d", "plazo_d",
    "ccl_mkt",
    "usd_ars_bid", "usd_ars_ask",
    "diff_buy_pct", "diff_sell_pct",
    "edge_buy_gross", "edge_sell_gross",
    "fee_buy_usd_rt", "fee_sell_usd_rt",
    "edge_buy_net", "edge_sell_net",
    "recommended_side", "n_target", "min_book_ars", "min_book_d",
    "source"
]

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
            if values[0] != header:
                ws.update("1:1", [header])
    return ws


def get_all_records(sheet, tab_name: str) -> List[Dict[str, Any]]:
    return sheet.worksheet(tab_name).get_all_records()


def append_row_aligned(ws, header: List[str], row: List[Any]):
    if len(row) < len(header):
        row = row + [""] * (len(header) - len(row))
    elif len(row) > len(header):
        row = row[:len(header)]
    ws.append_row(row, value_input_option="USER_ENTERED")


def append_price_daily(sheet, ticker: str, price: float, source: str):
    ws = sheet.worksheet(PRICES_SHEET)
    ws.append_row([str(date.today()), ticker, price, source])


def update_portfolio_last_price(sheet, ticker: str, last_price: float):
    ws = sheet.worksheet(PORTFOLIO_SHEET)
    cells = ws.findall(ticker)
    for c in cells:
        ws.update_cell(c.row, 5, last_price)


# =========================
# TIME
# =========================
def now_arg():
    return time.time() - 3 * 3600  # epoch shifted only for hour calc


def hhmm_arg() -> str:
    from datetime import datetime
    return datetime.utcfromtimestamp(now_arg()).strftime("%H:%M")


def in_allowed_window() -> bool:
    if not USE_TIME_WINDOW:
        return True

    from datetime import datetime
    dt = datetime.utcfromtimestamp(now_arg())
    h = dt.hour
    m = dt.minute
    current = h * 60 + m

    for sh, sm, eh, em in ALLOWED_WINDOWS:
        start = sh * 60 + sm
        end = eh * 60 + em
        if start <= current <= end:
            return True
    return False


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


def guess_d_symbol(sym: str) -> str:
    return f"{sym}D"


# =========================
# YAHOO FALLBACK / CACHE
# =========================
_stock_cache: Dict[str, Optional[float]] = {}


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
    if ticker in _stock_cache:
        return _stock_cache[ticker]
    val = _yf_last_close(ticker, interval="5m")
    _stock_cache[ticker] = val
    return val


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


def parse_iol_quote_full(q: Dict[str, Any]) -> Dict[str, Any]:
    last = safe_float(q.get("ultimoPrecio"))
    plazo = q.get("plazo")
    monto = safe_float(q.get("montoOperado"))

    bid = None
    ask = None
    bid_qty = 0
    ask_qty = 0

    puntas = q.get("puntas") or []
    if isinstance(puntas, list) and len(puntas) > 0 and isinstance(puntas[0], dict):
        bid = safe_float(puntas[0].get("precioCompra"))
        ask = safe_float(puntas[0].get("precioVenta"))
        bid_qty = int(safe_float(puntas[0].get("cantidadCompra")) or 0)
        ask_qty = int(safe_float(puntas[0].get("cantidadVenta")) or 0)

    return {
        "last": last,
        "bid": bid,
        "ask": ask,
        "bid_qty": bid_qty,
        "ask_qty": ask_qty,
        "plazo": plazo,
        "monto": monto,
    }


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


def fee_roundtrip_usd(usd_base: float, fee_pct_per_tx: float) -> Optional[float]:
    if usd_base is None:
        return None
    return usd_base * ((2 * fee_pct_per_tx) / 100.0)


def pick_portfolio_price(last: Optional[float], bid: Optional[float], ask: Optional[float]) -> Optional[float]:
    mark = (bid + ask) / 2.0 if (bid is not None and ask is not None) else last
    if PORTFOLIO_PRICE_MODE == "bid" and bid is not None:
        return bid
    if PORTFOLIO_PRICE_MODE == "ask" and ask is not None:
        return ask
    if PORTFOLIO_PRICE_MODE == "last" and last is not None:
        return last
    return mark


def required_cedears_for_target_usd(target_usd: float, bid_d: float, ask_d: float, side: str) -> Optional[int]:
    usd_per_ce = bid_d if side == "COMPRA" else ask_d
    if not usd_per_ce or usd_per_ce <= 0:
        return None
    return int(math.ceil(target_usd / usd_per_ce))


def min_qty_thresholds_for_target(n: int) -> Tuple[int, int]:
    if not n or n <= 0:
        return (1, 1)
    return (max(MIN_TOP_QTY_ARS, n), max(MIN_TOP_QTY_D, n))


def is_executable_for_size(n: int, bid_qty_ars: int, ask_qty_ars: int, bid_qty_d: int, ask_qty_d: int, side: str) -> bool:
    if not n or n <= 0:
        return False
    if side == "COMPRA":
        return ask_qty_ars >= n and bid_qty_d >= n
    return bid_qty_ars >= n and ask_qty_d >= n


def opportunity_flag(edge_net: float, diff_pct: float, n_cedears: int, bid_qty_ars: int, ask_qty_ars: int, bid_qty_d: int, ask_qty_d: int) -> str:
    ultra_liq = (
        bid_qty_ars >= 4 * n_cedears and
        ask_qty_ars >= 4 * n_cedears and
        bid_qty_d >= 4 * n_cedears and
        ask_qty_d >= 4 * n_cedears
    )
    if edge_net >= 1.50 and abs(diff_pct) >= 4.0 and ultra_liq:
        return "🔥 ULTRA"
    if edge_net >= 0.50 or abs(diff_pct) >= 2.5:
        return "🟢 STRONG"
    return "🟡 MEDIUM"


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
    if not in_allowed_window():
        print("⏱ Fuera de ventana operativa, no corro watchlist/portfolio.")
        return

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
        header=WATCHLIST_HISTORY_HEADER,
    )

    portfolio = get_all_records(sheet, PORTFOLIO_SHEET)
    watchlist = get_all_records(sheet, WATCHLIST_SHEET)
    ccl_mkt = get_ccl_market()
    today = str(date.today())
    hhmm = hhmm_arg()

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

        last = bid = ask = None
        src = "YAHOO"

        if iol:
            q = iol.get_quote(IOL_MERCADO, ticker)
            if q:
                parsed = parse_iol_quote_full(q)
                last = parsed["last"]
                bid = parsed["bid"]
                ask = parsed["ask"]
                src = "IOL"

        if last is None and bid is None and ask is None:
            last = yahoo_cedear_price_ars(ticker)
            src = "YAHOO"

        price = pick_portfolio_price(last, bid, ask)
        if price is None:
            continue

        mark = (bid + ask) / 2.0 if (bid is not None and ask is not None) else last

        stock_usd = yahoo_stock_price_usd(ticker)
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

    # ---------- WATCHLIST (ARS vs D) ----------
    watch_opps: List[Tuple[float, str]] = []

    for w in watchlist:
        ticker = (w.get("ticker") or "").strip().upper()
        tipo = (w.get("tipo") or "").upper().strip()
        ratio = safe_float(w.get("ratio")) or 1.0
        ticker_d = (w.get("ticker_d") or "").strip().upper()

        if not ticker or tipo != "CEDEAR":
            continue

        sym_d = ticker_d if ticker_d else guess_d_symbol(ticker)

        if not iol or not ccl_mkt:
            continue

        q_ars = iol.get_quote(IOL_MERCADO, ticker)
        q_d = iol.get_quote(IOL_MERCADO, sym_d)

        if not q_ars or not q_d:
            continue

        ars = parse_iol_quote_full(q_ars)
        d = parse_iol_quote_full(q_d)

        bid_ars = ars["bid"]
        ask_ars = ars["ask"]
        bid_qty_ars = ars["bid_qty"]
        ask_qty_ars = ars["ask_qty"]
        plazo_ars = ars["plazo"]
        monto_ars = ars["monto"]

        bid_d = d["bid"]
        ask_d = d["ask"]
        bid_qty_d = d["bid_qty"]
        ask_qty_d = d["ask_qty"]
        plazo_d = d["plazo"]

        if bid_ars is None or ask_ars is None or bid_d is None or ask_d is None:
            continue
        if plazo_ars != plazo_d:
            continue
        if monto_ars is not None and monto_ars < MIN_MONTO_OPERADO_ARS:
            continue

        # USD implícito del CEDEAR usando CCL de mercado
        usd_ars_bid = usd_per_cedear(bid_ars, ccl_mkt)
        usd_ars_ask = usd_per_cedear(ask_ars, ccl_mkt)

        if usd_ars_bid is None or usd_ars_ask is None:
            continue

        # COMPRA FX: comprás ARS al ask, vendés D al bid
        diff_buy_pct = ((bid_d - usd_ars_ask) / usd_ars_ask) * 100 if usd_ars_ask > 0 else None
        edge_buy_gross = bid_d - usd_ars_ask
        fee_buy = fee_roundtrip_usd(usd_ars_ask, BROKER_FEE_PCT) or 0.0
        edge_buy_net = edge_buy_gross - fee_buy

        # VENTA FX: vendés ARS al bid, comprás D al ask
        diff_sell_pct = ((usd_ars_bid - ask_d) / ask_d) * 100 if ask_d > 0 else None
        edge_sell_gross = usd_ars_bid - ask_d
        fee_sell = fee_roundtrip_usd(usd_ars_bid, BROKER_FEE_PCT) or 0.0
        edge_sell_net = edge_sell_gross - fee_sell

        recommended_side = ""
        diff_pct = None
        edge_net = None
        n_target = None
        min_book_ars = None
        min_book_d = None
        arb_side = ""
        arb_edge_net = None

        # COMPRA
        if diff_buy_pct is not None and diff_buy_pct >= WATCH_MIN_DIFF_PCT and edge_buy_net >= WATCH_MIN_NET_USD_PER_CEDEAR:
            n_target = required_cedears_for_target_usd(TARGET_USD, bid_d, ask_d, "COMPRA")
            if n_target:
                min_book_ars, min_book_d = min_qty_thresholds_for_target(n_target)
                if (
                    bid_qty_ars >= MIN_TOP_QTY_ARS and ask_qty_ars >= MIN_TOP_QTY_ARS and
                    bid_qty_d >= MIN_TOP_QTY_D and ask_qty_d >= MIN_TOP_QTY_D and
                    is_executable_for_size(n_target, bid_qty_ars, ask_qty_ars, bid_qty_d, ask_qty_d, "COMPRA")
                ):
                    recommended_side = "COMPRA"
                    diff_pct = diff_buy_pct
                    edge_net = edge_buy_net
                    arb_side = "barato en ARS / caro en D"
                    arb_edge_net = edge_buy_net

        # VENTA
        if not recommended_side and diff_sell_pct is not None and diff_sell_pct >= WATCH_MIN_DIFF_PCT and edge_sell_net >= WATCH_MIN_NET_USD_PER_CEDEAR:
            n_target = required_cedears_for_target_usd(TARGET_USD, bid_d, ask_d, "VENTA")
            if n_target:
                min_book_ars, min_book_d = min_qty_thresholds_for_target(n_target)
                if (
                    bid_qty_ars >= MIN_TOP_QTY_ARS and ask_qty_ars >= MIN_TOP_QTY_ARS and
                    bid_qty_d >= MIN_TOP_QTY_D and ask_qty_d >= MIN_TOP_QTY_D and
                    is_executable_for_size(n_target, bid_qty_ars, ask_qty_ars, bid_qty_d, ask_qty_d, "VENTA")
                ):
                    recommended_side = "VENTA"
                    diff_pct = diff_sell_pct
                    edge_net = edge_sell_net
                    arb_side = "caro en ARS / barato en D"
                    arb_edge_net = edge_sell_net

        if not recommended_side:
            continue

        flag = opportunity_flag(
            edge_net=edge_net,
            diff_pct=diff_pct,
            n_cedears=n_target,
            bid_qty_ars=bid_qty_ars,
            ask_qty_ars=ask_qty_ars,
            bid_qty_d=bid_qty_d,
            ask_qty_d=ask_qty_d,
        )

        # guarda solo oportunidades
        append_row_aligned(ws_watch_hist, WATCHLIST_HISTORY_HEADER, [
            today, hhmm, ticker, sym_d, ratio,
            bid_ars, ask_ars, bid_qty_ars, ask_qty_ars, monto_ars if monto_ars is not None else "", plazo_ars,
            bid_d, ask_d, bid_qty_d, ask_qty_d, plazo_d,
            ccl_mkt,
            usd_ars_bid, usd_ars_ask,
            diff_buy_pct, diff_sell_pct,
            edge_buy_gross, edge_sell_gross,
            fee_buy, fee_sell,
            edge_buy_net, edge_sell_net,
            recommended_side, n_target, min_book_ars, min_book_d,
            "IOL"
        ])

        usd_trade = edge_net * n_target if n_target else 0.0
        side_text = "Comprá ARS → Vendé D" if recommended_side == "COMPRA" else "Vendé ARS → Comprá D"

        watch_opps.append((
            edge_net,
            f"{flag} ⚡ {ticker} {recommended_side}\n"
            f"{side_text}\n"
            f"diff {diff_pct:+.2f}%\n"
            f"edge {edge_net:.2f} USD/CEDEAR\n"
            f"≈ {usd_trade:.2f} USD por {n_target} CEDEAR\n"
            f"book ARS {bid_qty_ars}/{ask_qty_ars} | D {bid_qty_d}/{ask_qty_d}"
        ))

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

    if watch_opps:
        msg += f"\n👀 Watchlist oportunidades ARS vs D (umbral {WATCH_MIN_DIFF_PCT:.1f}% | neto {WATCH_MIN_NET_USD_PER_CEDEAR:.2f} USD):\n\n"
        watch_opps_sorted = [m for _, m in sorted(watch_opps, key=lambda x: x[0], reverse=True)]
        msg += "\n\n".join(watch_opps_sorted)
        msg += "\n\nPipeline funcionando 🤖"
        send_telegram(msg)
    else:
        print("No watchlist opportunities today")


if __name__ == "__main__":
    main()
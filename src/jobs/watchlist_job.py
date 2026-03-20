import os
import json
import time
import math
import requests
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from datetime import date, datetime, timedelta
from typing import Optional, Dict, Any, List, Tuple

# =========================
# CONFIG
# =========================
SPREADSHEET_NAME = "ai-portfolio-agent"
WATCHLIST_SHEET = "watchlist"
WATCHLIST_HISTORY_SHEET = "watchlist_history_v2"

IOL_BASE = "https://api.invertironline.com"
IOL_MERCADO = "bcba"

BROKER_FEE_PCT = float(os.environ.get("BROKER_FEE_PCT", "0.5"))
WATCH_MIN_DIFF_PCT = float(os.environ.get("WATCH_MIN_DIFF_PCT", "1.0"))
WATCH_MIN_NET_USD_PER_CEDEAR = float(os.environ.get("WATCH_MIN_NET_USD_PER_CEDEAR", "0.12"))
TARGET_USD = float(os.environ.get("TARGET_USD", "300"))

MIN_MONTO_OPERADO_ARS = int(os.environ.get("MIN_MONTO_OPERADO_ARS", "0"))
MIN_TOP_QTY_ARS = int(os.environ.get("MIN_TOP_QTY_ARS", "1"))
MIN_TOP_QTY_D = int(os.environ.get("MIN_TOP_QTY_D", "1"))
MIN_EXEC_QTY = int(os.environ.get("MIN_EXEC_QTY", "5"))

USE_TIME_WINDOW = os.environ.get("USE_TIME_WINDOW", "0") == "1"
TOP_N_ALERTS = int(os.environ.get("TOP_N_ALERTS", "3"))

FLAG_STRONG_EDGE_USD = float(os.environ.get("FLAG_STRONG_EDGE_USD", "0.50"))
FLAG_STRONG_DIFF_PCT = float(os.environ.get("FLAG_STRONG_DIFF_PCT", "2.5"))
FLAG_ULTRA_EDGE_USD = float(os.environ.get("FLAG_ULTRA_EDGE_USD", "1.50"))
FLAG_ULTRA_DIFF_PCT = float(os.environ.get("FLAG_ULTRA_DIFF_PCT", "4.0"))

MEP_CCL_DIVERGENCE_ALERT_PCT = float(os.environ.get("MEP_CCL_DIVERGENCE_ALERT_PCT", "1.0"))

ALLOWED_WINDOWS = [
    (11, 0, 13, 0),
    (16, 0, 17, 0),
]

WATCHLIST_HISTORY_HEADER = [
    "date", "time_arg", "ticker", "ticker_d", "ratio",
    "bid_ars", "ask_ars", "bid_qty_ars", "ask_qty_ars", "monto_ars", "plazo_ars",
    "bid_d", "ask_d", "bid_qty_d", "ask_qty_d", "plazo_d",
    "ccl_mkt", "mep_mkt", "mep_ccl_diff_pct",
    "usd_ars_bid", "usd_ars_ask",
    "diff_buy_pct", "diff_sell_pct",
    "edge_buy_gross", "edge_sell_gross",
    "fee_buy_usd_rt", "fee_sell_usd_rt",
    "edge_buy_net", "edge_sell_net",
    "recommended_side",
    "n_target", "n_executable", "min_book_ars", "min_book_d",
    "price_ars", "price_d",
    "usd_trade_exec", "score",
    "flag", "source"
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

def ensure_worksheet(sheet, title: str, rows: int = 2000, cols: int = 50, header: Optional[List[str]] = None):
    try:
        ws = sheet.worksheet(title)
    except gspread.exceptions.WorksheetNotFound:
        ws = sheet.add_worksheet(title=title, rows=rows, cols=cols)

    if header:
        values = ws.get_all_values()
        if not values:
            ws.append_row(header)
        elif values[0] != header:
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

# =========================
# TIME
# =========================
def now_arg() -> datetime:
    return datetime.utcnow() - timedelta(hours=3)

def hhmm_arg() -> str:
    return now_arg().strftime("%H:%M")

def in_allowed_window() -> bool:
    if not USE_TIME_WINDOW:
        return True
    dt = now_arg()
    current = dt.hour * 60 + dt.minute
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

def fee_roundtrip_usd(usd_base: float, fee_pct_per_tx: float) -> Optional[float]:
    if usd_base is None:
        return None
    return usd_base * ((2 * fee_pct_per_tx) / 100.0)

def usd_per_cedear(price_ars: float, ccl_mkt: float) -> Optional[float]:
    if not price_ars or not ccl_mkt or ccl_mkt <= 0:
        return None
    return price_ars / ccl_mkt

def required_cedears_for_target_usd(target_usd: float, bid_d: float, ask_d: float, side: str) -> Optional[int]:
    usd_per_ce = bid_d if side == "COMPRA" else ask_d
    if not usd_per_ce or usd_per_ce <= 0:
        return None
    return int(math.ceil(target_usd / usd_per_ce))

def min_qty_thresholds_for_target(n: int) -> Tuple[int, int]:
    if not n or n <= 0:
        return (1, 1)
    return (max(MIN_TOP_QTY_ARS, n), max(MIN_TOP_QTY_D, n))

def executable_qty(n_target: int, bid_qty_ars: int, ask_qty_ars: int, bid_qty_d: int, ask_qty_d: int, side: str) -> int:
    if not n_target or n_target <= 0:
        return 0
    if side == "COMPRA":
        # comprar ARS al ask, vender D al bid
        return min(n_target, ask_qty_ars, bid_qty_d)
    # comprar D al ask, vender ARS al bid
    return min(n_target, bid_qty_ars, ask_qty_d)

def opportunity_flag(edge_net: float, diff_pct: float, n_executable: int, bid_qty_ars: int, ask_qty_ars: int, bid_qty_d: int, ask_qty_d: int) -> str:
    ultra_liq = (
        bid_qty_ars >= 4 * max(n_executable, 1) and
        ask_qty_ars >= 4 * max(n_executable, 1) and
        bid_qty_d >= 4 * max(n_executable, 1) and
        ask_qty_d >= 4 * max(n_executable, 1)
    )
    if edge_net >= FLAG_ULTRA_EDGE_USD and abs(diff_pct) >= FLAG_ULTRA_DIFF_PCT and ultra_liq:
        return "🔥 ULTRA"
    if edge_net >= FLAG_STRONG_EDGE_USD or abs(diff_pct) >= FLAG_STRONG_DIFF_PCT:
        return "🟢 STRONG"
    return "🟡 MEDIUM"

def opportunity_score(usd_trade_exec: float, n_exec: int, n_target: int) -> float:
    if not n_target or n_target <= 0:
        return 0.0
    fill_ratio = min(1.0, n_exec / n_target)
    return usd_trade_exec * fill_ratio

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
# MARKET REFS
# =========================
def get_dollar_refs() -> Tuple[Optional[float], Optional[float]]:
    try:
        url = "https://dolarapi.com/v1/dolares"
        r = requests.get(url, timeout=10)
        data = r.json()

        ccl = None
        mep = None

        for item in data:
            casa = (item.get("casa") or "").lower()
            venta = safe_float(item.get("venta"))
            if casa == "contadoconliqui":
                ccl = venta
            elif casa == "bolsa":
                mep = venta

        return ccl, mep
    except:
        return None, None

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
    print("🚀 Iniciando watchlist standalone refinado")

    if not in_allowed_window():
        print("⏱ Fuera de ventana operativa.")
        return

    if not os.environ.get("IOL_USERNAME") or not os.environ.get("IOL_PASSWORD"):
        raise RuntimeError("Faltan IOL_USERNAME / IOL_PASSWORD.")

    iol = IOLClient(os.environ["IOL_USERNAME"], os.environ["IOL_PASSWORD"])
    ccl_mkt, mep_mkt = get_dollar_refs()
    if not ccl_mkt:
        print("❌ No CCL market ref")
        return

    mep_ccl_diff_pct = None
    if mep_mkt and mep_mkt > 0:
        mep_ccl_diff_pct = ((ccl_mkt - mep_mkt) / mep_mkt) * 100

    sheet = connect_sheets()
    ws_watch_hist = ensure_worksheet(
        sheet,
        WATCHLIST_HISTORY_SHEET,
        header=WATCHLIST_HISTORY_HEADER,
    )
    watchlist = get_all_records(sheet, WATCHLIST_SHEET)

    today = str(date.today())
    hhmm = hhmm_arg()

    watch_opps: List[Tuple[float, str, list]] = []

    for w in watchlist:
        ticker = (w.get("ticker") or "").strip().upper()
        tipo = (w.get("tipo") or "").upper().strip()
        ratio = safe_float(w.get("ratio")) or 1.0
        ticker_d = (w.get("ticker_d") or "").strip().upper()

        if not ticker or tipo != "CEDEAR":
            continue

        sym_d = ticker_d if ticker_d else guess_d_symbol(ticker)

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

        usd_ars_bid = usd_per_cedear(bid_ars, ccl_mkt)
        usd_ars_ask = usd_per_cedear(ask_ars, ccl_mkt)
        if usd_ars_bid is None or usd_ars_ask is None:
            continue

        # COMPRA FX: comprás ARS al ask, vendés D al bid
        diff_buy_pct = ((bid_d - usd_ars_ask) / usd_ars_ask) * 100 if usd_ars_ask > 0 else None
        edge_buy_gross = bid_d - usd_ars_ask
        fee_buy = fee_roundtrip_usd(usd_ars_ask, BROKER_FEE_PCT) or 0.0
        edge_buy_net = edge_buy_gross - fee_buy

        # VENTA FX: comprás D al ask, vendés ARS al bid
        diff_sell_pct = ((usd_ars_bid - ask_d) / ask_d) * 100 if ask_d > 0 else None
        edge_sell_gross = usd_ars_bid - ask_d
        fee_sell = fee_roundtrip_usd(usd_ars_bid, BROKER_FEE_PCT) or 0.0
        edge_sell_net = edge_sell_gross - fee_sell

        recommended_side = ""
        diff_pct = None
        edge_net = None
        n_target = None
        n_exec = 0
        min_book_ars = None
        min_book_d = None
        price_ars = None
        price_d = None

        # lado ARS -> D
        if diff_buy_pct is not None and diff_buy_pct >= WATCH_MIN_DIFF_PCT and edge_buy_net >= WATCH_MIN_NET_USD_PER_CEDEAR:
            n_target = required_cedears_for_target_usd(TARGET_USD, bid_d, ask_d, "COMPRA")
            if n_target:
                min_book_ars, min_book_d = min_qty_thresholds_for_target(n_target)
                n_exec = executable_qty(n_target, bid_qty_ars, ask_qty_ars, bid_qty_d, ask_qty_d, "COMPRA")
                if (
                    bid_qty_ars >= MIN_TOP_QTY_ARS and ask_qty_ars >= MIN_TOP_QTY_ARS and
                    bid_qty_d >= MIN_TOP_QTY_D and ask_qty_d >= MIN_TOP_QTY_D and
                    n_exec >= MIN_EXEC_QTY
                ):
                    recommended_side = "COMPRA"
                    diff_pct = diff_buy_pct
                    edge_net = edge_buy_net
                    price_ars = ask_ars
                    price_d = bid_d

        # lado D -> ARS
        if not recommended_side and diff_sell_pct is not None and diff_sell_pct >= WATCH_MIN_DIFF_PCT and edge_sell_net >= WATCH_MIN_NET_USD_PER_CEDEAR:
            n_target = required_cedears_for_target_usd(TARGET_USD, bid_d, ask_d, "VENTA")
            if n_target:
                min_book_ars, min_book_d = min_qty_thresholds_for_target(n_target)
                n_exec = executable_qty(n_target, bid_qty_ars, ask_qty_ars, bid_qty_d, ask_qty_d, "VENTA")
                if (
                    bid_qty_ars >= MIN_TOP_QTY_ARS and ask_qty_ars >= MIN_TOP_QTY_ARS and
                    bid_qty_d >= MIN_TOP_QTY_D and ask_qty_d >= MIN_TOP_QTY_D and
                    n_exec >= MIN_EXEC_QTY
                ):
                    recommended_side = "VENTA"
                    diff_pct = diff_sell_pct
                    edge_net = edge_sell_net
                    price_ars = bid_ars
                    price_d = ask_d

        if not recommended_side or not n_target or n_exec < MIN_EXEC_QTY:
            continue

        usd_trade_exec = edge_net * n_exec
        score = opportunity_score(usd_trade_exec, n_exec, n_target)

        flag = opportunity_flag(
            edge_net=edge_net,
            diff_pct=diff_pct,
            n_executable=n_exec,
            bid_qty_ars=bid_qty_ars,
            ask_qty_ars=ask_qty_ars,
            bid_qty_d=bid_qty_d,
            ask_qty_d=ask_qty_d,
        )

        row = [
            today, hhmm, ticker, sym_d, ratio,
            bid_ars, ask_ars, bid_qty_ars, ask_qty_ars, monto_ars if monto_ars is not None else "", plazo_ars,
            bid_d, ask_d, bid_qty_d, ask_qty_d, plazo_d,
            ccl_mkt, mep_mkt if mep_mkt is not None else "", mep_ccl_diff_pct if mep_ccl_diff_pct is not None else "",
            usd_ars_bid, usd_ars_ask,
            diff_buy_pct, diff_sell_pct,
            edge_buy_gross, edge_sell_gross,
            fee_buy, fee_sell,
            edge_buy_net, edge_sell_net,
            recommended_side,
            n_target, n_exec, min_book_ars, min_book_d,
            price_ars, price_d,
            usd_trade_exec, score,
            flag, "IOL"
        ]

        if recommended_side == "COMPRA":
            side_text = "Comprá ARS → Vendé D (USD barato)"
            label_ars = "ASK"
            label_d = "BID"
            order_text = (
                f"Orden sugerida:\n"
                f"- Comprar {ticker} {n_exec} @ {price_ars:.2f} {label_ars}\n"
                f"- Vender {sym_d} {n_exec} @ {price_d:.2f} {label_d}"
            )
        else:
            side_text = "Comprá D → Vendé ARS (USD caro)"
            label_ars = "BID"
            label_d = "ASK"
            order_text = (
                f"Orden sugerida:\n"
                f"- Comprar {sym_d} {n_exec} @ {price_d:.2f} {label_d}\n"
                f"- Vender {ticker} {n_exec} @ {price_ars:.2f} {label_ars}"
            )

        msg_item = (
            f"{flag} ⚡ {ticker} {recommended_side}\n"
            f"{side_text}\n\n"
            f"📍 ARS: {price_ars:.2f} ({label_ars})\n"
            f"📍 USD: {price_d:.2f} ({label_d})\n\n"
            f"Qty objetivo: {n_target} CEDEAR\n"
            f"Qty ejecutable ahora: {n_exec} CEDEAR\n"
            f"diff {diff_pct:+.2f}%\n"
            f"edge {edge_net:.2f} USD/CEDEAR\n"
            f"≈ {usd_trade_exec:.2f} USD total ejecutable\n"
            f"score {score:.2f}\n\n"
            f"{order_text}\n\n"
            f"book ARS {bid_qty_ars}/{ask_qty_ars} | D {bid_qty_d}/{ask_qty_d}"
        )

        watch_opps.append((score, msg_item, row))

    if not watch_opps:
        print("No watchlist opportunities today")
        return

    watch_opps_sorted = sorted(watch_opps, key=lambda x: x[0], reverse=True)[:TOP_N_ALERTS]

    for _, _, row in watch_opps_sorted:
        append_row_aligned(ws_watch_hist, WATCHLIST_HISTORY_HEADER, row)

    header = f"👀 Watchlist oportunidades ARS vs D\nCCL: {ccl_mkt:.0f}"
    if mep_mkt:
        header += f" | MEP: {mep_mkt:.0f}"
    header += f" | {hhmm}"

    if mep_ccl_diff_pct is not None and abs(mep_ccl_diff_pct) >= MEP_CCL_DIVERGENCE_ALERT_PCT:
        header += f"\n⚠️ Divergencia MEP/CCL: {mep_ccl_diff_pct:+.2f}%"

    formatted = []
    for i, (_, msg_item, _) in enumerate(watch_opps_sorted, start=1):
        formatted.append(f"#{i}\n{msg_item}")

    msg = header + "\n\n" + "\n\n".join(formatted) + "\n\nPipeline funcionando 🤖"
    send_telegram(msg)

if __name__ == "__main__":
    main()

import os
import sys
import json
import time
import math
import requests
import gspread
from datetime import datetime, timedelta
from typing import Optional, Dict, Any, List, Tuple
from oauth2client.service_account import ServiceAccountCredentials

# =========================
# CONFIG
# =========================
SPREADSHEET_NAME = "ai-portfolio-agent"
PENDING_TRADES_SHEET = "pending_trades"

IOL_BASE = "https://api.invertironline.com"
IOL_MERCADO = "bcba"

BROKER_FEE_PCT = float(os.environ.get("BROKER_FEE_PCT", "0.5"))
WATCH_MIN_DIFF_PCT = float(os.environ.get("WATCH_MIN_DIFF_PCT", "1.0"))
WATCH_MIN_NET_USD_PER_CEDEAR = float(os.environ.get("WATCH_MIN_NET_USD_PER_CEDEAR", "0.12"))
MIN_EXEC_QTY = int(os.environ.get("MIN_EXEC_QTY", "5"))

PENDING_TRADES_HEADER = [
    "trade_id",
    "created_at_arg",
    "expires_at_arg",
    "ticker",
    "ticker_d",
    "side",
    "price_ars",
    "price_d",
    "qty_target",
    "qty_exec",
    "edge_net",
    "diff_pct",
    "ccl_mkt",
    "status",
    "reason"
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

def ensure_worksheet(sheet, title: str, rows: int = 2000, cols: int = 20, header: Optional[List[str]] = None):
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

def update_pending_trade_status(ws, row_idx: int, status: str, reason: str = ""):
    # columns N:O = status, reason
    ws.update(f"N{row_idx}:O{row_idx}", [[status, reason]])

def find_pending_trade(sheet, trade_id: str):
    rows = get_all_records(sheet, PENDING_TRADES_SHEET)
    for i, r in enumerate(rows, start=2):
        if (r.get("trade_id") or "").strip() == trade_id:
            return i, r
    return None, None

# =========================
# UTILS
# =========================
def now_arg() -> datetime:
    return datetime.utcnow() - timedelta(hours=3)

def safe_float(x) -> Optional[float]:
    try:
        if x is None:
            return None
        if isinstance(x, str) and x.strip() == "":
            return None
        return float(x)
    except:
        return None

def fee_roundtrip_usd(usd_base: float, fee_pct_per_tx: float) -> Optional[float]:
    if usd_base is None:
        return None
    return usd_base * ((2 * fee_pct_per_tx) / 100.0)

def usd_per_cedear(price_ars: float, ccl_mkt: float) -> Optional[float]:
    if not price_ars or not ccl_mkt or ccl_mkt <= 0:
        return None
    return price_ars / ccl_mkt

def executable_qty(n_target: int, bid_qty_ars: int, ask_qty_ars: int, bid_qty_d: int, ask_qty_d: int, side: str) -> int:
    if not n_target or n_target <= 0:
        return 0
    if side == "COMPRA":
        return min(n_target, ask_qty_ars, bid_qty_d)
    return min(n_target, bid_qty_ars, ask_qty_d)

# =========================
# IOL
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
    plazo = q.get("plazo")
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
        "bid": bid,
        "ask": ask,
        "bid_qty": bid_qty,
        "ask_qty": ask_qty,
        "plazo": plazo,
    }

# =========================
# TELEGRAM
# =========================
def send_telegram(msg: str):
    token = os.environ["TELEGRAM_TOKEN"]
    chat_id = os.environ["TELEGRAM_CHAT_ID"]
    url = f"https://api.telegram.org/bot{token}/sendMessage"
    requests.post(url, json={"chat_id": chat_id, "text": msg}, timeout=10)

# =========================
# DRY RUN
# =========================
def dry_run_trade(trade_id: str):
    sheet = connect_sheets()
    ws_pending = ensure_worksheet(sheet, PENDING_TRADES_SHEET, header=PENDING_TRADES_HEADER)

    row_idx, trade = find_pending_trade(sheet, trade_id)
    if not trade:
        send_telegram(f"❌ trade_id no encontrado: {trade_id}")
        return

    status = (trade.get("status") or "").strip().upper()
    if status != "PENDING":
        send_telegram(f"⚠️ trade {trade_id} no está PENDING (status={status})")
        return

    exp_raw = (trade.get("expires_at_arg") or "").strip()
    if exp_raw:
        exp_dt = datetime.strptime(exp_raw, "%Y-%m-%d %H:%M:%S")
        if now_arg() > exp_dt:
            update_pending_trade_status(ws_pending, row_idx, "EXPIRED", "timeout")
            send_telegram(f"⌛ trade vencido: {trade_id}")
            return

    ticker = (trade.get("ticker") or "").strip().upper()
    ticker_d = (trade.get("ticker_d") or "").strip().upper()
    side = (trade.get("side") or "").strip().upper()

    ref_price_ars = safe_float(trade.get("price_ars"))
    ref_price_d = safe_float(trade.get("price_d"))
    qty_target = int(safe_float(trade.get("qty_target")) or 0)
    old_qty_exec = int(safe_float(trade.get("qty_exec")) or 0)
    ref_edge = safe_float(trade.get("edge_net"))
    ref_diff = safe_float(trade.get("diff_pct"))
    ccl_mkt = safe_float(trade.get("ccl_mkt"))

    iol = IOLClient(os.environ["IOL_USERNAME"], os.environ["IOL_PASSWORD"])

    q_ars = iol.get_quote(IOL_MERCADO, ticker)
    q_d = iol.get_quote(IOL_MERCADO, ticker_d)
    if not q_ars or not q_d:
        update_pending_trade_status(ws_pending, row_idx, "REJECTED", "quotes_missing")
        send_telegram(f"❌ Sin quotes actuales: {trade_id}")
        return

    ars = parse_iol_quote_full(q_ars)
    d = parse_iol_quote_full(q_d)

    bid_ars = ars["bid"]
    ask_ars = ars["ask"]
    bid_qty_ars = ars["bid_qty"]
    ask_qty_ars = ars["ask_qty"]

    bid_d = d["bid"]
    ask_d = d["ask"]
    bid_qty_d = d["bid_qty"]
    ask_qty_d = d["ask_qty"]

    if side == "COMPRA":
        # Comprar ARS al ask, vender D al bid
        if ask_ars is None or bid_d is None:
            update_pending_trade_status(ws_pending, row_idx, "REJECTED", "missing_revalidation_prices")
            send_telegram(f"❌ Faltan precios para revalidar: {trade_id}")
            return

        usd_ars_ask = usd_per_cedear(ask_ars, ccl_mkt)
        diff_now = ((bid_d - usd_ars_ask) / usd_ars_ask) * 100 if usd_ars_ask and usd_ars_ask > 0 else None
        edge_gross = bid_d - usd_ars_ask
        fee = fee_roundtrip_usd(usd_ars_ask, BROKER_FEE_PCT) or 0.0
        edge_now = edge_gross - fee
        qty_exec_now = executable_qty(qty_target, bid_qty_ars, ask_qty_ars, bid_qty_d, ask_qty_d, side)

        still_valid = (
            ask_ars <= ref_price_ars and
            bid_d >= ref_price_d and
            qty_exec_now >= MIN_EXEC_QTY and
            diff_now is not None and diff_now >= WATCH_MIN_DIFF_PCT and
            edge_now >= WATCH_MIN_NET_USD_PER_CEDEAR
        )

        action_text = (
            f"- Comprar {ticker} {qty_exec_now} @ {ask_ars:.2f} ASK\n"
            f"- Vender {ticker_d} {qty_exec_now} @ {bid_d:.2f} BID"
        )

    else:
        # Comprar D al ask, vender ARS al bid
        if ask_d is None or bid_ars is None:
            update_pending_trade_status(ws_pending, row_idx, "REJECTED", "missing_revalidation_prices")
            send_telegram(f"❌ Faltan precios para revalidar: {trade_id}")
            return

        usd_ars_bid = usd_per_cedear(bid_ars, ccl_mkt)
        diff_now = ((usd_ars_bid - ask_d) / ask_d) * 100 if ask_d and ask_d > 0 else None
        edge_gross = usd_ars_bid - ask_d
        fee = fee_roundtrip_usd(usd_ars_bid, BROKER_FEE_PCT) or 0.0
        edge_now = edge_gross - fee
        qty_exec_now = executable_qty(qty_target, bid_qty_ars, ask_qty_ars, bid_qty_d, ask_qty_d, side)

        still_valid = (
            bid_ars >= ref_price_ars and
            ask_d <= ref_price_d and
            qty_exec_now >= MIN_EXEC_QTY and
            diff_now is not None and diff_now >= WATCH_MIN_DIFF_PCT and
            edge_now >= WATCH_MIN_NET_USD_PER_CEDEAR
        )

        action_text = (
            f"- Comprar {ticker_d} {qty_exec_now} @ {ask_d:.2f} ASK\n"
            f"- Vender {ticker} {qty_exec_now} @ {bid_ars:.2f} BID"
        )

    usd_trade_exec_now = edge_now * qty_exec_now if qty_exec_now > 0 else 0.0

    if still_valid:
        msg = (
            f"✅ DRY RUN OK: {trade_id}\n\n"
            f"Ticker: {ticker}\n"
            f"Lado: {side}\n"
            f"Qty objetivo original: {qty_target}\n"
            f"Qty ejecutable original: {old_qty_exec}\n"
            f"Qty ejecutable ahora: {qty_exec_now}\n\n"
            f"Diff original: {ref_diff:+.2f}%\n"
            f"Diff ahora: {diff_now:+.2f}%\n"
            f"Edge original: {ref_edge:.2f} USD/CEDEAR\n"
            f"Edge ahora: {edge_now:.2f} USD/CEDEAR\n"
            f"≈ {usd_trade_exec_now:.2f} USD total ejecutable\n\n"
            f"Orden sugerida ahora:\n{action_text}"
        )
        send_telegram(msg)
    else:
        update_pending_trade_status(ws_pending, row_idx, "REJECTED", "dry_run_revalidation_failed")
        msg = (
            f"❌ DRY RUN REJECTED: {trade_id}\n\n"
            f"Ticker: {ticker}\n"
            f"Lado: {side}\n"
            f"Qty ejecutable ahora: {qty_exec_now}\n"
            f"Diff ahora: {diff_now if diff_now is not None else 'N/A'}\n"
            f"Edge ahora: {edge_now:.2f} USD/CEDEAR\n\n"
            f"La oportunidad ya no está lo suficientemente buena."
        )
        send_telegram(msg)

def main():
    if len(sys.argv) < 2:
        raise RuntimeError("Falta trade_id")

    trade_id = sys.argv[1]
    print("DRY RUN trade_id:", trade_id)
    dry_run_trade(trade_id)

if __name__ == "__main__":
    main()
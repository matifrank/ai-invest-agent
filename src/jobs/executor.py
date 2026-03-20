import os
import sys
import json
import time
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
MIN_BUFFER_RATIO = float(os.environ.get("MIN_BUFFER_RATIO", "1.2"))
ALT_INCLUDE_OFFICIAL = os.environ.get("ALT_INCLUDE_OFFICIAL", "0") == "1"
FILTER_BEATS_ALT = os.environ.get("FILTER_BEATS_ALT", "1") == "1"

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
    "alt_best",
    "alt_best_label",
    "fx_impl_trade",
    "vs_alt_pct",
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


def ensure_worksheet(sheet, title: str, rows: int = 2000, cols: int = 30, header: Optional[List[str]] = None):
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
    ws.update(f"R{row_idx}:S{row_idx}", [[status, reason]])


def find_pending_trade(sheet, trade_id: str):
    rows = get_all_records(sheet, PENDING_TRADES_SHEET)
    for i, r in enumerate(rows, start=2):
        if (r.get("trade_id") or "").strip() == trade_id:
            return i, r
    return None, None


# =========================
# TIME / UTILS
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


def pick_mark_or_last(pq: dict) -> Optional[float]:
    if pq.get("bid") is not None and pq.get("ask") is not None:
        return (pq["bid"] + pq["ask"]) / 2.0
    return pq.get("last")


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


def execution_buffer_ratio(side: str, bid_qty_ars: int, ask_qty_ars: int, bid_qty_d: int, ask_qty_d: int, n_exec: int) -> float:
    if n_exec <= 0:
        return 0.0
    if side == "COMPRA":
        relevant_min = min(ask_qty_ars, bid_qty_d)
    else:
        relevant_min = min(bid_qty_ars, ask_qty_d)
    return relevant_min / n_exec


def choose_best_alt(
    mep_api: Optional[float],
    mep_own: Optional[float],
    official: Optional[float],
    include_official: bool,
) -> Tuple[Optional[float], str]:
    candidates: List[Tuple[str, float]] = []

    if mep_api and mep_api > 0:
        candidates.append(("MEP API", mep_api))
    if mep_own and mep_own > 0:
        candidates.append(("MEP propio AL30", mep_own))
    if include_official and official and official > 0:
        candidates.append(("Oficial", official))

    if not candidates:
        return None, ""

    best_label, best_value = min(candidates, key=lambda x: x[1])
    return best_value, best_label


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
    last = safe_float(q.get("ultimoPrecio"))
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
        "last": last,
        "bid": bid,
        "ask": ask,
        "bid_qty": bid_qty,
        "ask_qty": ask_qty,
        "plazo": plazo,
    }


def get_mep_ref(iol: IOLClient, plazo_target: str) -> Optional[float]:
    q_ars = iol.get_quote(IOL_MERCADO, "AL30")
    q_usd = iol.get_quote(IOL_MERCADO, "AL30D")
    if not q_ars or not q_usd:
        return None

    p_ars = parse_iol_quote_full(q_ars)
    p_usd = parse_iol_quote_full(q_usd)

    if p_ars.get("plazo") != plazo_target or p_usd.get("plazo") != plazo_target:
        return None

    al30_ars = pick_mark_or_last(p_ars)
    al30d_usd = pick_mark_or_last(p_usd)

    if not al30_ars or not al30d_usd or al30d_usd <= 0:
        return None

    return al30_ars / al30d_usd


# =========================
# MARKET REFS
# =========================
def get_dollar_refs() -> Tuple[Optional[float], Optional[float], Optional[float]]:
    try:
        url = "https://dolarapi.com/v1/dolares"
        r = requests.get(url, timeout=10)
        data = r.json()

        official = None
        mep = None
        ccl = None

        for item in data:
            casa = (item.get("casa") or "").lower()
            venta = safe_float(item.get("venta"))
            if casa == "oficial":
                official = venta
            elif casa == "bolsa":
                mep = venta
            elif casa == "contadoconliqui":
                ccl = venta

        return official, mep, ccl
    except:
        return None, None, None


# =========================
# TELEGRAM
# =========================
def send_telegram(msg: str):
    token = os.environ["TELEGRAM_TOKEN"]
    chat_id = os.environ["TELEGRAM_CHAT_ID"]
    url = f"https://api.telegram.org/bot{token}/sendMessage"
    requests.post(url, json={"chat_id": chat_id, "text": msg}, timeout=10)


# =========================
# LIVE STUB
# =========================
def live_execute_stub(trade_id: str, action_text: str):
    send_telegram(
        f"🟡 LIVE STUB: {trade_id}\n\n"
        f"Aún no hay envío real de órdenes a IOL.\n"
        f"Esto sería lo que se ejecutaría:\n\n{action_text}"
    )


# =========================
# EXECUTION
# =========================
def process_trade(trade_id: str, mode: str):
    print(f"🚀 Processing trade_id={trade_id} | mode={mode}")

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

    official_now, mep_api_now, _ = get_dollar_refs()
    mep_own_now = get_mep_ref(iol, "T1")
    alt_best_now, alt_best_label_now = choose_best_alt(
        mep_api=mep_api_now,
        mep_own=mep_own_now,
        official=official_now,
        include_official=ALT_INCLUDE_OFFICIAL,
    )

    if side == "COMPRA":
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
        buffer_ratio = execution_buffer_ratio(side, bid_qty_ars, ask_qty_ars, bid_qty_d, ask_qty_d, qty_exec_now)

        gross_ars_now = ask_ars * qty_exec_now
        gross_usd_now = bid_d * qty_exec_now
        fx_impl_trade_now = (gross_ars_now / gross_usd_now) if gross_usd_now > 0 else None

        vs_alt_pct_now = None
        if alt_best_now and fx_impl_trade_now and alt_best_now > 0:
            vs_alt_pct_now = ((alt_best_now - fx_impl_trade_now) / alt_best_now) * 100

        still_valid = (
            qty_exec_now >= MIN_EXEC_QTY and
            buffer_ratio >= MIN_BUFFER_RATIO and
            diff_now is not None and diff_now >= WATCH_MIN_DIFF_PCT and
            edge_now >= WATCH_MIN_NET_USD_PER_CEDEAR and
            (not FILTER_BEATS_ALT or vs_alt_pct_now is None or vs_alt_pct_now > 0)
        )

        action_text = (
            f"- Comprar {ticker} {qty_exec_now} @ {ask_ars:.2f} ASK\n"
            f"  Total compra ARS: {gross_ars_now:,.2f}\n"
            f"- Vender {ticker_d} {qty_exec_now} @ {bid_d:.2f} BID\n"
            f"  Total venta USD: {gross_usd_now:,.2f}"
        )

    else:
        if bid_ars is None or ask_d is None:
            update_pending_trade_status(ws_pending, row_idx, "REJECTED", "missing_revalidation_prices")
            send_telegram(f"❌ Faltan precios para revalidar: {trade_id}")
            return

        usd_ars_bid = usd_per_cedear(bid_ars, ccl_mkt)
        diff_now = ((usd_ars_bid - ask_d) / ask_d) * 100 if ask_d and ask_d > 0 else None
        edge_gross = usd_ars_bid - ask_d
        fee = fee_roundtrip_usd(usd_ars_bid, BROKER_FEE_PCT) or 0.0
        edge_now = edge_gross - fee
        qty_exec_now = executable_qty(qty_target, bid_qty_ars, ask_qty_ars, bid_qty_d, ask_qty_d, side)
        buffer_ratio = execution_buffer_ratio(side, bid_qty_ars, ask_qty_ars, bid_qty_d, ask_qty_d, qty_exec_now)

        gross_ars_now = bid_ars * qty_exec_now
        gross_usd_now = ask_d * qty_exec_now
        fx_impl_trade_now = (gross_ars_now / gross_usd_now) if gross_usd_now > 0 else None

        vs_alt_pct_now = None
        if alt_best_now and fx_impl_trade_now and alt_best_now > 0:
            vs_alt_pct_now = ((alt_best_now - fx_impl_trade_now) / alt_best_now) * 100

        still_valid = (
            qty_exec_now >= MIN_EXEC_QTY and
            buffer_ratio >= MIN_BUFFER_RATIO and
            diff_now is not None and diff_now >= WATCH_MIN_DIFF_PCT and
            edge_now >= WATCH_MIN_NET_USD_PER_CEDEAR and
            (not FILTER_BEATS_ALT or vs_alt_pct_now is None or vs_alt_pct_now > 0)
        )

        action_text = (
            f"- Comprar {ticker_d} {qty_exec_now} @ {ask_d:.2f} ASK\n"
            f"  Total compra USD: {gross_usd_now:,.2f}\n"
            f"- Vender {ticker} {qty_exec_now} @ {bid_ars:.2f} BID\n"
            f"  Total venta ARS: {gross_ars_now:,.2f}"
        )

    usd_trade_exec_now = edge_now * qty_exec_now if qty_exec_now > 0 else 0.0

    alt_block = ""
    if alt_best_now and fx_impl_trade_now and vs_alt_pct_now is not None:
        alt_block = (
            f"FX implícito trade: {fx_impl_trade_now:.2f}\n"
            f"Mejor alternativa: {alt_best_now:.2f} ({alt_best_label_now})\n"
            f"vs alternativa: {vs_alt_pct_now:+.2f}%\n"
        )

    if not still_valid:
        update_pending_trade_status(ws_pending, row_idx, "REJECTED", "revalidation_failed")
        send_telegram(
            f"❌ {'DRY RUN' if mode == 'dry_run' else 'EXEC'} REJECTED: {trade_id}\n\n"
            f"Ticker: {ticker}\n"
            f"Lado: {side}\n"
            f"Qty ejecutable ahora: {qty_exec_now}\n"
            f"buffer liquidez: {buffer_ratio:.2f}x\n"
            f"Diff ahora: {diff_now if diff_now is not None else 'N/A'}\n"
            f"Edge ahora: {edge_now:.2f} USD/CEDEAR\n"
            f"{alt_block}\n"
            f"La oportunidad ya no está lo suficientemente buena."
        )
        return

    if mode == "dry_run":
        send_telegram(
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
            f"buffer liquidez: {buffer_ratio:.2f}x\n"
            f"{alt_block}"
            f"≈ {usd_trade_exec_now:.2f} USD total ejecutable\n\n"
            f"Orden sugerida ahora:\n{action_text}"
        )
        return

    if mode == "live":
        send_telegram(f"🟡 LIVE MODE solicitado para {trade_id}")
        update_pending_trade_status(ws_pending, row_idx, "CONFIRMED", "live_stub_ready")
        live_execute_stub(trade_id, action_text)
        return

    send_telegram(f"⚠️ mode desconocido: {mode}")


def main():
    if len(sys.argv) < 3:
        raise RuntimeError("Uso: python -m src.jobs.executor <trade_id> <mode>")

    trade_id = sys.argv[1]
    mode = sys.argv[2].strip().lower()
    process_trade(trade_id, mode)


if __name__ == "__main__":
    main()
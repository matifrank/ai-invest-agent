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
# Config
# =========================
SPREADSHEET_NAME = "ai-portfolio-agent"

PORTFOLIO_SHEET = "portfolio"
WATCHLIST_SHEET = "watchlist"
PRICES_SHEET = "prices_daily"

PORTFOLIO_HISTORY_SHEET = "portfolio_history"
WATCHLIST_HISTORY_SHEET = "watchlist_history"

WATCH_EDGE_USD_MIN = 0.05   # edge por CEDEAR (USD) mínimo para reportar
WATCH_DIFF_PCT_MIN = 0.5    # diff % mínimo vs CCL mercado para reportar

IOL_BASE = "https://api.invertironline.com"

# =========================
# Google Sheets
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
        elif values and values[0] != header:
            # no pisamos data existente; solo dejamos header si estaba vacío
            if len(values) == 1 and (values[0] == [""] or values[0] == []):
                ws.clear()
                ws.append_row(header)
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
        ws.update_cell(c.row, 5, last_price)  # col 5 = last_price

# =========================
# Helpers
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
# Yahoo fallback
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
    except Exception as e:
        print(f"❌ Error yfinance {symbol}: {e}")
        return None

def yahoo_cedear_price_ars(ticker: str) -> Optional[float]:
    return _yf_last_close(f"{ticker}.BA", interval="5m")

def yahoo_stock_price_usd(ticker: str) -> Optional[float]:
    return _yf_last_close(ticker, interval="5m")

# =========================
# IOL Client (V1 login + V2 quotes)
# =========================
class IOLClient:
    def __init__(self, username: str, password: str):
        self.username = username
        self.password = password
        self.access_token: Optional[str] = None
        self.refresh_token: Optional[str] = None
        self.access_expires_at: float = 0.0

    def _login_password_grant(self) -> None:
        url = f"{IOL_BASE}/token"
        data = {
            "username": self.username,
            "password": self.password,
            "grant_type": "password",
        }
        r = requests.post(url, data=data, timeout=15)
        r.raise_for_status()
        payload = r.json()
        self.access_token = payload.get("access_token")
        self.refresh_token = payload.get("refresh_token")
        expires_in = payload.get("expires_in", 900)  # suele venir en segundos; fallback 15m
        self.access_expires_at = time.time() + float(expires_in) - 20  # margen
        if not self.access_token:
            raise RuntimeError(f"IOL login: no access_token in response: {payload}")

    def _refresh(self) -> None:
        if not self.refresh_token:
            self._login_password_grant()
            return
        url = f"{IOL_BASE}/token"
        data = {
            "refresh_token": self.refresh_token,
            "grant_type": "refresh_token",
        }
        r = requests.post(url, data=data, timeout=15)
        # si falla el refresh, re-login
        if r.status_code >= 400:
            self._login_password_grant()
            return
        payload = r.json()
        self.access_token = payload.get("access_token")
        self.refresh_token = payload.get("refresh_token", self.refresh_token)
        expires_in = payload.get("expires_in", 900)
        self.access_expires_at = time.time() + float(expires_in) - 20

    def _ensure_token(self) -> None:
        if not self.access_token or time.time() >= self.access_expires_at:
            # intenta refresh si hay, sino login
            if self.refresh_token:
                self._refresh()
            else:
                self._login_password_grant()

    def _headers(self) -> Dict[str, str]:
        self._ensure_token()
        return {"Authorization": f"Bearer {self.access_token}"}

    def get_cotizacion(self, mercado: str, simbolo: str) -> Optional[Dict[str, Any]]:
        """
        Endpoint de doc: GET /api/v2/{Mercado}/Titulos/{Simbolo}/Cotizacion
        Ejemplos de mercado posibles: "bcba" (BYMA/BCBA), etc.
        """
        url = f"{IOL_BASE}/api/v2/{mercado}/Titulos/{simbolo}/Cotizacion"
        try:
            r = requests.get(url, headers=self._headers(), timeout=15)
            if r.status_code == 401:
                # token vencido o inválido: refresh y reintentar una vez
                self._refresh()
                r = requests.get(url, headers=self._headers(), timeout=15)
            if r.status_code >= 400:
                print(f"⚠️ IOL cotizacion {mercado}/{simbolo} status {r.status_code}: {r.text[:200]}")
                return None
            return r.json()
        except Exception as e:
            print(f"❌ IOL cotizacion error {mercado}/{simbolo}: {e}")
            return None

def _extract_last_price_from_iol_quote(q: Dict[str, Any]) -> Optional[float]:
    """
    Extractor tolerante: intenta encontrar el 'último' precio.
    Si no encuentra, devuelve None.
    """
    if not q:
        return None

    # Candidatos típicos
    candidates = [
        "ultimoPrecio", "ultimo", "last", "precioUltimo", "precio", "ultimoPrecioOperado",
        "cierre", "precioCierre", "precioActual"
    ]
    for k in candidates:
        if k in q:
            v = safe_float(q.get(k))
            if v is not None:
                return v

    # A veces viene anidado
    for k in ["cotizacion", "data", "titulo", "resultado"]:
        if isinstance(q.get(k), dict):
            v = _extract_last_price_from_iol_quote(q.get(k))
            if v is not None:
                return v

    # Algunos endpoints listan puntas; si hay "puntas" usamos promedio bid/ask si existe
    for k in ["puntas", "punta", "book", "ordenes"]:
        obj = q.get(k)
        if isinstance(obj, dict):
            bid = safe_float(obj.get("precioCompra") or obj.get("bid") or obj.get("compra"))
            ask = safe_float(obj.get("precioVenta") or obj.get("ask") or obj.get("venta"))
            if bid is not None and ask is not None:
                return (bid + ask) / 2.0
        if isinstance(obj, list) and obj:
            # primer item
            it = obj[0]
            if isinstance(it, dict):
                bid = safe_float(it.get("precioCompra") or it.get("bid") or it.get("compra"))
                ask = safe_float(it.get("precioVenta") or it.get("ask") or it.get("venta"))
                if bid is not None and ask is not None:
                    return (bid + ask) / 2.0

    return None

# =========================
# CCL / Finance
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
    except Exception as e:
        print("❌ Error obteniendo CCL:", e)
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

def watch_edge_usd_per_cedear(cedear_ars: float, ccl_market: float, ccl_impl: float) -> Optional[float]:
    if not ccl_market or ccl_market <= 0 or not ccl_impl or ccl_impl <= 0:
        return None
    usd_mkt = cedear_ars / ccl_market
    usd_impl = cedear_ars / ccl_impl
    return usd_impl - usd_mkt

# =========================
# Telegram
# =========================
def send_telegram(msg: str):
    token = os.environ["TELEGRAM_TOKEN"]
    chat_id = os.environ["TELEGRAM_CHAT_ID"]
    url = f"https://api.telegram.org/bot{token}/sendMessage"
    r = requests.post(url, json={"chat_id": chat_id, "text": msg}, timeout=10)
    print("📨 Telegram status:", r.status_code)
    print("📨 Telegram response:", r.text)

# =========================
# Price fetchers with IOL + fallback
# =========================
def get_cedear_price_ars(ticker: str, iol: Optional[IOLClient]) -> Optional[float]:
    # 1) IOL (BCBA)
    if iol:
        q = iol.get_cotizacion("bcba", ticker)
        px = _extract_last_price_from_iol_quote(q) if q else None
        if px is not None:
            return px
        if q:
            print(f"🧩 IOL quote JSON (bcba/{ticker}) no parseado. Keys: {list(q.keys())[:30]}")
    # 2) Yahoo fallback
    return yahoo_cedear_price_ars(ticker)

def get_stock_usd_price(ticker: str) -> Optional[float]:
    # Para el subyacente, por ahora dejamos Yahoo (simple y funciona bien).
    # Si después querés, lo migramos a IOL si tu cuenta devuelve NYSE/NASDAQ por API.
    return yahoo_stock_price_usd(ticker)

# =========================
# Main
# =========================
def main():
    print("🚀 Iniciando pipeline")

    sheet = connect_sheets()

    ws_port_hist = ensure_worksheet(
        sheet,
        PORTFOLIO_HISTORY_SHEET,
        header=["date", "ticker", "qty", "ppc_ars", "last_ars", "ratio", "stock_usd", "ccl_impl", "usd_value", "gain_usd", "source_ars"],
    )
    ws_watch_hist = ensure_worksheet(
        sheet,
        WATCHLIST_HISTORY_SHEET,
        header=["date", "ticker", "ratio", "cedear_ars", "stock_usd", "ccl_impl", "ccl_market", "diff_pct", "edge_usd_per_cedear", "side", "source_ars"],
    )

    portfolio = get_all_records(sheet, PORTFOLIO_SHEET)
    watchlist = get_all_records(sheet, WATCHLIST_SHEET)
    ccl_mkt = get_ccl_market()

    # IOL client (si no hay secrets, sigue con Yahoo)
    iol = None
    iol_user = os.environ.get("IOL_USERNAME")
    iol_pass = os.environ.get("IOL_PASSWORD")
    if iol_user and iol_pass:
        iol = IOLClient(iol_user, iol_pass)

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

        last_ars = get_cedear_price_ars(ticker, iol=iol)
        stock_usd = get_stock_usd_price(ticker)
        if last_ars is None or stock_usd is None:
            print(f"⚠️ Portfolio skip {ticker}: sin precios")
            continue

        source_ars = "IOL" if iol and (iol.get_cotizacion("bcba", ticker) is not None) else "YAHOO"

        # sync sheet + daily prices
        update_portfolio_last_price(sheet, ticker, last_ars)
        append_price_daily(sheet, ticker, last_ars)

        ccl_impl_now = ccl_implicit(last_ars, stock_usd, ratio)
        if not ccl_impl_now:
            print(f"⚠️ Portfolio skip {ticker}: sin CCL implícito")
            continue

        usd_val = usd_value_broker_mode(qty, last_ars, ccl_impl_now)
        gain_usd = gain_usd_broker_mode(qty, ppc, last_ars, ccl_impl_now)

        total_ars += qty * last_ars
        total_usd += usd_val
        dist[ticker] = (usd_val, gain_usd, ccl_impl_now)

        ws_port_hist.append_row([
            str(date.today()), ticker, qty, ppc, last_ars, ratio, stock_usd, ccl_impl_now, usd_val, gain_usd, source_ars
        ])

    # ---------- WATCHLIST (solo oportunidades) ----------
    watch_opps = []
    for w in watchlist:
        ticker = (w.get("ticker") or "").strip()
        tipo = (w.get("tipo") or "").upper().strip()
        ratio = safe_float(w.get("ratio")) or 1.0

        if not ticker or tipo != "CEDEAR":
            continue

        cedear_ars = get_cedear_price_ars(ticker, iol=iol)
        stock_usd = get_stock_usd_price(ticker)
        if cedear_ars is None or stock_usd is None:
            continue

        source_ars = "IOL" if iol and (iol.get_cotizacion("bcba", ticker) is not None) else "YAHOO"

        ccl_impl = ccl_implicit(cedear_ars, stock_usd, ratio)
        if not ccl_impl:
            continue

        diff_pct = ""
        side = ""
        edge = ""
        if ccl_mkt and ccl_mkt > 0:
            diff = (ccl_impl - ccl_mkt) / ccl_mkt * 100
            diff_pct = diff
            side = "venta" if diff > 0 else "compra"
            e = watch_edge_usd_per_cedear(cedear_ars, ccl_mkt, ccl_impl)
            edge = e if e is not None else ""

            # OPORTUNIDAD: edge real + diff suficiente
            if e is not None and abs(e) >= WATCH_EDGE_USD_MIN and abs(diff) >= WATCH_DIFF_PCT_MIN:
                watch_opps.append(
                    f"⚡ {ticker} oportunidad {side} → edge ~{abs(e):.2f} USD/CEDEAR (diff {diff:+.1f}%)"
                )

        ws_watch_hist.append_row([
            str(date.today()), ticker, ratio, cedear_ars, stock_usd, ccl_impl, ccl_mkt if ccl_mkt else "", diff_pct, edge, side, source_ars
        ])

    # ---------- Telegram message ----------
    msg = (
        "📊 AI Portfolio Daily - Broker Mode\n\n"
        f"Valor ARS: ${total_ars:,.0f}\n"
        f"Valor USD real (CCL implícito por activo): ${total_usd:,.2f}\n\n"
        "Distribución principal:\n"
    )

    for t, (usd_val, gain_usd, ccl_impl_now) in sorted(dist.items(), key=lambda kv: kv[1][0], reverse=True)[:3]:
        msg += f"- {t}: ${usd_val:,.2f} ({gain_usd:+.2f} USD, CCL {ccl_impl_now:.0f})\n"

    msg += "\n🚨 Ganancia / pérdida cartera:\n"
    for t, (usd_val, gain_usd, ccl_impl_now) in sorted(dist.items(), key=lambda kv: kv[1][0], reverse=True):
        icon = "📈" if gain_usd >= 0 else "📉"
        msg += f"{icon} {t}: {gain_usd:+.2f} USD\n"

    if watch_opps:
        msg += "\n👀 Watchlist oportunidades:\n" + "\n".join(watch_opps) + "\n"
    else:
        msg += "\n👀 Watchlist oportunidades:\n(sin oportunidades relevantes hoy)\n"

    msg += "\nPipeline funcionando 🤖"

    send_telegram(msg)

if __name__ == "__main__":
    main()

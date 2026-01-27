import os
import json
import requests
import yfinance as yf
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from datetime import date

SPREADSHEET_NAME = "ai-portfolio-agent"
PORTFOLIO_SHEET = "portfolio"
WATCHLIST_SHEET = "watchlist"
PRICES_SHEET = "prices_daily"

# =========================
# Google Sheets
# =========================

def connect_sheets():
    scope = [
        "https://spreadsheets.google.com/feeds",
        "https://www.googleapis.com/auth/drive"
    ]
    creds_json = os.environ["PORTFOLIO_GS_CREDS"]
    creds_dict = json.loads(creds_json)
    creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, scope)
    client = gspread.authorize(creds)
    return client.open(SPREADSHEET_NAME)

def get_portfolio(sheet):
    return sheet.worksheet(PORTFOLIO_SHEET).get_all_records()

def get_watchlist(sheet):
    return sheet.worksheet(WATCHLIST_SHEET).get_all_records()

def save_price(sheet, ticker, price):
    ws = sheet.worksheet(PRICES_SHEET)
    ws.append_row([str(date.today()), ticker, price])

def update_last_price(sheet, ticker, price):
    ws = sheet.worksheet(PORTFOLIO_SHEET)
    cells = ws.findall(ticker)
    for c in cells:
        ws.update_cell(c.row, 5, price)  # Columna last_price

# =========================
# Market Data
# =========================

def get_cedear_price(ticker):
    """Último precio CEDEAR (en ARS)"""
    try:
        symbol = ticker + ".BA"
        data = yf.download(symbol, period="1d", interval="5m", progress=False)
        if data is None or data.empty:
            return None
        return float(data["Close"].dropna().iloc[-1])
    except:
        return None

def get_stock_usd_price(ticker):
    """Último precio acción subyacente en USD"""
    try:
        data = yf.download(ticker, period="1d", interval="5m", progress=False)
        if data is None or data.empty:
            return None
        return float(data["Close"].dropna().iloc[-1])
    except:
        return None

def get_ccl():
    """CCL mercado (USD)"""
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

# =========================
# Finance
# =========================

def safe_float(x):
    try:
        return float(x)
    except:
        return None

def compute_ccl_from_prices(cedear_ars, stock_usd, ratio):
    if not cedear_ars or not stock_usd or not ratio:
        return None
    return (cedear_ars * ratio) / stock_usd

def compute_cedear_usd_value(qty, price_ars, ccl_implicit):
    if not ccl_implicit or ccl_implicit == 0:
        return 0
    return qty * price_ars / ccl_implicit

def compute_gain_loss_usd(qty, price_ars, ppc, ccl_implicit):
    if not ccl_implicit or ccl_implicit == 0:
        return 0
    return qty * (price_ars - ppc) / ccl_implicit

# =========================
# Telegram
# =========================

def send_telegram(msg):
    token = os.environ["TELEGRAM_TOKEN"]
    chat_id = os.environ["TELEGRAM_CHAT_ID"]
    url = f"https://api.telegram.org/bot{token}/sendMessage"
    requests.post(url, json={"chat_id": chat_id, "text": msg})

# =========================
# Main
# =========================

def main():
    print("🚀 Iniciando pipeline")

    sheet = connect_sheets()
    portfolio = get_portfolio(sheet)
    watchlist = get_watchlist(sheet)
    ccl_market = get_ccl() or 0

    total_ars = 0
    total_usd = 0
    dist = {}
    alerts = []
    watch_alerts = []

    # ===== Portfolio real =====
    for p in portfolio:
        ticker = p.get("ticker")
        tipo = p.get("tipo", "").upper()
        qty = safe_float(p.get("cantidad"))
        ppc = safe_float(p.get("ppc"))
        ratio = safe_float(p.get("ratio"))

        if not ticker or not qty:
            continue

        if tipo == "CEDEAR":
            # Último precio CEDEAR y actualización sheet
            price_ars = get_cedear_price(ticker)
            if not price_ars:
                continue
            update_last_price(sheet, ticker, price_ars)
            save_price(sheet, ticker, price_ars)

            stock_usd = get_stock_usd_price(ticker)
            if not stock_usd:
                continue

            ccl_now = compute_ccl_from_prices(price_ars, stock_usd, ratio)

            usd_value = compute_cedear_usd_value(qty, price_ars, ccl_now)
            gain_usd = compute_gain_loss_usd(qty, price_ars, ppc, ccl_now)

            total_ars += qty * price_ars
            total_usd += usd_value
            dist[ticker] = (usd_value, ccl_now, gain_usd)

    # ===== Watchlist oportunidades =====
    for w in watchlist:
        ticker = w.get("ticker")
        tipo = w.get("tipo", "").upper()
        ratio = safe_float(w.get("ratio"))

        if not ticker:
            continue

        if tipo == "CEDEAR":
            last_price_ars = get_cedear_price(ticker)
            stock_usd = get_stock_usd_price(ticker)
            if not last_price_ars or not stock_usd:
                continue

            ccl_impl = compute_ccl_from_prices(last_price_ars, stock_usd, ratio)
            diff_pct = (ccl_impl - ccl_market) / ccl_market * 100 if ccl_market else 0
            action = "compra" if diff_pct > 0 else "venta"
            potential_usd = last_price_ars / ccl_impl
            watch_alerts.append(
                f"⚡ {ticker} arbitraje {action} → USD potencial {potential_usd:,.2f} (diff {diff_pct:+.1f}%)"
            )
    
    # ===== Después de calcular portfolio =====
    ws_portfolio_hist = sheet.worksheet("portfolio_history")
    today_str = str(date.today())

    for k, (usd_value, ccl_now, gain_usd) in dist.items():
        p = next((x for x in portfolio if x["ticker"] == k), None)
        if not p:
            continue
        qty = safe_float(p.get("cantidad"))
        ppc = safe_float(p.get("ppc"))
        last_price = safe_float(p.get("last_price")) or 0
        ratio = safe_float(p.get("ratio")) or 0

        # Guardar fila: fecha, ticker, qty, PPC, last_price, ratio, CCL implícito, USD value, ganancia/pérdida
        ws_portfolio_hist.append_row([
            today_str, k, qty, ppc, last_price, ratio, ccl_now, usd_value, gain_usd
        ])

    # ===== Después de calcular watchlist =====
    ws_watch_hist = sheet.worksheet("watchlist_history")

    for w in watchlist:
        ticker = w.get("ticker")
        tipo = w.get("tipo", "").upper()
        ratio = safe_float(w.get("ratio"))
        if not ticker or tipo != "CEDEAR":
            continue

        last_price_ars = get_cedear_price(ticker)
        stock_usd = get_stock_usd_price(ticker)
        if not last_price_ars or not stock_usd:
            continue

        ccl_impl = compute_ccl_from_prices(last_price_ars, stock_usd, ratio)
        diff_pct = (ccl_impl - ccl_market) / ccl_market * 100 if ccl_market else 0
        action = "compra" if diff_pct > 0 else "venta"
        potential_usd = last_price_ars / ccl_impl if ccl_impl else 0

        ws_watch_hist.append_row([
            today_str, ticker, last_price_ars, stock_usd, ratio, ccl_impl, diff_pct, action, potential_usd
        ])


    # ===== Mensaje Telegram =====
    msg = (
        "📊 AI Portfolio Daily - Broker Mode\n\n"
        f"Valor ARS: ${total_ars:,.0f}\n"
        f"Valor USD real (CCL implícito por activo): ${total_usd:,.2f}\n\n"
        "Distribución principal:\n"
    )

    for k, (usd_value, ccl_now, gain_usd) in sorted(dist.items(), key=lambda x: -x[1][0])[:3]:
        msg += f"- {k}: ${usd_value:,.2f} ({gain_usd:+.2f} USD, CCL {ccl_now:.0f})\n"

    if dist:
        msg += "\n🚨 Ganancia / pérdida cartera:\n"
        for k, (usd_value, ccl_now, gain_usd) in sorted(dist.items(), key=lambda x: -x[1][0]):
            msg += f"📈 {k}: {gain_usd:+.2f} USD\n" if gain_usd >=0 else f"📉 {k}: {gain_usd:+.2f} USD\n"

    if watch_alerts:
        msg += "\n👀 Watchlist oportunidades:\n" + "\n".join(watch_alerts)

    msg += "\n\nPipeline funcionando 🤖"

    send_telegram(msg)

if __name__ == "__main__":
    main()

import os
import json
import requests
import yfinance as yf
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from datetime import date

# =========================
# Configuración
# =========================
SPREADSHEET_NAME = "ai-portfolio-agent"
PORTFOLIO_SHEET = "portfolio"
PRICES_SHEET = "prices_daily"
WATCHLIST_SHEET = "watchlist"
WATCHLIST_HISTORY_SHEET = "watchlist_history"
PORTFOLIO_HISTORY_SHEET = "portfolio_history"

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
    ws = sheet.worksheet(PORTFOLIO_SHEET)
    return ws.get_all_records()


def save_price(sheet, ticker, price):
    ws = sheet.worksheet(PRICES_SHEET)
    ws.append_row([str(date.today()), ticker, price])


def get_or_create_sheet(sheet, name, rows=1000, cols=20):
    try:
        return sheet.worksheet(name)
    except gspread.exceptions.WorksheetNotFound:
        return sheet.add_worksheet(title=name, rows=rows, cols=cols)


# =========================
# Market Data
# =========================
def get_price(ticker):
    try:
        data = yf.download(ticker, period="1d", interval="1d", progress=False)
        if data is None or data.empty or "Close" not in data:
            print(f"⚠️ Ticker inválido o sin datos: {ticker}")
            return None
        price_series = data["Close"].dropna()
        if price_series.empty:
            print(f"⚠️ Sin precio válido para: {ticker}")
            return None
        return float(price_series.iloc[-1].item())  # evita FutureWarning
    except Exception as e:
        print(f"❌ Error obteniendo {ticker}: {e}")
        return None


def get_ccl():
    try:
        url = "https://dolarapi.com/v1/dolares"
        r = requests.get(url, timeout=10)
        data = r.json()
        for item in data:
            if item.get("casa") == "contadoconliqui":
                return float(item["venta"])
        print("⚠️ No se encontró CCL")
        return None
    except Exception as e:
        print("❌ Error obteniendo CCL:", e)
        return None


# =========================
# Portfolio Logic
# =========================
def safe_float(x):
    try:
        if x is None or x == "":
            return None
        return float(x)
    except:
        return None


def compute_cedear_usd_value(qty, price_ars, ccl, ratio):
    if not price_ars or not ccl or not ratio:
        return 0
    return qty * price_ars / ccl / ratio


def compute_stock_usd_value(qty, price_usd):
    if not price_usd:
        return 0
    return qty * price_usd


def compute_cedear_ccl(price_ars, price_usd, ratio):
    if not price_ars or not price_usd or not ratio:
        return 0
    return price_ars / (price_usd * ratio)


# =========================
# Telegram
# =========================
def send_telegram(msg):
    token = os.environ["TELEGRAM_TOKEN"]
    chat_id = os.environ["TELEGRAM_CHAT_ID"]
    url = f"https://api.telegram.org/bot{token}/sendMessage"
    payload = {"chat_id": chat_id, "text": msg}
    r = requests.post(url, data=payload)
    print("📨 Telegram status:", r.status_code)
    print("📨 Telegram response:", r.text)


# =========================
# Main
# =========================
def main():
    print("🚀 Iniciando pipeline")
    sheet = connect_sheets()
    ws_watch_hist = get_or_create_sheet(sheet, WATCHLIST_HISTORY_SHEET)
    ws_portfolio_hist = get_or_create_sheet(sheet, PORTFOLIO_HISTORY_SHEET)

    portfolio = get_portfolio(sheet)
    ccl_market = get_ccl()

    prices = {}
    portfolio_msg = []
    alerts = []
    total_ars = 0
    total_usd = 0

    # Procesar cartera
    for p in portfolio:
        ticker = p.get("ticker")
        tipo = p.get("tipo")
        qty = safe_float(p.get("cantidad"))
        ppc = safe_float(p.get("ppc"))
        ratio = safe_float(p.get("ratio"))

        last_price = get_price(ticker)
        if last_price is None:
            continue

        prices[ticker] = last_price
        save_price(sheet, ticker, last_price)

        if tipo == "CEDEAR":
            usd_value = compute_cedear_usd_value(qty, last_price, ccl_market, ratio)
        else:
            usd_value = compute_stock_usd_value(qty, last_price)

        total_ars += qty * last_price
        total_usd += usd_value

        gain_usd = usd_value - (qty * ppc / (ratio if tipo == "CEDEAR" else 1))
        portfolio_msg.append(f"- {ticker}: ${qty * last_price:,.2f} ({gain_usd:+.2f} USD, CCL {compute_cedear_ccl(last_price, last_price, ratio) if tipo=='CEDEAR' else 'N/A'})")

    # Procesar watchlist (solo oportunidades)
    ws_watchlist = sheet.worksheet(WATCHLIST_SHEET)
    watchlist_data = ws_watchlist.get_all_records()
    watchlist_msg = []
    for w in watchlist_data:
        ticker = w.get("ticker")
        tipo = w.get("tipo")
        ratio = safe_float(w.get("ratio"))
        price_ars = get_price(ticker)
        if price_ars is None:
            continue
        price_usd = price_ars / ccl_market / ratio if tipo == "CEDEAR" else price_ars
        diff_pct = (price_ars / price_usd / ratio - ccl_market) / ccl_market * 100 if tipo=="CEDEAR" else 0
        if abs(diff_pct) > 0.5:  # solo reportar spreads interesantes
            watchlist_msg.append(f"⚡ {ticker} arbitraje {'compra' if diff_pct>0 else 'venta'} → USD potencial {price_usd:.2f} (diff {diff_pct:+.1f}%)")
            ws_watch_hist.append_row([str(date.today()), ticker, price_ars, price_usd, ratio, ccl_market, diff_pct])

    # Armar mensaje
    msg = (
        f"📊 AI Portfolio Daily - Broker Mode\n\n"
        f"Valor ARS: ${total_ars:,.0f}\n"
        f"Valor USD real (CCL implícito por activo): ${total_usd:,.2f}\n\n"
        f"Distribución principal:\n" +
        "\n".join(portfolio_msg) + "\n\n"
        f"🚨 Ganancia / pérdida cartera:\n" +
        "\n".join([f"📈 {p.get('ticker')}: +{compute_cedear_usd_value(safe_float(p.get('cantidad')), get_price(p.get('ticker')), ccl_market, safe_float(p.get('ratio'))):.2f} USD" for p in portfolio]) + "\n\n"
        f"👀 Watchlist oportunidades:\n" + "\n".join(watchlist_msg) + "\n\n"
        f"Pipeline funcionando 🤖"
    )

    send_telegram(msg)
    print("✅ Pipeline completado")


if __name__ == "__main__":
    main()

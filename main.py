import os
import json
import requests
import yfinance as yf
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from datetime import date

SPREADSHEET_NAME = "ai-portfolio-agent"

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
    return sheet.worksheet("portfolio").get_all_records()

def save_price(sheet, ticker, price):
    ws = sheet.worksheet("prices_daily")
    ws.append_row([str(date.today()), ticker, price])

def update_last_price(sheet, ticker, price):
    ws = sheet.worksheet("portfolio")
    cells = ws.findall(ticker)
    for c in cells:
        ws.update_cell(c.row, 5, price)

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

# =========================
# Finance
# =========================

def safe_float(x):
    try:
        return float(x)
    except:
        return None

def compute_ccl_from_prices(cedear_ars, stock_usd, ratio):
    """CCL implícito por cada CEDEAR"""
    if not stock_usd or not ratio or not cedear_ars:
        return None
    return (cedear_ars * ratio) / stock_usd

def compute_cedear_usd_value(qty, price_ars, ccl_implicit):
    """Valor USD usando CCL implícito"""
    if not ccl_implicit:
        return 0
    return qty * price_ars / ccl_implicit

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

    total_ars = 0
    total_usd = 0
    dist = {}
    alerts = []

    for p in portfolio:
        ticker = p.get("ticker")
        tipo = p.get("tipo", "").upper()
        qty = safe_float(p.get("cantidad"))
        ppc = safe_float(p.get("ppc"))
        ratio = safe_float(p.get("ratio"))

        if not ticker or not qty:
            continue

        if tipo == "CEDEAR":

            # 1️⃣ Último precio CEDEAR en ARS
            price_ars = get_cedear_price(ticker)
            if not price_ars:
                continue
            update_last_price(sheet, ticker, price_ars)
            save_price(sheet, ticker, price_ars)

            # 2️⃣ Último precio subyacente en USD
            stock_usd = get_stock_usd_price(ticker)
            if not stock_usd:
                continue

            # 3️⃣ CCL implícito de compra y actual
            ccl_buy = compute_ccl_from_prices(ppc, stock_usd, ratio)
            ccl_now = compute_ccl_from_prices(price_ars, stock_usd, ratio)

            # 4️⃣ Valor USD real usando CCL implícito
            usd_value = compute_cedear_usd_value(qty, price_ars, ccl_now)
            total_usd += usd_value
            total_ars += qty * price_ars
            dist[ticker] = usd_value

            # 5️⃣ Alertas por desviación CCL
            if ccl_buy and ccl_now:
                diff = (ccl_now - ccl_buy) / ccl_buy * 100
                if abs(diff) > 6:
                    alerts.append(
                        f"💱 {ticker} CCL propio {diff:+.1f}% "
                        f"(compra {ccl_buy:.0f} → actual {ccl_now:.0f})"
                    )

    msg = (
        "📊 AI Portfolio Daily - Broker Mode\n\n"
        f"Valor ARS: ${total_ars:,.0f}\n"
        f"Valor USD real (CCL implícito por activo): ${total_usd:,.2f}\n\n"
        "Distribución principal:\n"
    )

    for k, v in sorted(dist.items(), key=lambda x: -x[1])[:3]:
        msg += f"- {k}: ${v:,.2f}\n"

    if alerts:
        msg += "\n🚨 Alertas:\n" + "\n".join(alerts)

    msg += "\n\nPipeline funcionando 🤖"

    send_telegram(msg)

if __name__ == "__main__":
    main()

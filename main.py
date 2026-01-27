import os
import json
import requests
import yfinance as yf
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from datetime import date

SPREADSHEET_NAME = "ai-portfolio-agent"

TELEGRAM_TOKEN = os.environ.get("TELEGRAM_TOKEN")
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID")

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
    ws = sheet.worksheet("portfolio")
    return ws.get_all_records()


def save_price(sheet, ticker, price):
    ws = sheet.worksheet("prices_daily")
    ws.append_row([str(date.today()), ticker, price])


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
            return None

        return float(price_series.iloc[-1].item())

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

        return None
    except Exception as e:
        print("❌ Error obteniendo CCL:", e)
        return None


# =========================
# Helpers financieros
# =========================

def safe_float(x):
    try:
        if x is None or x == "":
            return None
        return float(x)
    except:
        return None


def compute_portfolio_value_ars(portfolio, prices):
    total = 0.0
    for p in portfolio:
        qty = safe_float(p.get("cantidad"))
        ticker = p.get("ticker")

        if qty is None or ticker not in prices:
            continue

        total += qty * prices[ticker]
    return total


def compute_cedear_usd_value(qty, price_ars, ccl, ratio):
    if ccl <= 0 or ratio <= 0:
        return 0

    shares = qty / ratio
    price_usd = price_ars / ccl
    return shares * price_usd


def compute_stock_usd_value(qty, price_ars, ccl):
    try:
        return (qty * price_ars) / ccl
    except:
        return None


def compute_cedear_ccl(price_ars, price_usd, ratio):
    try:
        return price_ars / (price_usd * ratio)
    except:
        return None


# =========================
# Telegram
# =========================

def send_telegram(msg):
    if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID:
        print("❌ Telegram no configurado")
        return

    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"

    payload = {
        "chat_id": TELEGRAM_CHAT_ID,
        "text": msg
    }

    try:
        r = requests.post(url, json=payload, timeout=10)
        print("📨 Telegram status:", r.status_code)
        print("📨 Telegram response:", r.text)
    except Exception as e:
        print("❌ Error enviando Telegram:", e)


# =========================
# MAIN
# =========================

def main():
    print("🚀 Iniciando pipeline")

    sheet = connect_sheets()
    portfolio = get_portfolio(sheet)

    prices = {}

    for p in portfolio:
        ticker = p.get("ticker")
        if not ticker:
            continue

        price = get_price(ticker)
        if price is None:
            continue

        prices[ticker] = price
        save_price(sheet, ticker, price)

    value_ars = compute_portfolio_value_ars(portfolio, prices)
    ccl = get_ccl()

    asset_reports = []
    alerts = []
    total_usd_real = 0.0

    for p in portfolio:
        ticker = p.get("ticker")
        tipo = p.get("tipo")
        qty = safe_float(p.get("cantidad"))
        ratio = safe_float(p.get("ratio")) or 1

        if ticker not in prices or qty is None or not ccl:
            continue

        price_ars = prices[ticker]

        if tipo == "CEDEAR":
            usd_value = compute_cedear_usd_value(qty, price_ars, ccl, ratio)

            # arbitraje opcional
            price_usd = get_price(ticker)
            if price_usd is not None and price_usd > 0:
                ccl_impl = compute_cedear_ccl(price_ars, price_usd, ratio)
                diff = (ccl_impl - ccl) / ccl * 100

                if abs(diff) > 6:
                    alerts.append(f"💱 {ticker} desvío CCL {diff:+.1f}%")

        else:
            usd_value = compute_stock_usd_value(qty, price_usd)

        if usd_value:
            total_usd_real += usd_value
            asset_reports.append({
                "ticker": ticker,
                "usd_value": usd_value
            })

    # Concentración
    for a in asset_reports:
        weight = a["usd_value"] / total_usd_real * 100
        if weight > 35:
            alerts.append(f"⚠️ Alta concentración: {a['ticker']} {weight:.1f}%")

    # Mensaje Telegram
    msg = "📊 AI Portfolio Daily\n\n"
    msg += f"Valor ARS: ${value_ars:,.0f}\n"
    msg += f"CCL mercado: ${ccl:,.0f}\n"
    msg += f"Valor USD real: ${total_usd_real:,.2f}\n\n"

    msg += "Distribución principal:\n"
    for a in sorted(asset_reports, key=lambda x: x["usd_value"], reverse=True)[:3]:
        msg += f"- {a['ticker']}: ${a['usd_value']:.2f}\n"

    if alerts:
        msg += "\n🚨 Alertas:\n"
        for al in alerts:
            msg += f"{al}\n"
    else:
        msg += "\n✅ Sin alertas relevantes\n"

    msg += "\nPipeline funcionando 🤖"

    send_telegram(msg)


if __name__ == "__main__":
    main()
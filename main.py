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
            print(f"⚠️ Sin precio válido para: {ticker}")
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


def compute_value(portfolio, prices):
    total = 0.0

    for p in portfolio:
        qty = safe_float(p.get("cantidad"))
        ticker = p.get("ticker")

        if qty is None:
            print(f"⚠️ Cantidad inválida para {ticker}")
            continue

        if ticker not in prices:
            print(f"⚠️ Sin precio para {ticker}")
            continue

        total += qty * prices[ticker]

    return total


def compute_value_usd(value_ars, ccl):
    if not ccl or ccl == 0:
        return None
    return value_ars / ccl


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

    r = requests.post(url, json=payload, timeout=10)
    print("📨 Telegram status:", r.status_code)
    print("📨 Telegram response:", r.text)


# ================

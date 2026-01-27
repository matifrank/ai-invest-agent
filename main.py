import os
import json
import requests
import yfinance as yf
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from datetime import date
import requests
from bs4 import BeautifulSoup

SPREADSHEET_NAME = "ai-portfolio-agent"

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


def save_price(sheet, ticker, price):
    ws = sheet.worksheet("prices_daily")
    ws.append_row([str(date.today()), ticker, price])

import requests
from bs4 import BeautifulSoup

def get_ccl():
    try:
        url = "https://dolarapi.com/v1/dolares"
        r = requests.get(url, timeout=10)
        data = r.json()

        for item in data:
            if item.get("casa") == "contadoconliqui":
                return float(item["venta"])

        print("⚠️ No se encontró CCL en la API")
        return None

    except Exception as e:
        print("❌ Error obteniendo CCL:", e)
        return None



def compute_value(portfolio, prices):
    total = 0.0

    for p in portfolio:
        try:
            raw_qty = p.get("cantidad", None)

            if raw_qty is None or raw_qty == "":
                continue

            qty = float(raw_qty)
            ticker = p["ticker"]

            if ticker not in prices:
                continue

            total += qty * prices[ticker]

        except Exception as e:
            print(f"⚠️ Skipping row {p}: {e}")
            continue

    return total

def compute_value_usd(value_ars, ccl):
    if ccl is None or ccl == 0:
        return None
    return value_ars / ccl


def send_telegram(text):
    try:
        token = os.environ["TELEGRAM_TOKEN"]
        chat_id = os.environ["TELEGRAM_CHAT_ID"]

        url = f"https://api.telegram.org/bot{token}/sendMessage"
        payload = {
            "chat_id": chat_id,
            "text": text
        }

        r = requests.post(url, json=payload, timeout=10)

        print("📨 Telegram status:", r.status_code)
        print("📨 Telegram response:", r.text)

    except Exception as e:
        print("❌ Telegram error:", e)


def main():
    sheet = connect_sheets()
    portfolio = get_portfolio(sheet)

    prices = {}
    for p in portfolio:
        ticker = p["ticker"]
        price = get_price(ticker)
        
        if price is None:
            continue
    
        prices[ticker] = price
        save_price(sheet, ticker, price)

    value_ars = compute_value(portfolio, prices)

    ccl = get_ccl()
    value_usd = compute_value_usd(value_ars, ccl)

    msg = (
        "📊 AI Portfolio Daily\n\n"
        f"Valor ARS: ${value_ars:,.0f}\n"
    )

    if ccl:
        msg += f"CCL: ${ccl:,.0f}\n"

    if value_usd:
        msg += f"Valor USD: ${value_usd:,.2f}\n"

    msg += "\nPipeline funcionando ✅"

    send_telegram(msg)


if __name__ == "__main__":
    main()

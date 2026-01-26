import os
import json
import requests
import yfinance as yf
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from datetime import date

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

def compute_value(portfolio, prices):
    total = 0.0

    for p in portfolio:
        try:
            qty_raw = p.get("cantidad", "").strip()
            if qty_raw == "":
                continue

            qty = float(qty_raw)
            ticker = p["ticker"]

            if ticker not in prices:
                continue

            total += qty * prices[ticker]

        except Exception as e:
            print(f"⚠️ Skipping row {p}: {e}")
            continue

    return total

def send_telegram(msg):
    token = os.environ["TELEGRAM_TOKEN"]
    chat_id = os.environ["TELEGRAM_CHAT_ID"]

    url = f"https://api.telegram.org/bot{token}/sendMessage"
    requests.post(url, json={"chat_id": chat_id, "text": msg})

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

    value = compute_value(portfolio, prices)

    msg = (
        "📊 AI Portfolio Daily\n\n"
        f"Valor cartera aprox: ${value:,.2f}\n"
        f"Activos: {len(portfolio)}\n\n"
        "Pipeline funcionando ✅"
    )

    send_telegram(msg)

if __name__ == "__main__":
    main()

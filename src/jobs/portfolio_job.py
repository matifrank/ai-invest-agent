import os
from datetime import date
from typing import Dict, Tuple

from src.common.sheets import connect_sheets, ensure_worksheet, get_all_records
from src.common.telegram import send_telegram
from src.common.iol import IOLClient, parse_iol_quote
from src.common.yahoo import stock_usd_price
from src.common.calc import ccl_implicit, usd_value, gain_usd

SPREADSHEET_NAME = "ai-portfolio-agent"
PORTFOLIO_SHEET = "portfolio"
PRICES_SHEET = "prices_daily"
PORTFOLIO_HISTORY_SHEET = "portfolio_history_v2"

IOL_MERCADO = "bcba"
BROKER_MODE = "mark"  # mark|bid|ask|last

def safe_float(x):
    try:
        if x is None:
            return None
        if isinstance(x, str) and x.strip() == "":
            return None
        return float(x)
    except:
        return None

def pick_price(last, bid, ask):
    mark = (bid + ask) / 2.0 if (bid is not None and ask is not None) else last
    if BROKER_MODE == "bid" and bid is not None:
        return bid
    if BROKER_MODE == "ask" and ask is not None:
        return ask
    if BROKER_MODE == "last" and last is not None:
        return last
    return mark

def main():
    sheet = connect_sheets(SPREADSHEET_NAME)

    ws_hist = ensure_worksheet(
        sheet,
        PORTFOLIO_HISTORY_SHEET,
        header=["date","ticker","qty","ppc_ars","mark_ars","bid_ars","ask_ars","ratio","stock_usd","ccl_impl","usd_value","gain_usd","source"]
    )

    portfolio = get_all_records(sheet, PORTFOLIO_SHEET)
    today = str(date.today())

    iol = None
    if os.environ.get("IOL_USERNAME") and os.environ.get("IOL_PASSWORD"):
        iol = IOLClient(os.environ["IOL_USERNAME"], os.environ["IOL_PASSWORD"])

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

        last = bid = ask = None
        source = "YAHOO"
        if iol:
            q = iol.get_quote(IOL_MERCADO, ticker)
            if q:
                last, bid, ask = parse_iol_quote(q)
                if last is not None or bid is not None or ask is not None:
                    source = "IOL"

        price = pick_price(last, bid, ask)
        if price is None:
            continue

        stock_usd = stock_usd_price(ticker)
        if stock_usd is None:
            continue

        ccl_impl_now = ccl_implicit(price, stock_usd, ratio)
        if not ccl_impl_now:
            continue

        usd_val = usd_value(qty, price, ccl_impl_now)
        gain = gain_usd(qty, ppc, price, ccl_impl_now)

        total_ars += qty * price
        total_usd += usd_val
        dist[ticker] = (usd_val, gain, ccl_impl_now)

        ws_hist.append_row([
            today, ticker, qty, ppc,
            ((bid + ask)/2.0) if (bid is not None and ask is not None) else (last if last is not None else ""),
            bid if bid is not None else "",
            ask if ask is not None else "",
            ratio, stock_usd, ccl_impl_now, usd_val, gain, source
        ])

    msg = (
        "📊 AI Portfolio Daily - Broker Mode\n\n"
        f"Valor ARS: ${total_ars:,.0f}\n"
        f"Valor USD real (CCL implícito por activo): ${total_usd:,.2f}\n\n"
        "Distribución principal:\n"
    )

    for t, (usd_val, gain, ccl_now) in sorted(dist.items(), key=lambda kv: kv[1][0], reverse=True)[:3]:
        msg += f"- {t}: ${usd_val:,.2f} ({gain:+.2f} USD, CCL {ccl_now:.0f})\n"

    msg += "\n🚨 Ganancia / pérdida cartera:\n"
    for t, (_, gain, _) in sorted(dist.items(), key=lambda kv: kv[1][0], reverse=True):
        msg += f"{'📈' if gain >= 0 else '📉'} {t}: {gain:+.2f} USD\n"

    msg += "\nPipeline funcionando 🤖"
    send_telegram(msg)

if __name__ == "__main__":
    main()

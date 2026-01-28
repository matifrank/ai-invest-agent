import os
import requests
from datetime import date, datetime
from typing import List
from src.common.iol import get_last_price
from src.common.sheets import connect_sheets, ensure_worksheet, get_all_records
from src.common.telegram import send_telegram
from src.common.iol import IOLClient, parse_iol_quote
from src.common.yahoo import stock_usd_price, stock_usd_change_5m_pct
from src.common.calc import ccl_implicit, edges_intuitive

SPREADSHEET_NAME = "ai-portfolio-agent"
WATCHLIST_SHEET = "watchlist"
WATCHLIST_HISTORY_SHEET = "watchlist_history_v2"

IOL_MERCADO = "bcba"

BROKER_FEE_PCT = 0.5
WATCH_MIN_DIFF_PCT = 1.5
WATCH_MIN_NET_USD_PER_CEDEAR = 0.05

ADR_MAX_ABS_5M_PCT = 0.25  # tu regla
MIN_BOOK_LEVEL = 1         # hoy usamos puntas[0] (level 1)

def safe_float(x):
    try:
        if x is None:
            return None
        if isinstance(x, str) and x.strip() == "":
            return None
        return float(x)
    except:
        return None

def get_mep_ref(iol: IOLClient) -> Optional[float]:
    # AL30 en pesos vs AL30D en USD (especie D)
    al30_ars = get_last_price(iol, "bcba", "AL30")
    al30d_usd = get_last_price(iol, "bcba", "AL30D")
    if not al30_ars or not al30d_usd or al30d_usd <= 0:
        return None
    return al30_ars / al30d_usd

def main():
    sheet = connect_sheets(SPREADSHEET_NAME)
    ws_hist = ensure_worksheet(
        sheet,
        WATCHLIST_HISTORY_SHEET,
        header=[
            "date","ticker","ratio","stock_usd","bid_ars","ask_ars",
            "ccl_buy","ccl_sell","ccl_mkt","diff_buy_pct","diff_sell_pct",
            "usd_cedear_ask","usd_cedear_bid","usd_stock_per_cedear",
            "edge_buy_gross","edge_sell_gross","fee_buy_usd_rt","fee_sell_usd_rt",
            "edge_buy_net","edge_sell_net",
            "adr_5m_pct","source"
        ],
    )

    watchlist = get_all_records(sheet, WATCHLIST_SHEET)
    
    if not iol:
        raise RuntimeError("Watchlist requiere IOL para calcular MEP ref (AL30/AL30D).")

    mep_ref = get_mep_ref(iol)

    if not mep_ref:
        raise RuntimeError("No se pudo calcular MEP ref (AL30/AL30D).")

    today = str(date.today())
    now_hhmm = datetime.now().strftime("%H:%M")

    iol = None
    if os.environ.get("IOL_USERNAME") and os.environ.get("IOL_PASSWORD"):
        iol = IOLClient(os.environ["IOL_USERNAME"], os.environ["IOL_PASSWORD"])

    opps: List[str] = []

    for w in watchlist:
        ticker = (w.get("ticker") or "").strip()
        tipo = (w.get("tipo") or "").upper().strip()
        ratio = safe_float(w.get("ratio")) or 1.0
        if not ticker or tipo != "CEDEAR":
            continue

        # ARS quote from IOL
        last = bid = ask = None
        src = "YAHOO"
        if iol:
            q = iol.get_quote(IOL_MERCADO, ticker)
            if q:
                last, bid, ask = parse_iol_quote(q)
                if last is not None or bid is not None or ask is not None:
                    src = "IOL"

        # Need bid/ask for actionable watchlist
        if not ccl_mkt or bid is None or ask is None:
            continue

        # ADR / stock USD price + 5m stability
        stock_usd = stock_usd_price(ticker)
        adr_5m = stock_usd_change_5m_pct(ticker)
        if stock_usd is None or adr_5m is None:
            continue

        # Rule: ADR must be flat -> otherwise it’s equity, not FX spread
        if abs(adr_5m) > ADR_MAX_ABS_5M_PCT:
            # Still log history if you want; for now we log but no alerts
            pass

        ccl_buy = ccl_implicit(ask, stock_usd, ratio)
        ccl_sell = ccl_implicit(bid, stock_usd, ratio)
        if not ccl_buy or not ccl_sell:
            continue

        diff_buy = (ccl_buy - ccl_mkt) / ccl_mkt * 100
        diff_sell = (ccl_sell - ccl_mkt) / ccl_mkt * 100

        pack = edges_intuitive(bid, ask, stock_usd, ratio, ccl_mkt, BROKER_FEE_PCT)
        if not pack:
            continue

        # Save history
        ws_hist.append_row([
            today, ticker, ratio, stock_usd, bid, ask,
            ccl_buy, ccl_sell, ccl_mkt, diff_buy, diff_sell,
            pack["usd_cedear_ask"], pack["usd_cedear_bid"], pack["usd_stock_per_cedear"],
            pack["edge_buy_gross"], pack["edge_sell_gross"],
            pack["fee_buy_usd_rt"], pack["fee_sell_usd_rt"],
            pack["edge_buy_net"], pack["edge_sell_net"],
            adr_5m, src
        ])

        # Alerts only if within conditions
        if abs(adr_5m) <= ADR_MAX_ABS_5M_PCT:
            if diff_buy <= -WATCH_MIN_DIFF_PCT and pack["edge_buy_net"] >= WATCH_MIN_NET_USD_PER_CEDEAR:
                opps.append(
                    f"🔔 {ticker} COMPRA | Impl {ccl_buy:.0f} vs CCL {ccl_mkt:.0f} ({diff_buy:+.1f}%) | ADR {adr_5m:+.2f}% | {now_hhmm}\n"
                    f"   neto {pack['edge_buy_net']:.2f} USD/CEDEAR (fees {pack['fee_buy_usd_rt']:.2f})"
                )

            if diff_sell >= WATCH_MIN_DIFF_PCT and pack["edge_sell_net"] >= WATCH_MIN_NET_USD_PER_CEDEAR:
                opps.append(
                    f"🔔 {ticker} VENTA | Impl {ccl_sell:.0f} vs CCL {ccl_mkt:.0f} ({diff_sell:+.1f}%) | ADR {adr_5m:+.2f}% | {now_hhmm}\n"
                    f"   neto {pack['edge_sell_net']:.2f} USD/CEDEAR (fees {pack['fee_sell_usd_rt']:.2f})"
                )

    if opps:
        msg = "👀 Watchlist intradía (spread FX)\n\n" + "\n\n".join(opps)
    else:
        msg = f"👀 Watchlist intradía: sin señales (ADR quieto + neto) | {now_hhmm}"

    send_telegram(msg)

if __name__ == "__main__":
    main()

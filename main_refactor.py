import os
import sys
import math
import time
from datetime import date
from typing import Optional, List, Dict, Any, Tuple

# make src importable
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "src"))

from common import sheets as sheets_mod
from common import iol as iol_mod
from common import yahoo as yahoo_mod
from common import telegram as telegram_mod
from common import calc as calc_mod

# Config (small subset)
SPREADSHEET_NAME = os.environ.get("SPREADSHEET_NAME", "ai-portfolio-agent")
PORTFOLIO_SHEET = "portfolio"
WATCHLIST_SHEET = "watchlist"
PRICES_SHEET = "prices_daily"
PORTFOLIO_HISTORY_SHEET = "portfolio_history_v2"
WATCHLIST_HISTORY_SHEET = "watchlist_history_v2"

BROKER_FEE_PCT = float(os.environ.get("BROKER_FEE_PCT", "0.5"))
WATCH_MIN_DIFF_PCT = float(os.environ.get("WATCH_MIN_DIFF_PCT", "1.0"))
WATCH_MIN_NET_USD_PER_CEDEAR = float(os.environ.get("WATCH_MIN_NET_USD_PER_CEDEAR", "0.12"))
TARGET_USD = float(os.environ.get("TARGET_USD", "300"))

MIN_MONTO_OPERADO_ARS = int(os.environ.get("MIN_MONTO_OPERADO_ARS", "0"))
MIN_TOP_QTY_ARS = int(os.environ.get("MIN_TOP_QTY_ARS", "1"))
MIN_TOP_QTY_D = int(os.environ.get("MIN_TOP_QTY_D", "1"))

USE_TIME_WINDOW = os.environ.get("USE_TIME_WINDOW", "0") == "1"
IOL_MERCADO = "bcba"
ALLOWED_WINDOWS = [(11, 0, 13, 0), (16, 0, 17, 0)]


def now_arg():
    return time.time() - 3 * 3600


def hhmm_arg() -> str:
    from datetime import datetime
    return datetime.utcfromtimestamp(now_arg()).strftime("%H:%M")


def in_allowed_window() -> bool:
    if not USE_TIME_WINDOW:
        return True
    from datetime import datetime
    dt = datetime.utcfromtimestamp(now_arg())
    current = dt.hour * 60 + dt.minute
    for sh, sm, eh, em in ALLOWED_WINDOWS:
        if sh * 60 + sm <= current <= eh * 60 + em:
            return True
    return False


def append_row_aligned(ws, header: List[str], row: List[Any]):
    if len(row) < len(header):
        row = row + [""] * (len(header) - len(row))
    elif len(row) > len(header):
        row = row[:len(header)]
    ws.append_row(row, value_input_option="USER_ENTERED")


def main():
    print("🚀 Starting refactored pipeline")
    if not in_allowed_window():
        print("Outside allowed window, exiting")
        return

    sheet = sheets_mod.connect_sheets(SPREADSHEET_NAME)

    ws_port_hist = sheets_mod.ensure_worksheet(sheet, PORTFOLIO_HISTORY_SHEET, header=[
        "date", "ticker", "qty", "ppc_ars", "mark_ars", "bid_ars", "ask_ars",
        "ratio", "stock_usd", "ccl_impl", "usd_value", "gain_usd", "source"
    ])

    ws_watch_hist = sheets_mod.ensure_worksheet(sheet, WATCHLIST_HISTORY_SHEET, header=[
        "date", "time_arg", "ticker", "ticker_d", "ratio",
        "bid_ars", "ask_ars", "bid_qty_ars", "ask_qty_ars", "monto_ars", "plazo_ars",
        "bid_d", "ask_d", "bid_qty_d", "ask_qty_d", "plazo_d",
        "ccl_mkt", "usd_ars_bid", "usd_ars_ask",
        "diff_buy_pct", "diff_sell_pct",
        "edge_buy_gross", "edge_sell_gross", "fee_buy_usd_rt", "fee_sell_usd_rt",
        "edge_buy_net", "edge_sell_net", "recommended_side", "n_target", "min_book_ars", "min_book_d", "source"
    ])

    portfolio = sheets_mod.get_all_records(sheet, PORTFOLIO_SHEET)
    watchlist = sheets_mod.get_all_records(sheet, WATCHLIST_SHEET)

    ccl_mkt = calc_mod.get_ccl_market() if hasattr(calc_mod, 'get_ccl_market') else None
    today = str(date.today())
    hhmm = hhmm_arg()

    iol = None
    if os.environ.get("IOL_USERNAME") and os.environ.get("IOL_PASSWORD"):
        iol = iol_mod.IOLClient(os.environ["IOL_USERNAME"], os.environ["IOL_PASSWORD"])

    total_ars = 0.0
    total_usd = 0.0
    dist: Dict[str, Tuple[float, float, float]] = {}

    for p in portfolio:
        ticker = (p.get("ticker") or "").strip()
        tipo = (p.get("tipo") or "").upper().strip()
        qty = p.get("cantidad")
        try:
            qty = float(qty)
        except Exception:
            continue
        ppc = p.get("ppc")
        try:
            ppc = float(ppc) if ppc not in (None, "") else None
        except Exception:
            ppc = None
        ratio = p.get("ratio") or 1.0
        try:
            ratio = float(ratio)
        except Exception:
            ratio = 1.0

        if not ticker or not qty or tipo != "CEDEAR":
            continue

        last = bid = ask = None
        src = "YAHOO"

        if iol:
            q = iol.get_quote(IOL_MERCADO, ticker)
            if q:
                parsed = iol_mod.parse_iol_quote(q)
                last = parsed.get("last")
                bid = parsed.get("bid")
                ask = parsed.get("ask")
                src = "IOL"

        if last is None and bid is None and ask is None:
            last = yahoo_mod.stock_usd_price(f"{ticker}.BA")
            src = "YAHOO"

        price = None
        if last is not None:
            price = last
        elif bid is not None and ask is not None:
            price = (bid + ask) / 2.0

        if price is None:
            continue

        stock_usd = yahoo_mod.stock_usd_price(ticker)
        if stock_usd is None:
            continue

        ccl_impl_now = calc_mod.ccl_implicit(price, stock_usd, ratio)
        if not ccl_impl_now:
            continue

        usd_val = (qty * price) / ccl_impl_now
        gain = 0.0
        if ppc is not None and ccl_impl_now and ccl_impl_now > 0:
            gain = qty * (price - ppc) / ccl_impl_now

        total_ars += qty * price
        total_usd += usd_val
        dist[ticker] = (usd_val, gain, ccl_impl_now)

        try:
            ws_prices = sheet.worksheet(PRICES_SHEET)
            ws_prices.append_row([today, ticker, price, src])
        except Exception:
            pass

        try:
            ws = sheet.worksheet(PORTFOLIO_SHEET)
            cells = ws.findall(ticker)
            for c in cells:
                ws.update_cell(c.row, 5, price)
        except Exception:
            pass

        ws_port_hist.append_row([today, ticker, qty, ppc, price, bid or "", ask or "", ratio, stock_usd, ccl_impl_now, usd_val, gain, src])

    # Watchlist logic (simplified)
    watch_opps: List[Tuple[float, str]] = []
    for w in watchlist:
        ticker = (w.get("ticker") or "").strip().upper()
        tipo = (w.get("tipo") or "").upper().strip()
        ratio = w.get("ratio") or 1.0
        try:
            ratio = float(ratio)
        except Exception:
            ratio = 1.0
        ticker_d = (w.get("ticker_d") or "").strip().upper()

        if not ticker or tipo != "CEDEAR":
            continue
        sym_d = ticker_d if ticker_d else f"{ticker}D"
        if not iol or not ccl_mkt:
            continue
        q_ars = iol.get_quote(IOL_MERCADO, ticker)
        q_d = iol.get_quote(IOL_MERCADO, sym_d)
        if not q_ars or not q_d:
            continue
        ars = iol_mod.parse_iol_quote(q_ars)
        d = iol_mod.parse_iol_quote(q_d)
        bid_ars = ars.get("bid")
        ask_ars = ars.get("ask")
        bid_qty_ars = int(ars.get("bid_qty") or 0)
        ask_qty_ars = int(ars.get("ask_qty") or 0)
        monto_ars = ars.get("montoOperado")
        bid_d = d.get("bid")
        ask_d = d.get("ask")
        bid_qty_d = int(d.get("bid_qty") or 0)
        ask_qty_d = int(d.get("ask_qty") or 0)

        if bid_ars is None or ask_ars is None or bid_d is None or ask_d is None:
            continue
        if monto_ars is not None and monto_ars < MIN_MONTO_OPERADO_ARS:
            continue

        usd_ars_bid = usd_ars_ask = None
        try:
            usd_ars_bid = usd_ars_ask = bid_ars / ccl_mkt
        except Exception:
            continue

        diff_buy_pct = ((bid_d - usd_ars_ask) / usd_ars_ask) * 100 if usd_ars_ask and usd_ars_ask > 0 else None
        edge_buy_gross = (bid_d - usd_ars_ask) if usd_ars_ask is not None else None
        fee_buy = (usd_ars_ask * ((2 * BROKER_FEE_PCT) / 100.0)) if usd_ars_ask is not None else 0.0
        edge_buy_net = (edge_buy_gross - fee_buy) if edge_buy_gross is not None else None

        diff_sell_pct = ((usd_ars_bid - ask_d) / ask_d) * 100 if ask_d and ask_d > 0 else None
        edge_sell_gross = (usd_ars_bid - ask_d) if usd_ars_bid is not None else None
        fee_sell = (usd_ars_bid * ((2 * BROKER_FEE_PCT) / 100.0)) if usd_ars_bid is not None else 0.0
        edge_sell_net = (edge_sell_gross - fee_sell) if edge_sell_gross is not None else None

        recommended_side = ""
        diff_pct = None
        edge_net = None
        n_target = None

        if diff_buy_pct is not None and diff_buy_pct >= WATCH_MIN_DIFF_PCT and edge_buy_net and edge_buy_net >= WATCH_MIN_NET_USD_PER_CEDEAR:
            usd_per_ce = bid_d
            if usd_per_ce and usd_per_ce > 0:
                n_target = int(math.ceil(TARGET_USD / usd_per_ce))
                if ask_qty_ars >= n_target and bid_qty_d >= n_target:
                    recommended_side = "COMPRA"
                    diff_pct = diff_buy_pct
                    edge_net = edge_buy_net

        if not recommended_side and diff_sell_pct is not None and diff_sell_pct >= WATCH_MIN_DIFF_PCT and edge_sell_net and edge_sell_net >= WATCH_MIN_NET_USD_PER_CEDEAR:
            usd_per_ce = ask_d
            if usd_per_ce and usd_per_ce > 0:
                n_target = int(math.ceil(TARGET_USD / usd_per_ce))
                if bid_qty_ars >= n_target and ask_qty_d >= n_target:
                    recommended_side = "VENTA"
                    diff_pct = diff_sell_pct
                    edge_net = edge_sell_net

        if not recommended_side:
            continue

        flag = "🟡 MEDIUM"
        if edge_net and edge_net >= 1.5 and diff_pct and abs(diff_pct) >= 4.0:
            flag = "🔥 ULTRA"
        elif edge_net and (edge_net >= 0.5 or (diff_pct and abs(diff_pct) >= 2.5)):
            flag = "🟢 STRONG"

        append_row_aligned(ws_watch_hist, [
            "date", "time_arg", "ticker", "ticker_d", "ratio",
            "bid_ars", "ask_ars", "bid_qty_ars", "ask_qty_ars", "monto_ars", "plazo_ars",
            "bid_d", "ask_d", "bid_qty_d", "ask_qty_d", "plazo_d",
            "ccl_mkt", "usd_ars_bid", "usd_ars_ask",
            "diff_buy_pct", "diff_sell_pct",
            "edge_buy_gross", "edge_sell_gross", "fee_buy_usd_rt", "fee_sell_usd_rt",
            "edge_buy_net", "edge_sell_net", "recommended_side", "n_target", "min_book_ars", "min_book_d", "source"
        ], [
            today, hhmm, ticker, sym_d, ratio,
            bid_ars, ask_ars, bid_qty_ars, ask_qty_ars, monto_ars or "", "",
            bid_d, ask_d, bid_qty_d, ask_qty_d, "",
            ccl_mkt, usd_ars_bid, usd_ars_ask,
            diff_buy_pct, diff_sell_pct,
            edge_buy_gross, edge_sell_gross,
            fee_buy, fee_sell,
            edge_buy_net, edge_sell_net,
            recommended_side, n_target, "", "",
            "IOL"
        ])

    # Notify
    msg = "📊 AI Portfolio Daily - Refactor\n\n"
    msg += f"Valor ARS: ${total_ars:,.0f}\n"
    msg += f"Valor USD real (CCL implícito por activo): ${total_usd:,.2f}\n\n"
    msg += "Distribución principal:\n"
    for t, (usd_val, gain, ccl_impl_now) in sorted(dist.items(), key=lambda kv: kv[1][0], reverse=True)[:3]:
        msg += f"- {t}: ${usd_val:,.2f} ({gain:+.2f} USD, CCL {ccl_impl_now:.0f})\n"

    msg += "\n🚨 Ganancia / pérdida cartera:\n"
    for t, (_, gain, _) in sorted(dist.items(), key=lambda kv: kv[1][0], reverse=True):
        msg += f"{'📈' if gain >= 0 else '📉'} {t}: {gain:+.2f} USD\n"

    if "TELEGRAM_TOKEN" in os.environ and "TELEGRAM_CHAT_ID" in os.environ:
        if any(True for _ in []) :
            pass

    print("Done")


if __name__ == "__main__":
    main()

# src/jobs/watchlist_job.py
import os
from datetime import date, datetime, timedelta
from typing import List, Optional

from src.common.sheets import connect_sheets, ensure_worksheet, get_all_records
from src.common.telegram import send_telegram
from src.common.iol import IOLClient, parse_iol_quote
from src.common.yahoo import stock_usd_price, stock_usd_change_5m_pct
from src.common.calc import ccl_implicit, edges_intuitive

# =========================
# CONFIG
# =========================
SPREADSHEET_NAME = "ai-portfolio-agent"
WATCHLIST_SHEET = "watchlist"
WATCHLIST_HISTORY_SHEET = "watchlist_history_v2"

IOL_MERCADO = "bcba"

BROKER_FEE_PCT = 0.5
WATCH_MIN_DIFF_PCT = 1.5
WATCH_MIN_NET_USD_PER_CEDEAR = 0.05
ADR_MAX_ABS_5M_PCT = 0.25

# Liquidity filters (ajustá a gusto)
MIN_MONTO_OPERADO_ARS = 50_000_000  # 50M ARS
MIN_TOP_QTY = 50                   # mínimo en top of book (cantidad)

# Plazo (para no mezclar CI vs 48)
WATCH_PLAZO_TARGET = os.environ.get("WATCH_PLAZO_TARGET", "T1")

# Windows permitidas (ARG). Igual lo programás en cron, pero esto evita alerts fuera de ventana.
# Formato: (start_h, start_m, end_h, end_m)
ALLOWED_WINDOWS = [
    (11, 0, 13, 0),   # zona dorada
    (16, 0, 17, 0),   # cierre táctico
]


# =========================
# TIME HELPERS
# =========================
def now_arg() -> datetime:
    # GitHub runner suele estar en UTC; Argentina es UTC-3.
    return datetime.utcnow() - timedelta(hours=3)


def in_allowed_window(dt: datetime) -> bool:
    for sh, sm, eh, em in ALLOWED_WINDOWS:
        start = dt.replace(hour=sh, minute=sm, second=0, microsecond=0)
        end = dt.replace(hour=eh, minute=em, second=0, microsecond=0)
        if start <= dt <= end:
            return True
    return False


# =========================
# UTILS
# =========================
def safe_float(x) -> Optional[float]:
    try:
        if x is None:
            return None
        if isinstance(x, str) and x.strip() == "":
            return None
        return float(x)
    except:
        return None


def guess_d_symbol(sym: str) -> str:
    # convención típica en IOL: ticker + "D"
    return f"{sym}D"


def pick_mark_or_last(pq: dict) -> Optional[float]:
    # preferimos mark para “operable”; fallback a last
    if pq.get("bid") is not None and pq.get("ask") is not None:
        return (pq["bid"] + pq["ask"]) / 2.0
    return pq.get("last")


# =========================
# FX REF (MEP) via IOL
# =========================
def get_mep_ref(iol: IOLClient, plazo_target: str) -> Optional[float]:
    """
    MEP ref = AL30(ARS) / AL30D(USD), SOLO si ambos vienen con el mismo plazo (no mezclar CI/48).
    """
    q_ars = iol.get_quote(IOL_MERCADO, "AL30")
    q_usd = iol.get_quote(IOL_MERCADO, "AL30D")
    if not q_ars or not q_usd:
        return None

    p_ars = parse_iol_quote(q_ars)
    p_usd = parse_iol_quote(q_usd)

    if p_ars.get("plazo") != plazo_target or p_usd.get("plazo") != plazo_target:
        return None

    al30_ars = pick_mark_or_last(p_ars)
    al30d_usd = pick_mark_or_last(p_usd)

    if not al30_ars or not al30d_usd or al30d_usd <= 0:
        return None

    return al30_ars / al30d_usd


# =========================
# MAIN
# =========================
def main():
    sheet = connect_sheets(SPREADSHEET_NAME)

    ws_hist = ensure_worksheet(
        sheet,
        WATCHLIST_HISTORY_SHEET,
        header=[
            "date","time_arg","ticker","ratio",
            "stock_usd","adr_5m_pct",
            "bid_ars","ask_ars","bid_qty","ask_qty",
            "plazo_ars","montoOperado_ars",
            "bid_d","ask_d","bid_qty_d","ask_qty_d","plazo_d",
            "mep_ref",
            "ccl_buy","ccl_sell",
            "diff_buy_pct","diff_sell_pct",
            "usd_cedear_ask","usd_cedear_bid","usd_stock_per_cedear",
            "edge_buy_gross","edge_sell_gross",
            "fee_buy_usd_rt","fee_sell_usd_rt",
            "edge_buy_net","edge_sell_net",
            "arb_side","arb_edge_net",
            "source"
        ],
    )

    # IOL requerido para: bid/ask + MEP ref + especie D
    if not os.environ.get("IOL_USERNAME") or not os.environ.get("IOL_PASSWORD"):
        raise RuntimeError("Faltan IOL_USERNAME / IOL_PASSWORD (watchlist requiere IOL).")

    iol = IOLClient(os.environ["IOL_USERNAME"], os.environ["IOL_PASSWORD"])

    dt_arg = now_arg()
    today = str(date.today())
    hhmm = dt_arg.strftime("%H:%M")

    # Window guard
    if not in_allowed_window(dt_arg):
        send_telegram(f"👀 Watchlist intradía: fuera de ventana | {hhmm} (ARG)")
        return

    # MEP ref AL30 con el mismo plazo objetivo
    mep_ref = get_mep_ref(iol, WATCH_PLAZO_TARGET)
    if not mep_ref:
        send_telegram(f"👀 Watchlist intradía: no pude calcular MEP ref AL30 ({WATCH_PLAZO_TARGET}) | {hhmm}")
        return

    watchlist = get_all_records(sheet, WATCHLIST_SHEET)
    opps: List[str] = []

    for w in watchlist:
        ticker = (w.get("ticker") or "").strip()
        tipo = (w.get("tipo") or "").upper().strip()
        ratio = safe_float(w.get("ratio")) or 1.0
        if not ticker or tipo != "CEDEAR":
            continue

        # --- ARS quote (ticker) ---
        q_ars = iol.get_quote(IOL_MERCADO, ticker)
        if not q_ars:
            continue

        p_ars = parse_iol_quote(q_ars)
        bid = p_ars.get("bid")
        ask = p_ars.get("ask")
        bid_qty = p_ars.get("bid_qty")
        ask_qty = p_ars.get("ask_qty")
        plazo_ars = p_ars.get("plazo")
        monto_ars = p_ars.get("montoOperado")

        # Plazo guard (no mezclar CI/48)
        if plazo_ars != WATCH_PLAZO_TARGET:
            continue

        # Necesitamos bid/ask
        if bid is None or ask is None:
            continue

        # Liquidity filters (ARS)
        if monto_ars is not None and monto_ars < MIN_MONTO_OPERADO_ARS:
            continue
        if (bid_qty is not None and bid_qty < MIN_TOP_QTY) or (ask_qty is not None and ask_qty < MIN_TOP_QTY):
            continue

        # --- D quote (tickerD) ---
        sym_d = guess_d_symbol(ticker)
        q_d = iol.get_quote(IOL_MERCADO, sym_d)
        p_d = parse_iol_quote(q_d) if q_d else None

        bid_d = ask_d = bid_qty_d = ask_qty_d = plazo_d = None
        price_d_mark = None
        has_d = False

        if p_d:
            bid_d = p_d.get("bid")
            ask_d = p_d.get("ask")
            bid_qty_d = p_d.get("bid_qty")
            ask_qty_d = p_d.get("ask_qty")
            plazo_d = p_d.get("plazo")

            # D tiene que compartir plazo también
            if plazo_d == WATCH_PLAZO_TARGET and bid_d is not None and ask_d is not None:
                price_d_mark = (bid_d + ask_d) / 2.0
                if price_d_mark and price_d_mark > 0:
                    has_d = True

        # ADR proxy (NYSE)
        stock_usd = stock_usd_price(ticker)
        adr_5m = stock_usd_change_5m_pct(ticker)
        if stock_usd is None or adr_5m is None:
            continue

        # Implícitos vs MEP
        ccl_buy = ccl_implicit(ask, stock_usd, ratio)   # comprar ARS (ASK)
        ccl_sell = ccl_implicit(bid, stock_usd, ratio)  # vender ARS (BID)
        if not ccl_buy or not ccl_sell:
            continue

        diff_buy = (ccl_buy - mep_ref) / mep_ref * 100
        diff_sell = (ccl_sell - mep_ref) / mep_ref * 100

        pack = edges_intuitive(bid, ask, stock_usd, ratio, mep_ref, BROKER_FEE_PCT)
        if not pack:
            continue

        # --- ARS vs D "barato/caro" ---
        arb_side = ""
        arb_edge_net = ""

        if has_d:
            price_ars_mark = (bid + ask) / 2.0  # mark ARS
            usd_per_cedear_ars = price_ars_mark / mep_ref     # USD/CEDEAR via ARS
            usd_per_cedear_d = price_d_mark                  # USD/CEDEAR en D

            edge_gross = usd_per_cedear_d - usd_per_cedear_ars
            base = (usd_per_cedear_ars + usd_per_cedear_d) / 2.0
            fees_rt = base * ((2 * BROKER_FEE_PCT) / 100.0)
            edge_net = edge_gross - fees_rt

            arb_edge_net = edge_net

            if edge_net >= WATCH_MIN_NET_USD_PER_CEDEAR:
                if usd_per_cedear_ars < usd_per_cedear_d:
                    arb_side = "barato en ARS / caro en D"
                elif usd_per_cedear_ars > usd_per_cedear_d:
                    arb_side = "caro en ARS / barato en D"

        # Save history (siempre)
        ws_hist.append_row([
            today, hhmm, ticker, ratio,
            stock_usd, adr_5m,
            bid, ask,
            bid_qty if bid_qty is not None else "",
            ask_qty if ask_qty is not None else "",
            plazo_ars, monto_ars if monto_ars is not None else "",
            bid_d if bid_d is not None else "",
            ask_d if ask_d is not None else "",
            bid_qty_d if bid_qty_d is not None else "",
            ask_qty_d if ask_qty_d is not None else "",
            plazo_d if plazo_d is not None else "",
            mep_ref,
            ccl_buy, ccl_sell,
            diff_buy, diff_sell,
            pack["usd_cedear_ask"], pack["usd_cedear_bid"], pack["usd_stock_per_cedear"],
            pack["edge_buy_gross"], pack["edge_sell_gross"],
            pack["fee_buy_usd_rt"], pack["fee_sell_usd_rt"],
            pack["edge_buy_net"], pack["edge_sell_net"],
            arb_side if arb_side else "",
            arb_edge_net if arb_edge_net != "" else "",
            "IOL"
        ])

        # Alert conditions: ADR quieto + oportunidad neta
        if abs(adr_5m) > ADR_MAX_ABS_5M_PCT:
            continue

        # BUY: implícito bajo vs MEP
        if diff_buy <= -WATCH_MIN_DIFF_PCT and pack["edge_buy_net"] >= WATCH_MIN_NET_USD_PER_CEDEAR:
            extra = ""
            if arb_side == "barato en ARS / caro en D":
                extra = f"\n🧾 {arb_side} | neto {float(arb_edge_net):.2f} USD/CEDEAR"
            opps.append(
                f"🔔 {ticker} COMPRA (spread FX)\n"
                f"Impl: {ccl_buy:.0f} | MEP(AL30): {mep_ref:.0f} | {diff_buy:+.1f}% | plazo {plazo_ars}\n"
                f"ADR 5m: {adr_5m:+.2f}% | {hhmm}\n"
                f"Neto: {pack['edge_buy_net']:.2f} USD/CEDEAR (fees {pack['fee_buy_usd_rt']:.2f})"
                f"{extra}"
            )

        # SELL: implícito alto vs MEP
        if diff_sell >= WATCH_MIN_DIFF_PCT and pack["edge_sell_net"] >= WATCH_MIN_NET_USD_PER_CEDEAR:
            extra = ""
            if arb_side == "caro en ARS / barato en D":
                extra = f"\n🧾 {arb_side} | neto {float(arb_edge_net):.2f} USD/CEDEAR"
            opps.append(
                f"🔔 {ticker} VENTA (spread FX)\n"
                f"Impl: {ccl_sell:.0f} | MEP(AL30): {mep_ref:.0f} | {diff_sell:+.1f}% | plazo {plazo_ars}\n"
                f"ADR 5m: {adr_5m:+.2f}% | {hhmm}\n"
                f"Neto: {pack['edge_sell_net']:.2f} USD/CEDEAR (fees {pack['fee_sell_usd_rt']:.2f})"
                f"{extra}"
            )

    if opps:
        msg = (
            f"👀 Watchlist intradía (MEP ref AL30, {WATCH_PLAZO_TARGET}) — señales\n"
            f"MEP ref: {mep_ref:.0f} | {hhmm}\n\n"
            + "\n\n".join(opps)
        )
    else:
        msg = f"👀 Watchlist intradía: sin señales (MEP AL30 {mep_ref:.0f}, {WATCH_PLAZO_TARGET}) | {hhmm}"

    send_telegram(msg)


if __name__ == "__main__":
    main()

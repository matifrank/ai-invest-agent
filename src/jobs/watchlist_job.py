import os
from datetime import date, datetime, timedelta
from typing import List, Optional, Dict, Any, Tuple

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
WATCHLIST_ALERT_STATE_SHEET = "watchlist_alert_state"  # <--- NUEVO

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

# Ventanas permitidas (ARG)
ALLOWED_WINDOWS = [
    (11, 0, 13, 0),
    (16, 0, 17, 0),
]

# Dedupe
ALERT_COOLDOWN_MIN = int(os.environ.get("ALERT_COOLDOWN_MIN", "20"))  # no repetir por 20 min
ALERT_EDGE_IMPROVE_USD = float(os.environ.get("ALERT_EDGE_IMPROVE_USD", "0.05"))  # o si mejora >= 0.05 USD/CEDEAR

# Si querés forzar que no mande nada fuera de ventana:
WINDOW_GUARD_ENABLED = os.environ.get("WINDOW_GUARD_ENABLED", "1") == "1"


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


def parse_iso(dt_str: str) -> Optional[datetime]:
    try:
        # ISO básico: 2026-01-27T11:05:00
        return datetime.fromisoformat(dt_str)
    except:
        return None


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
    return f"{sym}D"


def pick_mark_or_last(pq: dict) -> Optional[float]:
    if pq.get("bid") is not None and pq.get("ask") is not None:
        return (pq["bid"] + pq["ask"]) / 2.0
    return pq.get("last")


# =========================
# FX REF (MEP) via IOL
# =========================
def get_mep_ref(iol: IOLClient, plazo_target: str) -> Optional[float]:
    """
    MEP ref = AL30(ARS) / AL30D(USD), SOLO si ambos vienen con el mismo plazo.
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
# DEDUPE STATE (Sheets)
# =========================
def load_alert_state(ws_state) -> Dict[str, Dict[str, Any]]:
    """
    Returns dict by ticker: {last_side, last_sent_at(datetime), last_edge_net(float), row_index(int)}
    """
    rows = ws_state.get_all_records()
    state: Dict[str, Dict[str, Any]] = {}
    # gspread get_all_records assumes header exists
    for idx, r in enumerate(rows, start=2):  # data starts at row 2
        t = (r.get("ticker") or "").strip().upper()
        if not t:
            continue
        last_side = (r.get("last_side") or "").strip().upper()
        last_sent_at = parse_iso(str(r.get("last_sent_at") or ""))
        last_edge_net = safe_float(r.get("last_edge_net"))
        state[t] = {
            "last_side": last_side,
            "last_sent_at": last_sent_at,
            "last_edge_net": last_edge_net,
            "row_index": idx,
        }
    return state


def should_send_alert(
    state: Dict[str, Dict[str, Any]],
    ticker: str,
    side: str,
    edge_net: float,
    now_dt: datetime,
) -> bool:
    """
    Dedupe policy:
    - If ticker not in state -> send
    - If side changed -> send
    - Else if cooldown passed -> send
    - Else if edge improved enough -> send
    """
    t = ticker.upper()
    side = side.upper()

    if t not in state:
        return True

    prev = state[t]
    prev_side = (prev.get("last_side") or "").upper()
    prev_time = prev.get("last_sent_at")
    prev_edge = prev.get("last_edge_net")

    # Side changed = new signal
    if prev_side and prev_side != side:
        return True

    # No previous time -> send
    if not prev_time:
        return True

    minutes = (now_dt - prev_time).total_seconds() / 60.0
    if minutes >= ALERT_COOLDOWN_MIN:
        return True

    # Edge improvement
    if prev_edge is None:
        return True

    if (edge_net - prev_edge) >= ALERT_EDGE_IMPROVE_USD:
        return True

    return False


def upsert_alert_state(ws_state, state: Dict[str, Dict[str, Any]], ticker: str, side: str, edge_net: float, now_dt: datetime):
    """
    Update row if exists, else append.
    """
    t = ticker.upper()
    side = side.upper()
    iso = now_dt.replace(microsecond=0).isoformat()

    if t in state:
        row = state[t]["row_index"]
        # columns: ticker, last_side, last_sent_at, last_edge_net
        ws_state.update(f"A{row}:D{row}", [[t, side, iso, edge_net]])
    else:
        ws_state.append_row([t, side, iso, edge_net])


# =========================
# MAIN
# =========================
def main():
    sheet = connect_sheets(SPREADSHEET_NAME)

    # History sheet (one, no new tabs)
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
            "edge_buy_net","edge_sell_net",
            "arb_side","arb_edge_net",
            "source"
        ],
    )

    # Alert state sheet
    ws_state = ensure_worksheet(
        sheet,
        WATCHLIST_ALERT_STATE_SHEET,
        header=["ticker", "last_side", "last_sent_at", "last_edge_net"],
        rows=500,
        cols=10,
    )
    state = load_alert_state(ws_state)

    if not os.environ.get("IOL_USERNAME") or not os.environ.get("IOL_PASSWORD"):
        raise RuntimeError("Faltan IOL_USERNAME / IOL_PASSWORD (watchlist requiere IOL).")

    iol = IOLClient(os.environ["IOL_USERNAME"], os.environ["IOL_PASSWORD"])

    dt_arg = now_arg()
    today = str(date.today())
    hhmm = dt_arg.strftime("%H:%M")

    if WINDOW_GUARD_ENABLED and (not in_allowed_window(dt_arg)):
        # Producción: no spam fuera de ventana -> NO mandamos mensaje
        return

    mep_ref = get_mep_ref(iol, WATCH_PLAZO_TARGET)
    if not mep_ref:
        # Sin MEP ref => silencio (para no spamear)
        return

    watchlist = get_all_records(sheet, WATCHLIST_SHEET)
    alerts_to_send: List[str] = []
    # Guardaremos updates para no escribir en Sheets si no enviamos nada
    pending_state_updates: List[Tuple[str, str, float]] = []

    for w in watchlist:
        ticker = (w.get("ticker") or "").strip().upper()
        tipo = (w.get("tipo") or "").upper().strip()
        ratio = safe_float(w.get("ratio")) or 1.0
        if not ticker or tipo != "CEDEAR":
            continue

        # --- ARS quote ---
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

        if plazo_ars != WATCH_PLAZO_TARGET:
            continue
        if bid is None or ask is None:
            continue

        # Liquidity ARS
        if monto_ars is not None and monto_ars < MIN_MONTO_OPERADO_ARS:
            continue
        if (bid_qty is not None and bid_qty < MIN_TOP_QTY) or (ask_qty is not None and ask_qty < MIN_TOP_QTY):
            continue

        # --- D quote (tickerD) ---
        ticker_d = (w.get("ticker_d") or "").strip().upper()
        sym_d = ticker_d if ticker_d else guess_d_symbol(ticker)

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

            if plazo_d == WATCH_PLAZO_TARGET and bid_d is not None and ask_d is not None:
                price_d_mark = (bid_d + ask_d) / 2.0
                if price_d_mark and price_d_mark > 0:
                    has_d = True

        # ADR proxy (NYSE)
        stock_usd = stock_usd_price(ticker)
        adr_5m = stock_usd_change_5m_pct(ticker)
        if stock_usd is None or adr_5m is None:
            continue

        # Spread FX “puro”
        if abs(adr_5m) > ADR_MAX_ABS_5M_PCT:
            continue

        # Implícitos vs MEP
        ccl_buy = ccl_implicit(ask, stock_usd, ratio)
        ccl_sell = ccl_implicit(bid, stock_usd, ratio)
        if not ccl_buy or not ccl_sell:
            continue

        diff_buy = (ccl_buy - mep_ref) / mep_ref * 100
        diff_sell = (ccl_sell - mep_ref) / mep_ref * 100

        pack = edges_intuitive(bid, ask, stock_usd, ratio, mep_ref, BROKER_FEE_PCT)
        if not pack:
            continue

        # ARS vs D “barato/caro”
        arb_side = ""
        arb_edge_net = None
        if has_d:
            price_ars_mark = (bid + ask) / 2.0
            usd_per_cedear_ars = price_ars_mark / mep_ref
            usd_per_cedear_d = price_d_mark
            edge_gross = usd_per_cedear_d - usd_per_cedear_ars
            base = (usd_per_cedear_ars + usd_per_cedear_d) / 2.0
            fees_rt = base * ((2 * BROKER_FEE_PCT) / 100.0)
            arb_edge_net = edge_gross - fees_rt
            if arb_edge_net >= WATCH_MIN_NET_USD_PER_CEDEAR:
                if usd_per_cedear_ars < usd_per_cedear_d:
                    arb_side = "barato en ARS / caro en D"
                elif usd_per_cedear_ars > usd_per_cedear_d:
                    arb_side = "caro en ARS / barato en D"

        # Save history (auditoría)
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
            pack["edge_buy_net"], pack["edge_sell_net"],
            arb_side,
            arb_edge_net if arb_edge_net is not None else "",
            "IOL"
        ])

        # Signal selection:
        # COMPRA => CEDEAR “barato” en ARS vs MEP (diff_buy negativo) y edge neto ok
        if diff_buy <= -WATCH_MIN_DIFF_PCT and pack["edge_buy_net"] >= WATCH_MIN_NET_USD_PER_CEDEAR:
            side = "COMPRA"
            edge_net = float(pack["edge_buy_net"])
            if should_send_alert(state, ticker, side, edge_net, dt_arg):
                extra = f"\n🧾 {arb_side} | neto {arb_edge_net:.2f} USD/CEDEAR" if (arb_side == "barato en ARS / caro en D" and arb_edge_net is not None) else ""
                alerts_to_send.append(
                    f"🔔 {ticker} {side} (spread FX)\n"
                    f"Impl: {ccl_buy:.0f} | MEP(AL30): {mep_ref:.0f} | {diff_buy:+.1f}% | {WATCH_PLAZO_TARGET}\n"
                    f"ADR 5m: {adr_5m:+.2f}% | {hhmm}\n"
                    f"Neto: {edge_net:.2f} USD/CEDEAR"
                    f"{extra}"
                )
                pending_state_updates.append((ticker, side, edge_net))

        # VENTA => CEDEAR “caro” en ARS vs MEP (diff_sell positivo) y edge neto ok
        if diff_sell >= WATCH_MIN_DIFF_PCT and pack["edge_sell_net"] >= WATCH_MIN_NET_USD_PER_CEDEAR:
            side = "VENTA"
            edge_net = float(pack["edge_sell_net"])
            if should_send_alert(state, ticker, side, edge_net, dt_arg):
                extra = f"\n🧾 {arb_side} | neto {arb_edge_net:.2f} USD/CEDEAR" if (arb_side == "caro en ARS / barato en D" and arb_edge_net is not None) else ""
                alerts_to_send.append(
                    f"🔔 {ticker} {side} (spread FX)\n"
                    f"Impl: {ccl_sell:.0f} | MEP(AL30): {mep_ref:.0f} | {diff_sell:+.1f}% | {WATCH_PLAZO_TARGET}\n"
                    f"ADR 5m: {adr_5m:+.2f}% | {hhmm}\n"
                    f"Neto: {edge_net:.2f} USD/CEDEAR"
                    f"{extra}"
                )
                pending_state_updates.append((ticker, side, edge_net))

    # Only send if there are real opportunities
    if alerts_to_send:
        msg = (
            f"👀 Watchlist intradía — oportunidades\n"
            f"MEP ref(AL30): {mep_ref:.0f} | {hhmm}\n\n"
            + "\n\n".join(alerts_to_send)
        )
        send_telegram(msg)

        # Update state only if we actually sent alerts
        for t, side, edge in pending_state_updates:
            upsert_alert_state(ws_state, state, t, side, edge, dt_arg)

if __name__ == "__main__":
    main()

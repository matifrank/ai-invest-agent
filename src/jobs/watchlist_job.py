import os
import math
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
WATCHLIST_ALERT_STATE_SHEET = "watchlist_alert_state"  # dedupe state

IOL_MERCADO = "bcba"

BROKER_FEE_PCT = 0.5

# --- Opportunity thresholds (more strict, non-marginal) ---
WATCH_MIN_DIFF_PCT = float(os.environ.get("WATCH_MIN_DIFF_PCT", "2.0"))  # was 1.5
WATCH_MIN_NET_USD_PER_CEDEAR = float(os.environ.get("WATCH_MIN_NET_USD_PER_CEDEAR", "0.50"))  # was 0.05
ADR_MAX_ABS_5M_PCT = float(os.environ.get("ADR_MAX_ABS_5M_PCT", "0.25"))

# Trade size you want to validate (USD 300–500 typical)
TARGET_USD = float(os.environ.get("TARGET_USD", "500"))

# Liquidity filters
MIN_MONTO_OPERADO_ARS = int(os.environ.get("MIN_MONTO_OPERADO_ARS", "50000000"))  # 50M ARS
MIN_TOP_QTY = int(os.environ.get("MIN_TOP_QTY", "50"))  # mínimo en top of book (cantidad)
MIN_TOP_QTY_D = int(os.environ.get("MIN_TOP_QTY_D", "10"))  # D suele ser más finita

# Plazo (no mezclar CI vs 48)
WATCH_PLAZO_TARGET = os.environ.get("WATCH_PLAZO_TARGET", "T1")

# Allowed windows (ARG)
ALLOWED_WINDOWS = [
    (11, 0, 13, 0),
    (16, 0, 17, 0),
]

# Dedupe
ALERT_COOLDOWN_MIN = int(os.environ.get("ALERT_COOLDOWN_MIN", "20"))
ALERT_EDGE_IMPROVE_USD = float(os.environ.get("ALERT_EDGE_IMPROVE_USD", "0.05"))

WINDOW_GUARD_ENABLED = os.environ.get("WINDOW_GUARD_ENABLED", "1") == "1"


# =========================
# TIME HELPERS
# =========================
def now_arg() -> datetime:
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


def required_cedears_for_target_usd(target_usd: float, bid_d: float, ask_d: float, side: str) -> Optional[int]:
    """
    side: "COMPRA" means buy ARS then sell D -> you will SELL in D at bid_d.
          "VENTA" means sell ARS then buy D -> you will BUY in D at ask_d.
    """
    if side == "COMPRA":
        usd_per_cedear = bid_d
    else:
        usd_per_cedear = ask_d
    if not usd_per_cedear or usd_per_cedear <= 0:
        return None
    return int(math.ceil(target_usd / usd_per_cedear))


def is_executable_for_size(n: int, bid_qty_ars: int, ask_qty_ars: int, bid_qty_d: int, ask_qty_d: int, side: str) -> bool:
    """
    COMPRA: buy ARS at ask (needs ask_qty_ars), sell D at bid (needs bid_qty_d)
    VENTA : sell ARS at bid (needs bid_qty_ars), buy D at ask (needs ask_qty_d)
    """
    if not n or n <= 0:
        return False
    if side == "COMPRA":
        return (ask_qty_ars >= n) and (bid_qty_d >= n)
    else:
        return (bid_qty_ars >= n) and (ask_qty_d >= n)


def instruction_block(side: str) -> str:
    if side == "COMPRA":
        return "✅ BARATO en ARS / CARO en D → Comprá ARS → Vendé D"
    return "✅ CARO en ARS / BARATO en D → Vendé ARS → Comprá D"


def footer_instructions() -> str:
    return (
        "\n\n🧾 Instrucciones rápidas:\n"
        "1) Abrí ARS y D del ticker elegido\n"
        "2) Mirá puntas (bid/ask) y confirmá que sigue >2%\n"
        "3) Ejecutá la dirección indicada (mismo plazo)\n"
        "4) Si no se confirma en 30s → no operar\n"
    )


# =========================
# FX REF (MEP) via IOL
# =========================
def get_mep_ref(iol: IOLClient, plazo_target: str) -> Optional[float]:
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
    rows = ws_state.get_all_records()
    state: Dict[str, Dict[str, Any]] = {}
    for idx, r in enumerate(rows, start=2):
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
    t = ticker.upper()
    side = side.upper()

    if t not in state:
        return True

    prev = state[t]
    prev_side = (prev.get("last_side") or "").upper()
    prev_time = prev.get("last_sent_at")
    prev_edge = prev.get("last_edge_net")

    if prev_side and prev_side != side:
        return True

    if not prev_time:
        return True

    minutes = (now_dt - prev_time).total_seconds() / 60.0
    if minutes >= ALERT_COOLDOWN_MIN:
        return True

    if prev_edge is None:
        return True

    if (edge_net - prev_edge) >= ALERT_EDGE_IMPROVE_USD:
        return True

    return False


def upsert_alert_state(ws_state, state: Dict[str, Dict[str, Any]], ticker: str, side: str, edge_net: float, now_dt: datetime):
    t = ticker.upper()
    side = side.upper()
    iso = now_dt.replace(microsecond=0).isoformat()

    if t in state:
        row = state[t]["row_index"]
        ws_state.update(f"A{row}:D{row}", [[t, side, iso, edge_net]])
    else:
        ws_state.append_row([t, side, iso, edge_net])


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
            "edge_buy_net","edge_sell_net",
            "arb_side","arb_edge_net",
            "recommended_side","recommended_steps","n_cedears_target",
            "source"
        ],
    )

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
        return

    mep_ref = get_mep_ref(iol, WATCH_PLAZO_TARGET)
    if not mep_ref:
        return

    watchlist = get_all_records(sheet, WATCHLIST_SHEET)
    alerts_to_send: List[str] = []
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

        bid_qty_i = int(bid_qty) if bid_qty is not None else 0
        ask_qty_i = int(ask_qty) if ask_qty is not None else 0

        if monto_ars is not None and monto_ars < MIN_MONTO_OPERADO_ARS:
            continue
        if bid_qty_i < MIN_TOP_QTY or ask_qty_i < MIN_TOP_QTY:
            continue

        # --- D quote ---
        ticker_d = (w.get("ticker_d") or "").strip().upper()
        sym_d = ticker_d if ticker_d else guess_d_symbol(ticker)

        q_d = iol.get_quote(IOL_MERCADO, sym_d)
        p_d = parse_iol_quote(q_d) if q_d else None

        bid_d = ask_d = plazo_d = None
        bid_qty_d = ask_qty_d = 0
        has_d = False

        if p_d:
            bid_d = p_d.get("bid")
            ask_d = p_d.get("ask")
            plazo_d = p_d.get("plazo")
            bid_qty_d = int(p_d.get("bid_qty") or 0)
            ask_qty_d = int(p_d.get("ask_qty") or 0)

            if plazo_d == WATCH_PLAZO_TARGET and bid_d is not None and ask_d is not None:
                has_d = True

        # Need D for actionable ARS<->D guidance
        if not has_d:
            continue

        # D liquidity (more permissive but not zero)
        if bid_qty_d < MIN_TOP_QTY_D or ask_qty_d < MIN_TOP_QTY_D:
            continue

        # ADR proxy
        stock_usd = stock_usd_price(ticker)
        adr_5m = stock_usd_change_5m_pct(ticker)
        if stock_usd is None or adr_5m is None:
            continue

        if abs(adr_5m) > ADR_MAX_ABS_5M_PCT:
            continue

        # MEP-anchored implicit via stock USD (legacy metrics)
        ccl_buy = ccl_implicit(ask, stock_usd, ratio)
        ccl_sell = ccl_implicit(bid, stock_usd, ratio)
        if not ccl_buy or not ccl_sell:
            continue

        diff_buy = (ccl_buy - mep_ref) / mep_ref * 100
        diff_sell = (ccl_sell - mep_ref) / mep_ref * 100

        pack = edges_intuitive(bid, ask, stock_usd, ratio, mep_ref, BROKER_FEE_PCT)
        if not pack:
            continue

        # ARS vs D net edge (execution-like, based on marks)
        price_ars_mark = (bid + ask) / 2.0
        price_d_mark = (bid_d + ask_d) / 2.0
        usd_per_cedear_ars = price_ars_mark / mep_ref
        usd_per_cedear_d = price_d_mark

        edge_gross = usd_per_cedear_d - usd_per_cedear_ars
        base = (usd_per_cedear_ars + usd_per_cedear_d) / 2.0
        fees_rt = base * ((2 * BROKER_FEE_PCT) / 100.0)
        arb_edge_net = edge_gross - fees_rt

        arb_side = ""
        if usd_per_cedear_ars < usd_per_cedear_d:
            arb_side = "barato en ARS / caro en D"
        elif usd_per_cedear_ars > usd_per_cedear_d:
            arb_side = "caro en ARS / barato en D"

        # Recommended direction based on ARS vs D relation
        # If ARS cheaper than D -> COMPRA ARS then VENDER D (gain USD)
        # If ARS more expensive than D -> VENTA ARS then COMPRAR D
        recommended_side = "COMPRA" if arb_side == "barato en ARS / caro en D" else "VENTA"
        recommended_steps = instruction_block(recommended_side)

        # Size feasibility
        n_cedears = required_cedears_for_target_usd(TARGET_USD, bid_d, ask_d, recommended_side) or 0
        executable = is_executable_for_size(n_cedears, bid_qty_i, ask_qty_i, bid_qty_d, ask_qty_d, recommended_side)

        # Save history always (auditable)
        ws_hist.append_row([
            today, hhmm, ticker, ratio,
            stock_usd, adr_5m,
            bid, ask,
            bid_qty_i, ask_qty_i,
            plazo_ars, monto_ars if monto_ars is not None else "",
            bid_d, ask_d, bid_qty_d, ask_qty_d, plazo_d,
            mep_ref,
            ccl_buy, ccl_sell,
            diff_buy, diff_sell,
            pack["edge_buy_net"], pack["edge_sell_net"],
            arb_side,
            arb_edge_net if arb_edge_net is not None else "",
            recommended_side, recommended_steps, n_cedears if n_cedears else "",
            "IOL"
        ])

        # --- Alert selection (strict, non-marginal) ---
        # Use "recommended_side" and the corresponding net edge from pack:
        # If recommended is COMPRA -> we care about edge_buy_net and diff_buy (negative vs MEP)
        # If recommended is VENTA  -> edge_sell_net and diff_sell (positive vs MEP)
        if recommended_side == "COMPRA":
            side = "COMPRA"
            diff_pct = diff_buy
            edge_net = float(pack["edge_buy_net"])
        else:
            side = "VENTA"
            diff_pct = diff_sell
            edge_net = float(pack["edge_sell_net"])

        if abs(diff_pct) < WATCH_MIN_DIFF_PCT:
            continue
        if edge_net < WATCH_MIN_NET_USD_PER_CEDEAR:
            continue
        if not executable:
            continue

        if should_send_alert(state, ticker, side, edge_net, dt_arg):
            alerts_to_send.append(
                f"🔔 {ticker} — {side} FX\n"
                f"{recommended_steps}\n"
                f"Impl: {('%.0f' % (ccl_buy if side=='COMPRA' else ccl_sell))} | MEP(AL30): {mep_ref:.0f} | {diff_pct:+.1f}% | {WATCH_PLAZO_TARGET}\n"
                f"ADR 5m: {adr_5m:+.2f}% | {hhmm}\n"
                f"Neto: {edge_net:.2f} USD/CEDEAR | ~{TARGET_USD:.0f} USD ⇒ {n_cedears} CEDEARs (book OK)"
            )
            pending_state_updates.append((ticker, side, edge_net))

    if alerts_to_send:
        msg = (
            f"👀 Watchlist intradía — oportunidades FX\n"
            f"MEP ref(AL30): {mep_ref:.0f} | {hhmm} (ARG)\n\n"
            + "\n\n".join(alerts_to_send)
            + footer_instructions()
        )
        send_telegram(msg)

        for t, side, edge in pending_state_updates:
            upsert_alert_state(ws_state, state, t, side, edge, dt_arg)


if __name__ == "__main__":
    main()

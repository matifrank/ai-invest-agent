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
WATCHLIST_ALERT_STATE_SHEET = "watchlist_alert_state"

IOL_MERCADO = "bcba"

BROKER_FEE_PCT = 0.5

# Strict / Premium thresholds
WATCH_MIN_DIFF_PCT = float(os.environ.get("WATCH_MIN_DIFF_PCT", "2.0"))
WATCH_MIN_NET_USD_PER_CEDEAR = float(os.environ.get("WATCH_MIN_NET_USD_PER_CEDEAR", "0.50"))
ADR_MAX_ABS_5M_PCT = float(os.environ.get("ADR_MAX_ABS_5M_PCT", "0.25"))

# Target sizing (USD 300–500)
TARGET_USD = float(os.environ.get("TARGET_USD", "500"))

# Liquidity filters (ARS)
MIN_MONTO_OPERADO_ARS = int(os.environ.get("MIN_MONTO_OPERADO_ARS", "50000000"))  # 50M ARS

# Plazo (para no mezclar CI vs 48)
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
    side:
      - COMPRA: Comprá ARS (ask_ars) -> Vendé D (bid_d)
      - VENTA : Vendé ARS (bid_ars) -> Comprá D (ask_d)
    """
    usd_per_cedear = bid_d if side == "COMPRA" else ask_d
    if not usd_per_cedear or usd_per_cedear <= 0:
        return None
    return int(math.ceil(target_usd / usd_per_cedear))


def min_qty_thresholds_for_target(n: int) -> Tuple[int, int]:
    """
    Premium mode: pedimos ~2×n en la punta.
    ARS: mínimo 50
    D  : mínimo 20 (D suele ser más finita y si es 10 se vuelve ruidoso)
    """
    if not n or n <= 0:
        return (50, 20)
    min_ars = max(50, 2 * n)
    min_d = max(20, 2 * n)
    return (min_ars, min_d)


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


def side_for_direction(arb_side: str) -> str:
    if arb_side == "barato en ARS / caro en D":
        return "COMPRA"
    return "VENTA"


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
            "recommended_side","n_cedears_target",
            "min_book_ars","min_book_d",
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

        # --- D quote ---
        ticker_d = (w.get("ticker_d") or "").strip().upper()
        sym_d = ticker_d if ticker_d else guess_d_symbol(ticker)

        q_d = iol.get_quote(IOL_MERCADO, sym_d)
        p_d = parse_iol_quote(q_d) if q_d else None
        if not p_d:
            continue

        bid_d = p_d.get("bid")
        ask_d = p_d.get("ask")
        plazo_d = p_d.get("plazo")
        bid_qty_d = int(p_d.get("bid_qty") or 0)
        ask_qty_d = int(p_d.get("ask_qty") or 0)

        if plazo_d != WATCH_PLAZO_TARGET:
            continue
        if bid_d is None or ask_d is None:
            continue

        # ADR proxy (NYSE)
        stock_usd = stock_usd_price(ticker)
        adr_5m = stock_usd_change_5m_pct(ticker)
        if stock_usd is None or adr_5m is None:
            continue
        if abs(adr_5m) > ADR_MAX_ABS_5M_PCT:
            continue

        # Legacy implicit vs MEP based on stock USD anchor
        ccl_buy = ccl_implicit(ask, stock_usd, ratio)
        ccl_sell = ccl_implicit(bid, stock_usd, ratio)
        if not ccl_buy or not ccl_sell:
            continue

        diff_buy = (ccl_buy - mep_ref) / mep_ref * 100
        diff_sell = (ccl_sell - mep_ref) / mep_ref * 100

        pack = edges_intuitive(bid, ask, stock_usd, ratio, mep_ref, BROKER_FEE_PCT)
        if not pack:
            continue

        # --- ARS vs D relation (for direction) ---
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
        else:
            continue

        recommended_side = side_for_direction(arb_side)

        # --- Size & dynamic liquidity (2×n premium) ---
        n_cedears = required_cedears_for_target_usd(TARGET_USD, bid_d, ask_d, recommended_side)
        if not n_cedears:
            continue

        min_book_ars, min_book_d = min_qty_thresholds_for_target(n_cedears)

        # Need both sides healthy to avoid phantom book
        if bid_qty_i < min_book_ars or ask_qty_i < min_book_ars:
            continue
        if bid_qty_d < min_book_d or ask_qty_d < min_book_d:
            continue

        if not is_executable_for_size(n_cedears, bid_qty_i, ask_qty_i, bid_qty_d, ask_qty_d, recommended_side):
            continue

        # Pick matching diff/edge to enforce non-marginal opportunities
        if recommended_side == "COMPRA":
            side = "COMPRA"
            diff_pct = diff_buy
            edge_net = float(pack["edge_buy_net"])
            impl_show = ccl_buy
        else:
            side = "VENTA"
            diff_pct = diff_sell
            edge_net = float(pack["edge_sell_net"])
            impl_show = ccl_sell

        if abs(diff_pct) < WATCH_MIN_DIFF_PCT:
            continue
        if edge_net < WATCH_MIN_NET_USD_PER_CEDEAR:
            continue

        # Save history (auditable)
        ws_hist.append_row([
            today, hhmm, ticker, ratio,
            stock_usd, adr_5m,
            bid, ask, bid_qty_i, ask_qty_i,
            plazo_ars, monto_ars if monto_ars is not None else "",
            bid_d, ask_d, bid_qty_d, ask_qty_d, plazo_d,
            mep_ref,
            ccl_buy, ccl_sell,
            diff_buy, diff_sell,
            pack["edge_buy_net"], pack["edge_sell_net"],
            arb_side, arb_edge_net,
            side, n_cedears,
            min_book_ars, min_book_d,
            "IOL"
        ])

        if should_send_alert(state, ticker, side, edge_net, dt_arg):
            alerts_to_send.append(
                f"🔔 {ticker} — {side} FX\n"
                f"{instruction_block(side)}\n"
                f"Impl: {impl_show:.0f} | MEP(AL30): {mep_ref:.0f} | {diff_pct:+.1f}% | {WATCH_PLAZO_TARGET}\n"
                f"ADR 5m: {adr_5m:+.2f}% | {hhmm}\n"
                f"Neto: {edge_net:.2f} USD/CEDEAR | ~{TARGET_USD:.0f} USD ⇒ {n_cedears} CEDEARs\n"
                f"Book mínimo usado: ARS≥{min_book_ars} | D≥{min_book_d}"
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

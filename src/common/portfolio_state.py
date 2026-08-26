from typing import Dict, Any, Tuple, List, Optional
from src.common.sheets import get_all_records
from src.common.calc import ccl_implicit
from src.common.pricing import get_ars_price, get_usd_price
from src.common.iol import IOLClient


def compute_portfolio_state(sheet, iol: Optional[IOLClient] = None) -> Tuple[float, float, Dict[str, Dict[str, Any]], Dict[str, str]]:
    """Compute portfolio totals and per-ticker details.

    Returns: total_usd, tactical_usd, details, skipped
    details: {ticker: {qty, ppc, last_ars, stock_usd, ccl_impl, usd_val, pnl, classification}}
    skipped: {ticker: reason}
    """
    portfolio = get_all_records(sheet, "portfolio")

    # read tactical_positions
    tactical_set = set()
    try:
        tactical_rows = get_all_records(sheet, "tactical_positions")
        for r in tactical_rows:
            t = (r.get("ticker") or "").strip()
            if t:
                tactical_set.add(t)
    except Exception:
        tactical_set = set()

    total_usd = 0.0
    tactical_usd = 0.0
    details: Dict[str, Dict[str, Any]] = {}
    skipped: Dict[str, str] = {}

    for p in portfolio:
        ticker = (p.get("ticker") or "").strip()
        if not ticker:
            continue

        # parse numbers
        try:
            qty = float(str(p.get("cantidad") or p.get("qty") or 0).replace(',', ''))
        except Exception:
            qty = 0.0
        try:
            ppc = float(str(p.get("ppc") or p.get("price_paid") or 0).replace(',', ''))
        except Exception:
            ppc = 0.0
        try:
            ratio = float(str(p.get("ratio") or 1.0).replace(',', ''))
        except Exception:
            ratio = 1.0

        # resolve prices
        last_ars = get_ars_price(sheet, ticker, p, iol)
        stock_usd = get_usd_price(ticker)

        if stock_usd is None:
            skipped[ticker] = "no stock_usd found"
            continue
        if last_ars is None:
            skipped[ticker] = "no last ARS price"
            continue

        ccl_impl = ccl_implicit(last_ars, stock_usd, ratio)
        if not ccl_impl:
            skipped[ticker] = "no ccl_impl"
            continue

        usd_val = qty * last_ars / ccl_impl if ccl_impl else 0.0
        pnl = qty * ((last_ars - ppc) / ccl_impl) if ccl_impl else 0.0

        # classification: bucket or strategy/tag
        classification = (p.get("bucket") or p.get("strategy") or p.get("tag") or "").strip().upper()
        if not classification:
            # fallback to tactical_positions set
            if ticker in tactical_set:
                classification = "TACTICAL"
            else:
                # default to CORE for CEDEAR otherwise UNKNOWN
                tipo = (p.get("tipo") or "").strip().upper()
                classification = "CORE" if tipo == "CEDEAR" else "UNKNOWN"

        total_usd += usd_val
        if classification == "TACTICAL":
            tactical_usd += usd_val

        details[ticker] = {
            "qty": qty,
            "ppc": ppc,
            "last_ars": last_ars,
            "stock_usd": stock_usd,
            "ccl_impl": ccl_impl,
            "usd_val": usd_val,
            "pnl": pnl,
            "classification": classification,
        }

    return total_usd, tactical_usd, details, skipped

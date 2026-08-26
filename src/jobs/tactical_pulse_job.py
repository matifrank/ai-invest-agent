import os
from datetime import date
from typing import Dict, Any, List, Optional

from src.common.sheets import connect_sheets, ensure_worksheet, get_all_records
from src.common.telegram import send_telegram
from src.common.iol import IOLClient, parse_iol_quote, get_last_price
from src.common.yahoo import stock_usd_price
from src.common.calc import ccl_implicit

SPREADSHEET_NAME = "ai-portfolio-agent"
PORTFOLIO_SHEET = "portfolio"
AGENT_CONFIG_SHEET = "agent_config"


def read_agent_config(sheet) -> Dict[str, Any]:
    try:
        rows = get_all_records(sheet, AGENT_CONFIG_SHEET)
    except Exception:
        return {}
    cfg: Dict[str, Any] = {}
    for r in rows:
        k = (r.get("key") or r.get("name") or "").strip()
        v = r.get("value")
        if not k:
            continue
        cfg[k] = v
    return cfg


def safe_float(x) -> Optional[float]:
    try:
        if x is None:
            return None
        if isinstance(x, str) and x.strip() == "":
            return None
        return float(x)
    except Exception:
        return None


def classify_portfolio_row(p: Dict[str, Any]) -> str:
    # Prefer explicit 'tipo' values, fallback to 'tag' or 'allocation'
    tipo = (p.get("tipo") or "").strip().upper()
    tag = (p.get("tag") or "").strip().upper()
    alloc = (p.get("allocation") or "").strip().upper()
    for val in (tipo, tag, alloc):
        if val in ("TACTICAL", "TACTIC", "T"):
            return "TACTICAL"
        if val in ("CORE", "C", "CORE SWEEP"):
            return "CORE"
    return "UNKNOWN"


def format_money(x: float) -> str:
    return f"${x:,.2f}"


def main(dry_run: bool = True, send_report: bool = True):
    sheet = connect_sheets(SPREADSHEET_NAME)

    cfg = read_agent_config(sheet)
    tactical_target_pct = safe_float(cfg.get("tactical_target_pct") or 0.10) or 0.10

    iol = None
    if os.environ.get("IOL_USERNAME") and os.environ.get("IOL_PASSWORD"):
        iol = IOLClient(os.environ["IOL_USERNAME"], os.environ["IOL_PASSWORD"])

    portfolio = get_all_records(sheet, PORTFOLIO_SHEET)

    total_usd = 0.0
    tactical_usd = 0.0
    details: Dict[str, Dict[str, Any]] = {}

    for p in portfolio:
        ticker = (p.get("ticker") or "").strip()
        if not ticker:
            continue
        qty = safe_float(p.get("cantidad") or p.get("qty") or 0) or 0.0
        ppc = safe_float(p.get("ppc") or p.get("price_paid") or 0) or 0.0
        ratio = safe_float(p.get("ratio") or 1.0) or 1.0

        # Determine current ARS price via IOL then fallback
        last_ars = None
        if iol:
            try:
                last_ars = get_last_price(iol, "bcba", ticker)
            except Exception:
                last_ars = None

        if last_ars is None:
            # try to parse from other sources or skip
            last_ars = None

        stock_usd = stock_usd_price(ticker)
        if stock_usd is None:
            continue

        if last_ars is None:
            # can't compute ccl, skip
            continue

        ccl_impl = ccl_implicit(last_ars, stock_usd, ratio)
        if not ccl_impl:
            continue

        usd_val = qty * last_ars / ccl_impl if ccl_impl else 0.0
        pnl = qty * ( (last_ars - ppc) / ccl_impl ) if ccl_impl else 0.0

        classification = classify_portfolio_row(p)
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

    # target tactical USD and delta
    target_tactical_usd = tactical_target_pct * total_usd
    tactical_delta = target_tactical_usd - tactical_usd

    # Build report
    now = str(date.today())
    header = f"📡 Tactical Pulse - Dry Run ({now})\n"
    body = ""
    body += f"Total portfolio USD: {format_money(total_usd)}\n"
    body += f"Current tactical sleeve USD: {format_money(tactical_usd)}\n"
    body += f"Target tactical sleeve ({tactical_target_pct*100:.1f}%): {format_money(target_tactical_usd)}\n"
    body += f"Delta (target - current): {format_money(tactical_delta)}\n\n"

    body += "Top tactical positions:\n"
    tactical_sorted = sorted(((t, d) for t, d in details.items() if d["classification"] == "TACTICAL"), key=lambda kv: kv[1]["usd_val"], reverse=True)
    for t, d in tactical_sorted[:10]:
        body += f"- {t}: {format_money(d['usd_val'])} (P&L {format_money(d['pnl'])})\n"

    body += "\nPotential Core Sweep candidates (largest Core positions):\n"
    core_sorted = sorted(((t, d) for t, d in details.items() if d["classification"] == "CORE"), key=lambda kv: kv[1]["usd_val"], reverse=True)
    for t, d in core_sorted[:5]:
        body += f"- {t}: {format_money(d['usd_val'])} (P&L {format_money(d['pnl'])})\n"

    report = header + body

    print(report)
    if send_report and send_telegram and dry_run:
        try:
            send_telegram(report)
        except Exception as e:
            print("Failed sending telegram:", e)


if __name__ == "__main__":
    main(dry_run=True, send_report=True)

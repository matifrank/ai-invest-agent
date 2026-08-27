"""
Lightweight entrypoint that delegates to `main_refactor.py`.

This keeps the repository backwards-compatible while centralizing the
implementation in `main_refactor.py`.
"""
import sys
import os

# Prefer running from repo root
sys.path.insert(0, os.path.dirname(__file__))

try:
    from main_refactor import main as run_main
except Exception as e:
    raise SystemExit(f"Failed to import refactored main: {e}")


if __name__ == "__main__":
    run_main()


# =========================
# MAIN
# =========================
def main():
    print("🚀 Iniciando pipeline")
    if not in_allowed_window():
        print("⏱ Fuera de ventana operativa, no corro watchlist/portfolio.")
        return

    sheet = connect_sheets()

    ws_port_hist = ensure_worksheet(
        sheet,
        PORTFOLIO_HISTORY_SHEET,
        header=[
            "date", "ticker", "qty", "ppc_ars",
            "mark_ars", "bid_ars", "ask_ars",
            "ratio", "stock_usd", "ccl_impl",
            "usd_value", "gain_usd", "source"
        ],
    )

    ws_watch_hist = ensure_worksheet(
        sheet,
        WATCHLIST_HISTORY_SHEET,
        header=WATCHLIST_HISTORY_HEADER,
    )

    portfolio = get_all_records(sheet, PORTFOLIO_SHEET)
    watchlist = get_all_records(sheet, WATCHLIST_SHEET)
    ccl_mkt = get_ccl_market()
    today = str(date.today())
    hhmm = hhmm_arg()

    iol = None
    if os.environ.get("IOL_USERNAME") and os.environ.get("IOL_PASSWORD"):
        iol = IOLClient(os.environ["IOL_USERNAME"], os.environ["IOL_PASSWORD"])

    # ---------- PORTFOLIO ----------
    total_ars = 0.0
    total_usd = 0.0
    dist: Dict[str, Tuple[float, float, float]] = {}

    for p in portfolio:
        ticker = (p.get("ticker") or "").strip()
        tipo = (p.get("tipo") or "").upper().strip()
        qty = safe_float(p.get("cantidad"))
        ppc = safe_float(p.get("ppc"))
        ratio = safe_float(p.get("ratio")) or 1.0

        if not ticker or not qty or tipo != "CEDEAR":
            continue

        last = bid = ask = None
        src = "YAHOO"

        if iol:
            q = iol.get_quote(IOL_MERCADO, ticker)
            if q:
                parsed = parse_iol_quote_full(q)
                last = parsed["last"]
                bid = parsed["bid"]
                ask = parsed["ask"]
                src = "IOL"

        if last is None and bid is None and ask is None:
            last = yahoo_cedear_price_ars(ticker)
            src = "YAHOO"

        price = pick_portfolio_price(last, bid, ask)
        if price is None:
            continue

        mark = (bid + ask) / 2.0 if (bid is not None and ask is not None) else last

        stock_usd = yahoo_stock_price_usd(ticker)
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

        update_portfolio_last_price(sheet, ticker, price)
        append_price_daily(sheet, ticker, price, src)

        ws_port_hist.append_row([
            today, ticker, qty, ppc,
            mark if mark is not None else "",
            bid if bid is not None else "",
            ask if ask is not None else "",
            ratio, stock_usd, ccl_impl_now,
            usd_val, gain, src
        ])

    # ---------- WATCHLIST (ARS vs D) ----------
    watch_opps: List[Tuple[float, str]] = []

    for w in watchlist:
        ticker = (w.get("ticker") or "").strip().upper()
        tipo = (w.get("tipo") or "").upper().strip()
        ratio = safe_float(w.get("ratio")) or 1.0
        ticker_d = (w.get("ticker_d") or "").strip().upper()

        if not ticker or tipo != "CEDEAR":
            continue

        sym_d = ticker_d if ticker_d else guess_d_symbol(ticker)

        if not iol or not ccl_mkt:
            continue

        q_ars = iol.get_quote(IOL_MERCADO, ticker)
        q_d = iol.get_quote(IOL_MERCADO, sym_d)

        if not q_ars or not q_d:
            continue

        ars = parse_iol_quote_full(q_ars)
        d = parse_iol_quote_full(q_d)

        bid_ars = ars["bid"]
        ask_ars = ars["ask"]
        bid_qty_ars = ars["bid_qty"]
        ask_qty_ars = ars["ask_qty"]
        plazo_ars = ars["plazo"]
        monto_ars = ars["monto"]

        bid_d = d["bid"]
        ask_d = d["ask"]
        bid_qty_d = d["bid_qty"]
        ask_qty_d = d["ask_qty"]
        plazo_d = d["plazo"]

        if bid_ars is None or ask_ars is None or bid_d is None or ask_d is None:
            continue
        if plazo_ars != plazo_d:
            continue
        if monto_ars is not None and monto_ars < MIN_MONTO_OPERADO_ARS:
            continue

        # USD implícito del CEDEAR usando CCL de mercado
        usd_ars_bid = usd_per_cedear(bid_ars, ccl_mkt)
        usd_ars_ask = usd_per_cedear(ask_ars, ccl_mkt)

        if usd_ars_bid is None or usd_ars_ask is None:
            continue

        # COMPRA FX: comprás ARS al ask, vendés D al bid
        diff_buy_pct = ((bid_d - usd_ars_ask) / usd_ars_ask) * 100 if usd_ars_ask > 0 else None
        edge_buy_gross = bid_d - usd_ars_ask
        fee_buy = fee_roundtrip_usd(usd_ars_ask, BROKER_FEE_PCT) or 0.0
        edge_buy_net = edge_buy_gross - fee_buy

        # VENTA FX: vendés ARS al bid, comprás D al ask
        diff_sell_pct = ((usd_ars_bid - ask_d) / ask_d) * 100 if ask_d > 0 else None
        edge_sell_gross = usd_ars_bid - ask_d
        fee_sell = fee_roundtrip_usd(usd_ars_bid, BROKER_FEE_PCT) or 0.0
        edge_sell_net = edge_sell_gross - fee_sell

        recommended_side = ""
        diff_pct = None
        edge_net = None
        n_target = None
        min_book_ars = None
        min_book_d = None
        arb_side = ""
        arb_edge_net = None

        # COMPRA
        if diff_buy_pct is not None and diff_buy_pct >= WATCH_MIN_DIFF_PCT and edge_buy_net >= WATCH_MIN_NET_USD_PER_CEDEAR:
            n_target = required_cedears_for_target_usd(TARGET_USD, bid_d, ask_d, "COMPRA")
            if n_target:
                min_book_ars, min_book_d = min_qty_thresholds_for_target(n_target)
                if (
                    bid_qty_ars >= MIN_TOP_QTY_ARS and ask_qty_ars >= MIN_TOP_QTY_ARS and
                    bid_qty_d >= MIN_TOP_QTY_D and ask_qty_d >= MIN_TOP_QTY_D and
                    is_executable_for_size(n_target, bid_qty_ars, ask_qty_ars, bid_qty_d, ask_qty_d, "COMPRA")
                ):
                    recommended_side = "COMPRA"
                    diff_pct = diff_buy_pct
                    edge_net = edge_buy_net
                    arb_side = "barato en ARS / caro en D"
                    arb_edge_net = edge_buy_net

        # VENTA
        if not recommended_side and diff_sell_pct is not None and diff_sell_pct >= WATCH_MIN_DIFF_PCT and edge_sell_net >= WATCH_MIN_NET_USD_PER_CEDEAR:
            n_target = required_cedears_for_target_usd(TARGET_USD, bid_d, ask_d, "VENTA")
            if n_target:
                min_book_ars, min_book_d = min_qty_thresholds_for_target(n_target)
                if (
                    bid_qty_ars >= MIN_TOP_QTY_ARS and ask_qty_ars >= MIN_TOP_QTY_ARS and
                    bid_qty_d >= MIN_TOP_QTY_D and ask_qty_d >= MIN_TOP_QTY_D and
                    is_executable_for_size(n_target, bid_qty_ars, ask_qty_ars, bid_qty_d, ask_qty_d, "VENTA")
                ):
                    recommended_side = "VENTA"
                    diff_pct = diff_sell_pct
                    edge_net = edge_sell_net
                    arb_side = "caro en ARS / barato en D"
                    arb_edge_net = edge_sell_net

        if not recommended_side:
            continue

        flag = opportunity_flag(
            edge_net=edge_net,
            diff_pct=diff_pct,
            n_cedears=n_target,
            bid_qty_ars=bid_qty_ars,
            ask_qty_ars=ask_qty_ars,
            bid_qty_d=bid_qty_d,
            ask_qty_d=ask_qty_d,
        )

        # guarda solo oportunidades
        append_row_aligned(ws_watch_hist, WATCHLIST_HISTORY_HEADER, [
            today, hhmm, ticker, sym_d, ratio,
            bid_ars, ask_ars, bid_qty_ars, ask_qty_ars, monto_ars if monto_ars is not None else "", plazo_ars,
            bid_d, ask_d, bid_qty_d, ask_qty_d, plazo_d,
            ccl_mkt,
            usd_ars_bid, usd_ars_ask,
            diff_buy_pct, diff_sell_pct,
            edge_buy_gross, edge_sell_gross,
            fee_buy, fee_sell,
            edge_buy_net, edge_sell_net,
            recommended_side, n_target, min_book_ars, min_book_d,
            "IOL"
        ])

        usd_trade = edge_net * n_target if n_target else 0.0
        side_text = "Comprá ARS → Vendé D" if recommended_side == "COMPRA" else "Vendé ARS → Comprá D"

        watch_opps.append((
            edge_net,
            f"{flag} ⚡ {ticker} {recommended_side}\n"
            f"{side_text}\n"
            f"diff {diff_pct:+.2f}%\n"
            f"edge {edge_net:.2f} USD/CEDEAR\n"
            f"≈ {usd_trade:.2f} USD por {n_target} CEDEAR\n"
            f"book ARS {bid_qty_ars}/{ask_qty_ars} | D {bid_qty_d}/{ask_qty_d}"
        ))

    # ---------- TELEGRAM ----------
    msg = (
        "📊 AI Portfolio Daily - Broker Mode\n\n"
        f"Valor ARS: ${total_ars:,.0f}\n"
        f"Valor USD real (CCL implícito por activo): ${total_usd:,.2f}\n\n"
        "Distribución principal:\n"
    )

    for t, (usd_val, gain, ccl_impl_now) in sorted(dist.items(), key=lambda kv: kv[1][0], reverse=True)[:3]:
        msg += f"- {t}: ${usd_val:,.2f} ({gain:+.2f} USD, CCL {ccl_impl_now:.0f})\n"

    msg += "\n🚨 Ganancia / pérdida cartera:\n"
    for t, (_, gain, _) in sorted(dist.items(), key=lambda kv: kv[1][0], reverse=True):
        msg += f"{'📈' if gain >= 0 else '📉'} {t}: {gain:+.2f} USD\n"

    if watch_opps:
        msg += f"\n👀 Watchlist oportunidades ARS vs D (umbral {WATCH_MIN_DIFF_PCT:.1f}% | neto {WATCH_MIN_NET_USD_PER_CEDEAR:.2f} USD):\n\n"
        watch_opps_sorted = [m for _, m in sorted(watch_opps, key=lambda x: x[0], reverse=True)]
        msg += "\n\n".join(watch_opps_sorted)
        msg += "\n\nPipeline funcionando 🤖"
        send_telegram(msg)
    else:
        print("No watchlist opportunities today")


if __name__ == "__main__":
    main()
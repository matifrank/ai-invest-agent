from typing import Optional, Dict

def ccl_implicit(cedear_ars: float, stock_usd: float, ratio: float) -> Optional[float]:
    if not cedear_ars or not stock_usd or not ratio:
        return None
    if stock_usd <= 0 or ratio <= 0:
        return None
    return (cedear_ars * ratio) / stock_usd

def usd_value(qty: float, price_ars: float, ccl_impl: float) -> float:
    if not ccl_impl or ccl_impl <= 0:
        return 0.0
    return (qty * price_ars) / ccl_impl

def gain_usd(qty: float, ppc_ars: float, current_ars: float, ccl_impl: float) -> float:
    if ppc_ars is None or not ccl_impl or ccl_impl <= 0:
        return 0.0
    return qty * (current_ars - ppc_ars) / ccl_impl

def edges_intuitive(bid_ars: float, ask_ars: float, stock_usd: float, ratio: float, ccl_mkt: float, fee_pct_per_tx: float) -> Optional[Dict[str, float]]:
    if not ccl_mkt or ccl_mkt <= 0:
        return None

    usd_ask = ask_ars / ccl_mkt
    usd_bid = bid_ars / ccl_mkt
    usd_stock = stock_usd / ratio

    edge_buy_gross = usd_stock - usd_ask
    edge_sell_gross = usd_bid - usd_stock

    fee_buy = usd_ask * ((2 * fee_pct_per_tx) / 100.0)
    fee_sell = usd_bid * ((2 * fee_pct_per_tx) / 100.0)

    return {
        "usd_cedear_ask": usd_ask,
        "usd_cedear_bid": usd_bid,
        "usd_stock_per_cedear": usd_stock,
        "edge_buy_gross": edge_buy_gross,
        "edge_sell_gross": edge_sell_gross,
        "fee_buy_usd_rt": fee_buy,
        "fee_sell_usd_rt": fee_sell,
        "edge_buy_net": edge_buy_gross - fee_buy,
        "edge_sell_net": edge_sell_gross - fee_sell,
    }

from typing import Optional, Dict

def ccl_implicit(cedear_ars: float, stock_usd: float, ratio: float) -> Optional[float]:
    if not cedear_ars or not stock_usd or not ratio:
        return None
    if stock_usd <= 0 or ratio <= 0:
        return None
    return (cedear_ars * ratio) / stock_usd

def edges_intuitive(bid_ars: float, ask_ars: float, stock_usd: float, ratio: float, fx_ref: float, fee_pct_per_tx: float) -> Optional[Dict[str, float]]:
    if not fx_ref or fx_ref <= 0:
        return None

    usd_ask = ask_ars / fx_ref
    usd_bid = bid_ars / fx_ref
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

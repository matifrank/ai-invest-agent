"""Create example sheets and sample rows for Tactical Pulse testing.

Usage:
  python -m src.scripts.setup_example_sheets

Requires env:
  - PORTFOLIO_GS_CREDS (service account JSON string)
"""
from typing import List, Dict
from datetime import date
from src.common.sheets import connect_sheets, ensure_worksheet, get_all_records

SPREADSHEET_NAME = "ai-portfolio-agent"


TRADING_JOURNAL_HEADER = ["trade_id", "date", "ticker", "side", "qty", "price", "tag", "notes", "source"]
TACTICAL_POSITIONS_HEADER = ["ticker", "qty", "avg_price_ars", "avg_price_usd", "ratio", "entry_date", "status", "notes"]


EXAMPLE_JOURNAL: List[Dict[str, str]] = [
    {"trade_id": "TJ-001", "date": str(date.today()), "ticker": "AAPL", "side": "buy", "qty": "10", "price": "350", "tag": "TACTICAL", "notes": "initial tactical entry", "source": "manual"},
    {"trade_id": "TJ-002", "date": str(date.today()), "ticker": "TSLA", "side": "sell", "qty": "2", "price": "900", "tag": "CORE", "notes": "partial lock", "source": "manual"},
]

EXAMPLE_TACTICAL: List[Dict[str, str]] = [
    {"ticker": "AAPL", "qty": "10", "avg_price_ars": "35000", "avg_price_usd": "350", "ratio": "1", "entry_date": str(date.today()), "status": "open", "notes": "demo tactical"},
    {"ticker": "MSFT", "qty": "5", "avg_price_ars": "28000", "avg_price_usd": "280", "ratio": "1", "entry_date": str(date.today()), "status": "open", "notes": "demo tactical"},
]


def append_if_missing(ws, header: List[str], row_values: List[str]):
    vals = ws.get_all_values()
    # if only header exists or empty, append
    if not vals or len(vals) <= 1:
        ws.append_row(row_values)
        return True
    # check if row exists (simple check: same first col)
    first_col = row_values[0]
    for r in vals[1:]:
        if r and r[0] == first_col:
            return False
    ws.append_row(row_values)
    return True


def main():
    sheet = connect_sheets(SPREADSHEET_NAME)

    ws_journal = ensure_worksheet(sheet, "trading_journal", header=TRADING_JOURNAL_HEADER)
    ws_tactical = ensure_worksheet(sheet, "tactical_positions", header=TACTICAL_POSITIONS_HEADER)

    added_j = 0
    for r in EXAMPLE_JOURNAL:
        row = [r.get(h, "") for h in TRADING_JOURNAL_HEADER]
        if append_if_missing(ws_journal, TRADING_JOURNAL_HEADER, row):
            added_j += 1

    added_t = 0
    for r in EXAMPLE_TACTICAL:
        row = [r.get(h, "") for h in TACTICAL_POSITIONS_HEADER]
        if append_if_missing(ws_tactical, TACTICAL_POSITIONS_HEADER, row):
            added_t += 1

    print(f"trading_journal: added {added_j} rows; tactical_positions: added {added_t} rows")


if __name__ == "__main__":
    main()

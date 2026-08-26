"""Script to create an `agent_config` sheet and populate example key/value rows.

Usage:
  python -m src.scripts.setup_agent_config

Requires env:
  - PORTFOLIO_GS_CREDS (service account JSON string)
"""
from typing import List, Dict
from src.common.sheets import connect_sheets, ensure_worksheet, get_all_records

SPREADSHEET_NAME = "ai-portfolio-agent"
AGENT_CONFIG_SHEET = "agent_config"

SAMPLE_ROWS: List[Dict[str, str]] = [
    {"key": "tactical_target_pct", "value": "0.10", "description": "Target % of portfolio to allocate to tactical sleeve (0.10 = 10%)"},
    {"key": "tactical_cap_usd", "value": "10000", "description": "Optional absolute cap for tactical sleeve in USD"},
    {"key": "TOP_N_ALERTS", "value": "5", "description": "How many watchlist alerts to post"},
    {"key": "MIN_EXEC_QTY", "value": "1", "description": "Minimum executable quantity for trades"},
    {"key": "REPORT_CHAT_ID", "value": "", "description": "Optional override for Telegram chat id"},
]


def main():
    sheet = connect_sheets(SPREADSHEET_NAME)

    ws = ensure_worksheet(sheet, AGENT_CONFIG_SHEET, header=["key", "value", "description"])

    existing = get_all_records(sheet, AGENT_CONFIG_SHEET)
    existing_keys = set()
    for r in existing:
        k = (r.get("key") or "").strip()
        if k:
            existing_keys.add(k)

    added = 0
    for row in SAMPLE_ROWS:
        if row["key"] in existing_keys:
            continue
        ws.append_row([row["key"], row["value"], row["description"]])
        added += 1

    print(f"agent_config sheet ensured. Added {added} new rows (skipped {len(SAMPLE_ROWS)-added}).")


if __name__ == "__main__":
    main()

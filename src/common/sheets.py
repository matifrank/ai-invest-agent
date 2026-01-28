import os, json
import gspread
from typing import Optional, List, Dict, Any
from oauth2client.service_account import ServiceAccountCredentials

def connect_sheets(spreadsheet_name: str):
    scope = [
        "https://spreadsheets.google.com/feeds",
        "https://www.googleapis.com/auth/drive",
    ]
    creds_json = os.environ["PORTFOLIO_GS_CREDS"]
    creds_dict = json.loads(creds_json)
    creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, scope)
    client = gspread.authorize(creds)
    return client.open(spreadsheet_name)

def ensure_worksheet(sheet, title: str, rows: int = 2000, cols: int = 50, header: Optional[List[str]] = None):
    try:
        ws = sheet.worksheet(title)
    except gspread.exceptions.WorksheetNotFound:
        ws = sheet.add_worksheet(title=title, rows=rows, cols=cols)

    if header:
        values = ws.get_all_values()
        if not values:
            ws.append_row(header)
        else:
            if values[0] != header:
                ws.update("1:1", [header])
    return ws

def get_all_records(sheet, tab_name: str) -> List[Dict[str, Any]]:
    return sheet.worksheet(tab_name).get_all_records()

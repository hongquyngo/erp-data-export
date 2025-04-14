import datetime
import logging
import pytz
import pandas as pd
import streamlit as st

from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

SPREADSHEET_ID = "18uvsmtMSYQg1jacLjGF4Bj8GiX-Hjq0Cgi_PPM2Y0U4"
SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

def export_to_google_sheets(data, data_type):
    logger.info("📄 Starting export to Google Sheets...")

    credentials = service_account.Credentials.from_service_account_info(
        st.secrets["gcp_service_account"],
        scopes=SCOPES
    )
    service = build("sheets", "v4", credentials=credentials)
    sheets_api = service.spreadsheets()

    vn_tz = pytz.timezone("Asia/Ho_Chi_Minh")
    now = datetime.datetime.now(vn_tz).strftime("%Y%m%d_%H%M")
    prefix = data_type.lower().replace(" ", "_")
    new_sheet_title = f"{prefix}_{now}"

    try:
        metadata = sheets_api.get(spreadsheetId=SPREADSHEET_ID).execute()
        sheets = metadata.get("sheets", [])
        target_sheet_id = None
        old_sheet_title = None

        for s in sheets:
            title = s["properties"]["title"]
            if title.startswith(prefix):
                target_sheet_id = s["properties"]["sheetId"]
                old_sheet_title = title
                break

        if target_sheet_id:
            logger.info(f"♻️ Clearing and renaming old sheet: {old_sheet_title}")
            requests = [
                {"updateSheetProperties": {
                    "properties": {"sheetId": target_sheet_id, "title": new_sheet_title},
                    "fields": "title"
                }},
                {"updateCells": {
                    "range": {"sheetId": target_sheet_id},
                    "fields": "userEnteredValue"
                }}
            ]
            sheets_api.batchUpdate(spreadsheetId=SPREADSHEET_ID, body={"requests": requests}).execute()
        else:
            logger.info(f"🏰 Creating new sheet: {new_sheet_title}")
            sheets_api.batchUpdate(spreadsheetId=SPREADSHEET_ID, body={"requests": [
                {"addSheet": {"properties": {"title": new_sheet_title}}}
            ]}).execute()

        # Write headers
        sheets_api.values().update(
            spreadsheetId=SPREADSHEET_ID,
            range=f"{new_sheet_title}!A1",
            valueInputOption="RAW",
            body={"values": [list(data.columns)]}
        ).execute()

        format_sheet(service, SPREADSHEET_ID, new_sheet_title, data)

        # Write body
        sheets_api.values().update(
            spreadsheetId=SPREADSHEET_ID,
            range=f"{new_sheet_title}!A2",
            valueInputOption="USER_ENTERED",
            body={"values": data.astype(str).values.tolist()}
        ).execute()

        logger.info("✅ Export and formatting completed.")
        return new_sheet_title

    except Exception as e:
        logger.exception(f"❌ Error during export: {e}")
        raise

def format_sheet(service, sheet_id, sheet_name, df):
    sheets_api = service.spreadsheets()
    sheet_id_num = get_sheet_id_by_name(service, sheet_id, sheet_name)
    col_index = {col: idx for idx, col in enumerate(df.columns)}

    requests = []

    # Freeze + bold header
    requests.append({
        "updateSheetProperties": {
            "properties": {"sheetId": sheet_id_num, "gridProperties": {"frozenRowCount": 1}},
            "fields": "gridProperties.frozenRowCount"
        }
    })
    requests.append({
        "repeatCell": {
            "range": {"sheetId": sheet_id_num, "startRowIndex": 0, "endRowIndex": 1},
            "cell": {"userEnteredFormat": {"textFormat": {"bold": True}}},
            "fields": "userEnteredFormat.textFormat.bold"
        }
    })

    # Highlight specific columns
    highlight_columns = ['In-stock Quantity', 'Remaining Quantity']
    for col_name in highlight_columns:
        if col_name in col_index:
            col_idx = col_index[col_name]
            requests.append({
                "repeatCell": {
                    "range": {
                        "sheetId": sheet_id_num,
                        "startRowIndex": 1,
                        "startColumnIndex": col_idx,
                        "endColumnIndex": col_idx + 1
                    },
                    "cell": {
                        "userEnteredFormat": {
                            "textFormat": {"bold": True},
                            "backgroundColor": {"red": 0.8, "green": 0.95, "blue": 1.0}
                        }
                    },
                    "fields": "userEnteredFormat(textFormat, backgroundColor)"
                }
            })

    # Format invoice number as text
    if 'VAT Invoice Number' in col_index:
        col_idx = col_index['VAT Invoice Number']
        requests.append({
            "repeatCell": {
                "range": {
                    "sheetId": sheet_id_num,
                    "startRowIndex": 1,
                    "startColumnIndex": col_idx,
                    "endColumnIndex": col_idx + 1
                },
                "cell": {
                    "userEnteredFormat": {
                        "numberFormat": {"type": "TEXT"}
                    }
                },
                "fields": "userEnteredFormat.numberFormat"
            }
        })

    if requests:
        sheets_api.batchUpdate(spreadsheetId=sheet_id, body={"requests": requests}).execute()
        logger.info("🎨 Sheet formatting applied.")

def get_sheet_id_by_name(service, spreadsheet_id, sheet_name):
    sheets_api = service.spreadsheets()
    metadata = sheets_api.get(spreadsheetId=spreadsheet_id).execute()
    for sheet in metadata.get("sheets", []):
        if sheet["properties"]["title"] == sheet_name:
            return sheet["properties"]["sheetId"]
    raise Exception(f"Sheet name '{sheet_name}' not found.")

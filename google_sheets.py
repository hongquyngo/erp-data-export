import datetime
import logging
import streamlit as st
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
import pytz
import pandas as pd

# ------------------ CONFIG ------------------
SPREADSHEET_ID = "18uvsmtMSYQg1jacLjGF4Bj8GiX-Hjq0Cgi_PPM2Y0U4"
SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]

# ----------------- LOGGER -------------------
logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)


# ---------------- MAIN EXPORT ----------------
def export_to_google_sheets(data, data_type):
    logger.info("📄 Starting export to Google Sheets...")

    credentials = service_account.Credentials.from_service_account_file(
        "credentials.json",
        scopes=SCOPES
    )
    service = build("sheets", "v4", credentials=credentials)
    sheets_api = service.spreadsheets()

    # Tạo tên sheet mới với giờ Việt Nam
    vn_tz = pytz.timezone("Asia/Ho_Chi_Minh")
    now = datetime.datetime.now(vn_tz).strftime("%Y%m%d_%H%M")
    prefix = data_type.lower().replace(" ", "_")
    new_sheet_title = f"{prefix}_{now}"

    try:
        # Tìm sheet có prefix
        logger.info(f"🔍 Checking for existing sheet with prefix: {prefix}")
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
            logger.info(f"♻️ Found existing sheet: {old_sheet_title}. Will clear and rename.")
            requests = [
                {
                    "updateSheetProperties": {
                        "properties": {
                            "sheetId": target_sheet_id,
                            "title": new_sheet_title
                        },
                        "fields": "title"
                    }
                },
                {
                    "updateCells": {
                        "range": {"sheetId": target_sheet_id},
                        "fields": "userEnteredValue"
                    }
                }
            ]
            sheets_api.batchUpdate(spreadsheetId=SPREADSHEET_ID, body={"requests": requests}).execute()
        else:
            logger.info(f"🆕 No existing sheet found. Creating new sheet: {new_sheet_title}")
            sheets_api.batchUpdate(
                spreadsheetId=SPREADSHEET_ID,
                body={"requests": [{"addSheet": {"properties": {"title": new_sheet_title}}}]}
            ).execute()

        # Chuẩn bị dữ liệu đầu ra (giữ định dạng, ép TEXT cho vat_invoice_number)
        values = prepare_values_for_sheet(data)

        # Ghi dữ liệu với USER_ENTERED
        sheets_api.values().update(
            spreadsheetId=SPREADSHEET_ID,
            range=f"{new_sheet_title}!A1",
            valueInputOption="USER_ENTERED",
            body={"values": values}
        ).execute()

        # Format lại sheet
        format_sheet(service, SPREADSHEET_ID, new_sheet_title, data)

        logger.info("✅ Export and formatting completed successfully.")
        return new_sheet_title

    except Exception as e:
        logger.exception(f"❌ Error during export to Google Sheet: {e}")
        raise


# ---------------- SHEET FORMATTING ----------------
def format_sheet(service, sheet_id, sheet_name, df):
    sheets_api = service.spreadsheets()
    sheet_id_num = get_sheet_id_by_name(service, sheet_id, sheet_name)
    col_index = {col: idx for idx, col in enumerate(df.columns)}

    requests = []

    # Freeze header
    requests.append({
        "updateSheetProperties": {
            "properties": {
                "sheetId": sheet_id_num,
                "gridProperties": {
                    "frozenRowCount": 1
                }
            },
            "fields": "gridProperties.frozenRowCount"
        }
    })

    # Bold header
    requests.append({
        "repeatCell": {
            "range": {
                "sheetId": sheet_id_num,
                "startRowIndex": 0,
                "endRowIndex": 1
            },
            "cell": {
                "userEnteredFormat": {
                    "textFormat": {"bold": True}
                }
            },
            "fields": "userEnteredFormat.textFormat.bold"
        }
    })

    # In-stock Quantity: bold + màu xanh
    if 'In-stock Quantity' in col_index:
        col_idx = col_index['in_stock_quantity']
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

    # VAT Invoice Number: format text
    if 'VAT Invoice Number' in col_index:
        col_idx = col_index['vat_invoice_number']
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

    
    # Apply formatting
    if requests:
        try:
            sheets_api.batchUpdate(
                spreadsheetId=sheet_id,
                body={"requests": requests}
            ).execute()
            logger.info("🎨 Sheet formatting applied successfully.")
        except HttpError as e:
            logger.error(f"❌ Google Sheets formatting error: {e}")


# ---------------- UTILITY ----------------
def get_sheet_id_by_name(service, spreadsheet_id, sheet_name):
    sheets_api = service.spreadsheets()
    metadata = sheets_api.get(spreadsheetId=spreadsheet_id).execute()
    for sheet in metadata.get("sheets", []):
        if sheet["properties"]["title"] == sheet_name:
            return sheet["properties"]["sheetId"]
    raise Exception(f"Sheet name '{sheet_name}' not found.")


def prepare_values_for_sheet(df):
    result = [list(df.columns)]

    for _, row in df.iterrows():
        new_row = []
        for col, val in row.items():
            if pd.isna(val):
                new_row.append("")
            elif col == "vat_invoice_number":
                new_row.append(f"'{val}")  # Ép kiểu text bằng prefix '
            else:
                new_row.append(val)
        result.append(new_row)

    return result

import datetime
import logging
import streamlit as st
from google.oauth2 import service_account
from googleapiclient.discovery import build

# Cấu hình logger
logger = logging.getLogger(__name__)

SPREADSHEET_ID = "18uvsmtMSYQg1jacLjGF4Bj8GiX-Hjq0Cgi_PPM2Y0U4"
SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]


def export_to_google_sheets(data, data_type):
    logger.info("📄 Starting export to Google Sheets...")
    credentials = service_account.Credentials.from_service_account_file(
        "credentials.json",  # or use st.secrets if in cloud mode
        scopes=SCOPES
    )
    service = build("sheets", "v4", credentials=credentials)

    sheet = service.spreadsheets()
    import pytz
    # Set timezone (ví dụ: Asia/Ho_Chi_Minh cho Việt Nam)
    vn_tz = pytz.timezone("Asia/Ho_Chi_Minh")
    now = datetime.datetime.now(vn_tz).strftime("%Y%m%d_%H%M")
    new_sheet_title = f"{data_type.lower().replace(' ', '_')}_{now}"

    try:
        # Create new sheet
        sheet.batchUpdate(
            spreadsheetId=SPREADSHEET_ID,
            body={"requests": [{"addSheet": {"properties": {"title": new_sheet_title}}}]}
        ).execute()

        # Write data
        values = [list(data.columns)] + data.astype(str).values.tolist()
        sheet.values().update(
            spreadsheetId=SPREADSHEET_ID,
            range=f"{new_sheet_title}!A1",
            valueInputOption="RAW",
            body={"values": values}
        ).execute()

        # ✅ Apply formatting after writing data
        format_sheet(service, SPREADSHEET_ID, new_sheet_title, data)

        logger.info("✅ Export and formatting completed successfully.")
        return new_sheet_title

    except Exception as e:
        logger.exception(f"❌ Error during export to Google Sheet: {e}")
        raise


def format_sheet(sheet_service, sheet_id, sheet_name, df):
    from googleapiclient.errors import HttpError

    # Xác định vị trí index của các cột cần định dạng
    col_index = {col: idx for idx, col in enumerate(df.columns)}

    requests = []

    # Định dạng: In-stock Quantity -> in đậm + màu xanh
    if 'in_stock_quantity' in col_index:
        col_idx = col_index['in_stock_quantity']
        requests.append({
            "repeatCell": {
                "range": {
                    "sheetId": get_sheet_id_by_name(sheet_service, sheet_id, sheet_name),
                    "startRowIndex": 1,  # Bỏ qua header
                    "startColumnIndex": col_idx,
                    "endColumnIndex": col_idx + 1
                },
                "cell": {
                    "userEnteredFormat": {
                        "textFormat": {
                            "bold": True
                        },
                        "backgroundColor": {
                            "red": 0.8,
                            "green": 0.95,
                            "blue": 1.0
                        }
                    }
                },
                "fields": "userEnteredFormat(textFormat, backgroundColor)"
            }
        })

    # Định dạng: VAT Invoice Number -> giữ định dạng văn bản
    if 'vat_invoice_number' in col_index:
        col_idx = col_index['vat_invoice_number']
        requests.append({
            "repeatCell": {
                "range": {
                    "sheetId": get_sheet_id_by_name(sheet_service, sheet_id, sheet_name),
                    "startRowIndex": 1,
                    "startColumnIndex": col_idx,
                    "endColumnIndex": col_idx + 1
                },
                "cell": {
                    "userEnteredFormat": {
                        "numberFormat": {
                            "type": "TEXT"
                        }
                    }
                },
                "fields": "userEnteredFormat.numberFormat"
            }
        })

    if requests:
        try:
            sheet_service.spreadsheets().batchUpdate(
                spreadsheetId=sheet_id,
                body={"requests": requests}
            ).execute()
        except HttpError as e:
            logger.error(f"❌ Google Sheets formatting error: {e}")



def get_sheet_id_by_name(service, spreadsheet_id, sheet_name):
    metadata = service.spreadsheets().get(spreadsheetId=spreadsheet_id).execute()
    sheets = metadata.get("sheets", "")
    for sheet in sheets:
        if sheet["properties"]["title"] == sheet_name:
            return sheet["properties"]["sheetId"]
    raise Exception(f"Sheet name '{sheet_name}' not found.")

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
        "credentials.json",
        scopes=SCOPES
    )
    service = build("sheets", "v4", credentials=credentials)

    sheet = service.spreadsheets()
    import pytz
    # Set timezone (ví dụ: Asia/Ho_Chi_Minh cho Việt Nam)
    vn_tz = pytz.timezone("Asia/Ho_Chi_Minh")
    now = datetime.datetime.now(vn_tz).strftime("%Y%m%d_%H%M")
    prefix = data_type.lower().replace(" ", "_")
    new_sheet_title = f"{prefix}_{now}"

    try:
        # 1️⃣ Tìm sheet có prefix trùng
        logger.info(f"🔍 Checking for existing sheet with prefix: {prefix}")
        metadata = sheet.get(spreadsheetId=SPREADSHEET_ID).execute()
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
            # Xóa toàn bộ dữ liệu cũ và đổi tên
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
                        "range": {
                            "sheetId": target_sheet_id
                        },
                        "fields": "userEnteredValue"
                    }
                }
            ]
            sheet.batchUpdate(spreadsheetId=SPREADSHEET_ID, body={"requests": requests}).execute()
        else:
            logger.info(f"🆕 No existing sheet found. Creating new sheet: {new_sheet_title}")
            sheet.batchUpdate(
                spreadsheetId=SPREADSHEET_ID,
                body={"requests": [{"addSheet": {"properties": {"title": new_sheet_title}}}]}
            ).execute()

        # 2️⃣ Ghi dữ liệu
        values = [list(data.columns)] + data.astype(str).values.tolist()
        sheet.values().update(
            spreadsheetId=SPREADSHEET_ID,
            range=f"{new_sheet_title}!A1",
            valueInputOption="RAW",
            body={"values": values}
        ).execute()

        # 3️⃣ Format
        format_sheet(sheet, SPREADSHEET_ID, new_sheet_title, data)

        logger.info("✅ Export and formatting completed successfully.")
        return new_sheet_title

    except Exception as e:
        logger.exception(f"❌ Error during export to Google Sheet: {e}")
        raise


def format_sheet(sheet_service, sheet_id, sheet_name, df):
    from googleapiclient.errors import HttpError

    # Lấy sheetId theo tên sheet
    sheet_id_num = get_sheet_id_by_name(sheet_service, sheet_id, sheet_name)
    col_index = {col: idx for idx, col in enumerate(df.columns)}

    requests = []

    # 1️⃣ Freeze hàng tiêu đề đầu tiên
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

    # 2️⃣ Bôi đậm hàng tiêu đề (header)
    requests.append({
        "repeatCell": {
            "range": {
                "sheetId": sheet_id_num,
                "startRowIndex": 0,
                "endRowIndex": 1
            },
            "cell": {
                "userEnteredFormat": {
                    "textFormat": {
                        "bold": True
                    }
                }
            },
            "fields": "userEnteredFormat.textFormat.bold"
        }
    })

    # 3️⃣ In-stock Quantity -> in đậm, màu xanh
    if 'in_stock_quantity' in col_index:
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

    # 4️⃣ VAT Invoice Number -> giữ định dạng text
    if 'vat_invoice_number' in col_index:
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
                        "numberFormat": {
                            "type": "TEXT"
                        }
                    }
                },
                "fields": "userEnteredFormat.numberFormat"
            }
        })

    # 5️⃣ Thực hiện các yêu cầu định dạng
    if requests:
        try:
            sheet_service.spreadsheets().batchUpdate(
                spreadsheetId=sheet_id,
                body={"requests": requests}
            ).execute()
            logger.info("🎨 Sheet formatting applied successfully.")
        except HttpError as e:
            logger.error(f"❌ Google Sheets formatting error: {e}")




def get_sheet_id_by_name(service, spreadsheet_id, sheet_name):
    metadata = service.spreadsheets().get(spreadsheetId=spreadsheet_id).execute()
    sheets = metadata.get("sheets", "")
    for sheet in sheets:
        if sheet["properties"]["title"] == sheet_name:
            return sheet["properties"]["sheetId"]
    raise Exception(f"Sheet name '{sheet_name}' not found.")

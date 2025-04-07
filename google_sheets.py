import datetime
import logging
import streamlit as st
from google.oauth2 import service_account
from googleapiclient.discovery import build

# Cấu hình logger
logger = logging.getLogger(__name__)

SPREADSHEET_ID = "18uvsmtMSYQg1jacLjGF4Bj8GiX-Hjq0Cgi_PPM2Y0U4"
SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]

def export_to_google_sheets(data, data_type: str):
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
    new_sheet_title = f"{data_type.lower().replace(' ', '_')}_{now}"
    logger.info(f"📝 Creating new sheet: {new_sheet_title}")

    try:
        sheet.batchUpdate(
            spreadsheetId=SPREADSHEET_ID,
            body={"requests": [{"addSheet": {"properties": {"title": new_sheet_title}}}]}
        ).execute()

        values = [list(data.columns)] + data.astype(str).values.tolist()
        sheet.values().update(
            spreadsheetId=SPREADSHEET_ID,
            range=f"{new_sheet_title}!A1",
            valueInputOption="RAW",
            body={"values": values}
        ).execute()

        logger.info("✅ Export completed successfully.")
        return new_sheet_title

    except Exception as e:
        logger.exception(f"❌ Error during export to Google Sheet: {e}")
        raise

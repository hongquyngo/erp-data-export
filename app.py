import streamlit as st
import pandas as pd
import pymysql
from sqlalchemy import create_engine
from urllib.parse import quote_plus
from google.oauth2 import service_account
from googleapiclient.discovery import build
import datetime
import logging
import os

# ----------------- LOGGING -----------------
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ----------------- CONFIG ------------------
DB_CONFIG = {
    "host": "erp-all-production.cx1uaj6vj8s5.ap-southeast-1.rds.amazonaws.com",
    "port": 3306,
    "user": "streamlit_user",
    "password": "StrongPass456@#",  # Không encode ở đây!
    "database": "prostechvn"
}

SPREADSHEET_ID = "18uvsmtMSYQg1jacLjGF4Bj8GiX-Hjq0Cgi_PPM2Y0U4"
SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]

# ------------- QUERY DISPATCHER ------------
def run_query(data_type):
    if data_type == "Order Confirmations":
        return "SELECT * FROM order_confirmation_full_view"
    elif data_type == "Inventory":
        return "SELECT * FROM inventory_full_view"
    elif data_type == "Purchase Orders":
        return "SELECT * FROM purchase_order_full_view"
    elif data_type == "Sales Invoices":
        return "SELECT * FROM sales_invoice_full_view"
    elif data_type == "Customer Payments":
        return "SELECT * FROM customer_payment_full_view"
    elif data_type == "Deliveries":
        return "SELECT * FROM delivery_full_view"
    elif data_type == "Product Code Mapping":
        return "SELECT * FROM product_code_mapping_full_view"
    elif data_type == "Inbound Logistic Charges":
        return "SELECT * FROM inbound_logistic_charge_full_view"
    elif data_type == "Outbound Logistic Charges":
        return "SELECT * FROM outbound_logistic_charge_full_view"
    else:
        return ""

# ----------- CONNECT DATABASE --------------
def get_db_connection():
    logger.info("🔌 Connecting to database...")
    user = DB_CONFIG["user"]
    password = quote_plus(DB_CONFIG["password"])  # ✅ Chỉ encode ở đây
    host = DB_CONFIG["host"]
    port = DB_CONFIG["port"]
    database = DB_CONFIG["database"]

    url = f"mysql+pymysql://{user}:{password}@{host}:{port}/{database}"
    logger.info(f"🔐 Using SQLAlchemy URL: mysql+pymysql://{user}:***@{host}:{port}/{database}")
    return create_engine(url)


# ----------- EXPORT TO GOOGLE SHEETS -------
def export_to_google_sheets(data, data_type):
    logger.info("📄 Starting export to Google Sheets...")
    credentials = service_account.Credentials.from_service_account_file(
        "credentials.json",  # <-- Tên file JSON gốc
        scopes=SCOPES
    )
    service = build("sheets", "v4", credentials=credentials)

    sheet = service.spreadsheets()
    now = datetime.datetime.now().strftime("%Y%m%d_%H%M")
    new_sheet_title = f"{data_type.lower().replace(' ', '_')}_{now}"

    logger.info(f"📝 Creating new sheet: {new_sheet_title}")
    try:
        # Create a new sheet
        sheet.batchUpdate(
            spreadsheetId=SPREADSHEET_ID,
            body={
                "requests": [
                    {"addSheet": {"properties": {"title": new_sheet_title}}}
                ]
            }
        ).execute()

        # Write data
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

# ------------------- UI ---------------------
def main():
    st.title("📤 Export ERP Data to Google Sheets")
    data_type = st.selectbox("Select data type to export:", [
        "Order Confirmations",
        "Inventory",
        "Purchase Orders",
        "Sales Invoices",
        "Customer Payments",
        "Deliveries",
        "Product Code Mapping",
        "Inbound Logistic Charges",
        "Outbound Logistic Charges"
    ])

    if st.button("Export to Google Sheets"):
        query = run_query(data_type)
        if not query:
            st.error("❌ Invalid data type selected.")
            return

        try:
            logger.info(f"📥 Running query for: {data_type}")
            engine = get_db_connection()
            df = pd.read_sql(query, engine)
            logger.info(f"📊 Retrieved {len(df)} rows.")
            new_sheet = export_to_google_sheets(df, data_type)
            st.success(f"✅ Exported to sheet: {new_sheet}")
        except Exception as e:
            logger.exception(f"❌ Error in main flow: {e}")
            st.error("❌ Export failed. Check logs for details.")

if __name__ == "__main__":
    main()

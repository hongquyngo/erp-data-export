import streamlit as st
import pandas as pd
import pymysql
from sqlalchemy import create_engine
from urllib.parse import quote_plus
from google.oauth2 import service_account
from googleapiclient.discovery import build
import datetime
import logging

# --------------------------- CONFIGURATION ---------------------------
DB_CONFIG = {
    "host": "erp-all-production.cx1uaj6vj85s.ap-southeast-1.rds.amazonaws.com",
    "port": 3306,
    "user": "python_app",
    "password": "PythonApp123#@!",
    "database": "prostechvn"
}

SPREADSHEET_ID = "18uvsmtMSYQg1jacLjGF4Bj8GiX-Hjq0Cgi_PPM2Y0U4"
SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]

# --------------------------- LOGGING ---------------------------
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# --------------------------- FUNCTIONS ---------------------------
def get_db_connection():
    try:
        logger.info("Encoding password for DB connection")
        password_encoded = quote_plus(DB_CONFIG['password'])
        url = f"mysql+pymysql://{DB_CONFIG['user']}:{password_encoded}@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}"
        logger.info("Creating DB engine")
        return create_engine(url)
    except Exception as e:
        logger.error(f"Error during DB engine creation: {e}")
        raise

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

def get_credentials():
    try:
        logger.info("Loading Google credentials")
        return service_account.Credentials.from_service_account_info(
            st.secrets["gcp_service_account"], scopes=SCOPES
        )
    except Exception as e:
        logger.error(f"Error loading Google credentials: {e}")
        raise

def export_to_gsheet(dataframe):
    try:
        creds = get_credentials()
        service = build("sheets", "v4", credentials=creds)
        sheet = service.spreadsheets()

        logger.info("Clearing old sheets")
        sheet_metadata = sheet.get(spreadsheetId=SPREADSHEET_ID).execute()
        sheets = sheet_metadata.get("sheets", [])
        for s in sheets:
            sheet_id = s["properties"]["sheetId"]
            sheet.batchUpdate(
                spreadsheetId=SPREADSHEET_ID,
                body={"requests": [{"deleteSheet": {"sheetId": sheet_id}}]}
            ).execute()

        timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M")
        new_sheet_title = f"order_confirmation_{timestamp}"

        logger.info(f"Creating new sheet: {new_sheet_title}")
        add_sheet_request = {
            "requests": [
                {"addSheet": {"properties": {"title": new_sheet_title}}}
            ]
        }
        sheet.batchUpdate(spreadsheetId=SPREADSHEET_ID, body=add_sheet_request).execute()

        logger.info("Writing data to sheet")
        values = [dataframe.columns.tolist()] + dataframe.values.tolist()
        sheet.values().update(
            spreadsheetId=SPREADSHEET_ID,
            range=f"{new_sheet_title}!A1",
            valueInputOption="RAW",
            body={"values": values}
        ).execute()

        return new_sheet_title
    except Exception as e:
        logger.error(f"Error during export to Google Sheet: {e}")
        raise

# --------------------------- UI ---------------------------
st.title("Export ERP Data to Google Sheets")
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
    try:
        logger.info("Running query")
        engine = get_db_connection()
        query = run_query(data_type)
        if not query:
            st.error("Invalid data type selected")
            raise ValueError("No query for selected type")

        df = pd.read_sql(query, engine)
        logger.info("Query successful")
        new_sheet = export_to_gsheet(df)
        st.success(f"Exported successfully to sheet: {new_sheet}")
    except Exception as e:
        logger.exception("Unhandled exception occurred")
        st.error(f"❌ Export failed: {e}")

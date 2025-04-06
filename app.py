import streamlit as st
import pandas as pd
from google.oauth2.service_account import Credentials
import gspread
from gspread_dataframe import set_with_dataframe
from sqlalchemy import create_engine
import pymysql
import datetime

# --------------------------- CONFIGURATION ---------------------------
DB_CONFIG = {
    "host": "erp-all-production.cx1uaj6vj8s5.ap-southeast-1.rds.amazonaws.com",
    "port": 3306,
    "user": "python_app",
    "password": "PythonApp123#@!",
    "database": "prostechvn"
}

SPREADSHEET_ID = "18uvsmtMSYQg1jacLjGF4Bj8GiX-Hjq0Cgi_PPM2Y0U4"
SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]

# --------------------------- FUNCTIONS ---------------------------
def get_db_connection():
    url = f"mysql+pymysql://{DB_CONFIG['user']}:{DB_CONFIG['password']}@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}"
    return create_engine(url)

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

def export_to_google_sheets(df, prefix):
    timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M")
    sheet_name = f"{prefix}_{timestamp}"

    creds = Credentials.from_service_account_info(st.secrets["gcp_service_account"], scopes=SCOPES)
    client = gspread.authorize(creds)
    spreadsheet = client.open_by_key(SPREADSHEET_ID)

    # Check if existing sheet with same prefix
    for worksheet in spreadsheet.worksheets():
        if worksheet.title.startswith(prefix):
            spreadsheet.del_worksheet(worksheet)
            break

    worksheet = spreadsheet.add_worksheet(title=sheet_name, rows=str(len(df) + 1), cols=str(len(df.columns)))
    set_with_dataframe(worksheet, df)

    # Highlight In-stock Quantity column if exists
    headers = df.columns.str.lower().tolist()
    if "in-stock quantity" in headers:
        idx = headers.index("in-stock quantity") + 1
        format_range = f"{chr(64 + idx)}2:{chr(64 + idx)}{len(df) + 1}"
        fmt = gspread.formatting.cellFormat(
            textFormat=gspread.formatting.textFormat(bold=True),
            foregroundColor=gspread.formatting.color(0, 0, 1)
        )
        gspread.formatting.format_cell_range(worksheet, format_range, fmt)

    # Format VAT Invoice Number as string if exists
    if "vat invoice number" in headers:
        idx = headers.index("vat invoice number")
        df.iloc[:, idx] = df.iloc[:, idx].apply(lambda x: f"'{x}" if pd.notnull(x) else x)

    return sheet_name

# --------------------------- UI ---------------------------
st.set_page_config(page_title="ERP Data Export", layout="centered")
st.title("📤 Export ERP Data to Google Sheets")

option = st.selectbox("Select data type to export:", [
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
    with st.spinner("Connecting to database and running query..."):
        engine = get_db_connection()
        query = run_query(option)
        if not query:
            st.error("❌ Query not defined for this data type.")
        else:
            df = pd.read_sql(query, engine)
            if df.empty:
                st.warning("⚠️ No data returned from query.")
            else:
                sheet_name = export_to_google_sheets(df, prefix=option.lower().replace(" ", "_"))
                st.success(f"✅ Exported {len(df)} rows to sheet: {sheet_name}")


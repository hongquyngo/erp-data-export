import streamlit as st
import pandas as pd
from google.oauth2.service_account import Credentials
import gspread
from gspread_dataframe import set_with_dataframe
from gspread_formatting import CellFormat, textFormat, format_cell_range, Color
from sqlalchemy import create_engine
import urllib.parse
import datetime

# ---------------- CONFIG ----------------

DB_CONFIG = {
    "host": "erp-all-production.cx1uaj6vj8s5.ap-southeast-1.rds.amazonaws.com",
    "port": 3306,
    "user": "python_app",
    "password": "PythonApp123@!",  # chứa ký tự đặc biệt
    "database": "prostechvn",
}

SPREADSHEET_ID = "18uvsmtMSYQg1jacLjGF4Bj8GiX-Hjq0Cgi_PPM2Y0U4"
SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]

# ---------------- FUNCTION ----------------

def get_db_connection():
    encoded_pw = urllib.parse.quote_plus(DB_CONFIG['password'])
    url = f"mysql+pymysql://{DB_CONFIG['user']}:{encoded_pw}@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}"
    return create_engine(url)

def get_google_sheet_client():
    creds = Credentials.from_service_account_info(st.secrets["gcp_service_account"], scopes=SCOPES)
    client = gspread.authorize(creds)
    return client

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

def export_data_to_sheet(df, sheet_name_prefix):
    client = get_google_sheet_client()
    spreadsheet = client.open_by_key(SPREADSHEET_ID)

    timestamp = datetime.datetime.now(datetime.timezone(datetime.timedelta(hours=7))).strftime("%Y%m%d_%H%M")
    new_sheet_name = f"{sheet_name_prefix}{timestamp}"

    worksheet = spreadsheet.add_worksheet(title=new_sheet_name, rows=str(len(df)+1), cols=str(len(df.columns)))
    set_with_dataframe(worksheet, df)

    headers = df.columns.tolist()

    # Highlight 'in-stock quantity' column
    if "in-stock quantity" in headers:
        idx = headers.index("in-stock quantity") + 1
        col_letter = chr(64 + idx) if idx <= 26 else f"A{chr(64 + idx - 26)}"
        fmt = CellFormat(textFormat=textFormat(bold=True), foregroundColor=Color(0, 0, 1))
        format_cell_range(worksheet, f"{col_letter}2:{col_letter}{df.shape[0] + 1}", fmt)

    # Format 'vat invoice number' as text
    if "vat invoice number" in headers:
        idx = headers.index("vat invoice number") + 1
        vat_range = worksheet.range(2, idx, df.shape[0]+1, idx)
        for cell in vat_range:
            if cell.value and not cell.value.startswith("'"):
                cell.value = f"'{cell.value}"
        worksheet.update_cells(vat_range)

    st.success(f"✅ Data exported to sheet: {new_sheet_name}")

# ---------------- UI ----------------

st.title("Export ERP Data to Google Sheets")

data_options = [
    "Order Confirmations",
    "Inventory",
    "Purchase Orders",
    "Sales Invoices",
    "Customer Payments",
    "Deliveries",
    "Product Code Mapping",
    "Inbound Logistic Charges",
    "Outbound Logistic Charges"
]

data_type = st.selectbox("Select data type to export:", data_options)

if st.button("Export to Google Sheets"):
    try:
        engine = get_db_connection()
        query = run_query(data_type)
        if query == "":
            st.warning("⚠️ No query defined for this data type.")
        else:
            with st.spinner("Querying database..."):
                df = pd.read_sql(query, engine)
                if df.empty:
                    st.warning("⚠️ No data returned.")
                else:
                    prefix = data_type.lower().replace(" ", "_") + "_"
                    export_data_to_sheet(df, prefix)
    except Exception as e:
        st.error(f"❌ Error: {e}")

import streamlit as st
import pandas as pd
import logging
from db import run_query, get_db_engine
from google_sheets import export_to_google_sheets

# Cấu hình logger
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def main():
    st.set_page_config(page_title="ERP Export", page_icon="📤")
    st.title("📤 Export ERP Data to Google Sheets")

    data_type = st.selectbox("Select data type to export:", [
        "Inventory Summary",
        "Inventory Details",
        "Order Confirmations",
        "Purchase Orders",
        "Sales Invoices",
        "Customer Payments",
        "Deliveries",
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
            engine = get_db_engine()
            df = pd.read_sql(query, engine)
            logger.info(f"📊 Retrieved {len(df)} rows.")

            sheet_name = export_to_google_sheets(df, data_type)
            st.success(f"✅ Exported to sheet: {sheet_name}")

        except Exception as e:
            logger.exception("❌ Error in main flow:")
            st.error("❌ Export failed. Check logs for details.")

if __name__ == "__main__":
    main()

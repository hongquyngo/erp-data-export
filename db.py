import pandas as pd
from sqlalchemy import create_engine
from urllib.parse import quote_plus
import logging

# Cấu hình logger
logger = logging.getLogger(__name__)

# Thông tin kết nối cơ sở dữ liệu
DB_CONFIG = {
    "host": "erp-all-production.cx1uaj6vj8s5.ap-southeast-1.rds.amazonaws.com",
    "port": 3306,
    "user": "streamlit_user",
    "password": "StrongPass456@#",
    "database": "prostechvn"
}

def get_db_engine():
    logger.info("🔌 Connecting to database...")
    user = DB_CONFIG["user"]
    password = quote_plus(DB_CONFIG["password"])
    host = DB_CONFIG["host"]
    port = DB_CONFIG["port"]
    database = DB_CONFIG["database"]

    url = f"mysql+pymysql://{user}:{password}@{host}:{port}/{database}"
    logger.info(f"🔐 Using SQLAlchemy URL: mysql+pymysql://{user}:***@{host}:{port}/{database}")
    return create_engine(url)

def run_query(data_type: str) -> str:
    queries = {
        "Order Confirmations": "SELECT * FROM order_confirmation_full_view",
        "Inventory Summary": "SELECT * FROM inventory_full_view",
        "Inventory Details": "SELECT * FROM inventory_detailed_view",
        "Purchase Orders": "SELECT * FROM purchase_order_full_view",
        "Sales Invoices": "SELECT * FROM sales_invoice_full_view",
        "Customer Payments": "SELECT * FROM customer_payment_full_view",
        "Deliveries": "SELECT * FROM delivery_full_view",
        "Inbound Logistic Charges": "SELECT * FROM inbound_logistic_charge_full_view",
        "Outbound Logistic Charges": "SELECT * FROM outbound_logistic_charge_full_view",
        "CAN Details": "SELECT * FROM can_tracking_full_view"
    }
    return queries.get(data_type, "")

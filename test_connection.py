import streamlit as st
from sqlalchemy import create_engine, text
from urllib.parse import quote_plus
import logging

# Thiết lập logger để in ra thông tin
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

st.title("🔌 MySQL Connection Test")

# Thông tin kết nối - thay bằng tài khoản mới bạn đã tạo
user = "streamlit_user"
password = quote_plus("StrongPass456@#")  # đảm bảo bạn encode đúng ký tự đặc biệt
host = "erp-all-production.cx1uaj6vj8s5.ap-southeast-1.rds.amazonaws.com"
port = 3306
database = "prostechvn"

connection_string = f"mysql+pymysql://{user}:{password}@{host}:{port}/{database}"

# Thử kết nối
try:
    st.write("⏳ Đang thử kết nối đến cơ sở dữ liệu...")
    logger.info("Kết nối đến DB...")
    engine = create_engine(connection_string)
    with engine.connect() as conn:
        result = conn.execute(text("SELECT NOW()"))
        row = result.fetchone()
        st.success(f"✅ Kết nối thành công! Server time: {row[0]}")
except Exception as e:
    logger.error(f"Lỗi kết nối: {e}")
    st.error(f"❌ Kết nối thất bại: {e}")

import streamlit as st
import snowflake.connector
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.backends import default_backend

st.title("اتصال به Snowflake با Private Key")

# خواندن اطلاعات از secrets
snowflake_secrets = st.secrets["snowflake"]
user = snowflake_secrets["user"]
account = snowflake_secrets["account"]
private_key_str = snowflake_secrets["private_key"]
warehouse = snowflake_secrets.get("warehouse", "")
database = snowflake_secrets.get("database", "")
schema = snowflake_secrets.get("schema", "")

# اضافه کردن فریمینگ درست PEM به کلید خصوصی
private_key_pem = f"-----BEGIN PRIVATE KEY-----\n{private_key_str}\n-----END PRIVATE KEY-----".encode("utf-8")

# بارگذاری کلید خصوصی
private_key = serialization.load_pem_private_key(
    private_key_pem,
    password=None,  # اگر کلید شما رمز دارد، password=b"رمز_کلید"
    backend=default_backend()
)

# تبدیل کلید به بایت برای Snowflake
private_key_bytes = private_key.private_bytes(
    encoding=serialization.Encoding.DER,
    format=serialization.PrivateFormat.PKCS8,
    encryption_algorithm=serialization.NoEncryption()
)

# اتصال به Snowflake
conn = snowflake.connector.connect(
    user=user,
    account=account,
    private_key=private_key_bytes,
    warehouse=warehouse,
    database=database,
    schema=schema
)

st.success("اتصال به Snowflake برقرار شد!")

# اجرای یک کوئری ساده برای تست
try:
    cs = conn.cursor()
    cs.execute("SELECT CURRENT_VERSION()")
    version = cs.fetchone()[0]
    st.write(f"نسخه Snowflake: {version}")
finally:
    cs.close()
    conn.close()

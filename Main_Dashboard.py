import streamlit as st
import pandas as pd
import snowflake.connector
import plotly.express as px
from cryptography.hazmat.primitives import serialization

# --- Page Config ---
st.set_page_config(
    page_title="Axelar Dashboard",
    page_icon="📊",
    layout="wide"
)

st.title("Axelar at the Frontier of Interchain Innovation")
st.info("📊 Data is retrieved securely from Snowflake using Key-Pair Authentication. Users do not need Snowflake credentials.")

# --- Load private key from secrets ---
def get_private_key():
    private_key_str = st.secrets["snowflake"]["private_key"].encode()
    return serialization.load_pem_private_key(
        private_key_str,
        password=None,
    )

# --- Connect to Snowflake and cache data ---
@st.cache_data(ttl=300)  # کش داده‌ها برای 5 دقیقه
def load_data():
    try:
        private_key = get_private_key()
        conn = snowflake.connector.connect(
            user=st.secrets["snowflake"]["user"],
            account=st.secrets["snowflake"]["account"],
            warehouse=st.secrets["snowflake"]["warehouse"],
            database=st.secrets["snowflake"]["database"],
            schema=st.secrets["snowflake"]["schema"],
            role=st.secrets["snowflake"]["role"],
            private_key=private_key,
        )
        cur = conn.cursor()

        # مثال: گرفتن داده از جدول
        cur.execute("SELECT COLUMN1, COLUMN2 FROM MY_TABLE LIMIT 100;")
        data = cur.fetchall()
        df = pd.DataFrame(data, columns=["COLUMN1", "COLUMN2"])

        cur.close()
        conn.close()
        return df

    except Exception as e:
        st.error(f"❌ Snowflake connection failed: {e}")
        return pd.DataFrame()

# --- Load Data ---
df = load_data()

if not df.empty:
    st.subheader("Example Chart")
    fig = px.line(df, x="COLUMN1", y="COLUMN2", title="COLUMN1 vs COLUMN2")
    st.plotly_chart(fig, use_container_width=True)
else:
    st.warning("⚠️ No data to display.")

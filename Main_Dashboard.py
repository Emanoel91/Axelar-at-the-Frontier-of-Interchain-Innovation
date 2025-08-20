import streamlit as st
import pandas as pd
import snowflake.connector
import plotly.express as px

# --- Page Config ---
st.set_page_config(
    page_title="Axelar Dashboard",
    page_icon="📊",
    layout="wide"
)

st.title("Axelar at the Frontier of Interchain Innovation")

st.info("📊 Data is retrieved from Snowflake. Users do not need Snowflake credentials.")

# --- Connect to Snowflake ---
@st.cache_data(ttl=300)  # cache data for 5 minutes
def load_data():
    try:
        # اتصال با حساب تو (SSO)
        conn = snowflake.connector.connect(
            **st.secrets["connections"]["Axelar_dashboards"]
        )
        cur = conn.cursor()
        
        # مثال: گرفتن داده از جدول نمونه
        cur.execute("SELECT COLUMN1, COLUMN2 FROM MY_TABLE LIMIT 100;")
        data = cur.fetchall()
        df = pd.DataFrame(data, columns=["COLUMN1", "COLUMN2"])
        
        cur.close()
        conn.close()
        return df
    except Exception as e:
        st.error(f"Snowflake connection failed: {e}")
        return pd.DataFrame()

# --- Load Data ---
df = load_data()

if not df.empty:
    st.subheader("Example Chart")
    fig = px.line(df, x="COLUMN1", y="COLUMN2", title="COLUMN1 vs COLUMN2")
    st.plotly_chart(fig, use_container_width=True)
else:
    st.warning("No data to display.")

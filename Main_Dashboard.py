import streamlit as st
import pandas as pd
import snowflake.connector
import plotly.express as px
import plotly.graph_objects as go
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.backends import default_backend

# --- Page Config: Tab Title & Icon -----------------------------------------------------------------------------------------------------------------------------------
st.set_page_config(
    page_title="Axelar at the Frontier of Interchain Innovation",
    page_icon="https://axelarscan.io/logos/logo.png",
    layout="wide"
)

# --- Title with Logo ----------------------------------------------------------------------------------------------------------------------------------------------------
st.markdown(
    """
    <div style="display: flex; align-items: center; gap: 15px;">
        <img src="https://img.cryptorank.io/coins/axelar1663924228506.png" alt="axelar Logo" style="width:60px; height:60px;">
        <h1 style="margin: 0;">Axelar at the Frontier of Interchain Innovation</h1>
    </div>
    """,
    unsafe_allow_html=True
)

# --- Builder Info -------------------------------------------------------------------------------------------------------------------------------------------------------
st.markdown(
    """
    <div style="margin-top: 20px; margin-bottom: 20px; font-size: 16px;">
        <div style="display: flex; align-items: center; gap: 10px;">
            <img src="https://pbs.twimg.com/profile_images/1841479747332608000/bindDGZQ_400x400.jpg" style="width:25px; height:25px; border-radius: 50%;">
            <span>Built by: <a href="https://x.com/0xeman_raz" target="_blank">Eman Raz</a></span>
        </div>
    </div>
    """,
    unsafe_allow_html=True
)

# --- Info Box ----------------------------------------------------------------------------------------------------------------------------------------------------------

st.info("📊Charts initially display data for a default time range. Select a custom range to view results for your desired period.")
st.info("⏳On-chain data retrieval may take a few moments. Please wait while the results load.")

# --- Snowflake Connection -----------------------------------------------------------------------------------------------------------------------------------------------
snowflake_secrets = st.secrets["snowflake"]
user = snowflake_secrets["user"]
account = snowflake_secrets["account"]
private_key_str = snowflake_secrets["private_key"]
warehouse = snowflake_secrets.get("warehouse", "")
database = snowflake_secrets.get("database", "")
schema = snowflake_secrets.get("schema", "")

private_key_pem = f"-----BEGIN PRIVATE KEY-----\n{private_key_str}\n-----END PRIVATE KEY-----".encode("utf-8")
private_key = serialization.load_pem_private_key(
    private_key_pem,
    password=None,
    backend=default_backend()
)
private_key_bytes = private_key.private_bytes(
    encoding=serialization.Encoding.DER,
    format=serialization.PrivateFormat.PKCS8,
    encryption_algorithm=serialization.NoEncryption()
)

conn = snowflake.connector.connect(
    user=user,
    account=account,
    private_key=private_key_bytes,
    warehouse=warehouse,
    database=database,
    schema=schema
)

# --- Time Frame & Period Selection ------------------------------------------------------------------------------------------------------------------------------
timeframe = st.selectbox("Select Time Frame", ["week", "month", "day"])
start_date = st.date_input("Start Date", value=pd.to_datetime("2025-01-01"))
end_date = st.date_input("End Date", value=pd.to_datetime("2025-07-31"))

# --- Queries with Filters & Cached Functions -------------------------------------------------------------------------------------------------------------------
# --- Row 1 -----------------------------------------------------------------------------------------------------------------------------------------------------
@st.cache_data
def load_kpi_data(start_date, end_date):
    query = f"""
        SELECT
              COUNT(DISTINCT tx_id) AS "Number of Txns",
              COUNT(DISTINCT tx_from) AS "Number of Users",
              ROUND(COUNT(DISTINCT tx_id)/COUNT(DISTINCT tx_from)) AS "Avg Txn per User"
        FROM axelar.core.fact_transactions
        WHERE tx_succeeded = 'TRUE'
        AND block_timestamp::date >= '{start_date}'
        AND block_timestamp::date <= '{end_date}'
    """
    return pd.read_sql(query, conn)

# --- KPI Row -------------------------------
kpi_df = load_kpi_data(start_date, end_date)

col1, col2, col3 = st.columns(3)
col1.metric("Number of Transactions", f"{kpi_df['Number of Txns'][0]:,} Txns")
col2.metric("Number of Users", f"{kpi_df['Number of Users'][0]:,} Wallets")
col3.metric("Avg Txn per User", f"{kpi_df['Avg Txn per User'][0]:,} Txns")

# --- Row 2 -----------------------------------------------------------------------------------------------------------------------------------------------------
@st.cache_data
def load_txn_status_data(start_date, end_date, timeframe):
    query = f"""
        SELECT 
            DATE_TRUNC('{timeframe}', block_timestamp) AS "Date",
            COUNT(DISTINCT tx_id) AS  "Number of Txns",
            CASE WHEN tx_succeeded = 'TRUE' THEN 'Succeeded' ELSE 'Failed' END AS "Status"
        FROM axelar.core.fact_transactions
        WHERE block_timestamp::date >= '{start_date}'
        AND block_timestamp::date <= '{end_date}'
        GROUP BY 1,3
        ORDER BY 1
    """
    return pd.read_sql(query, conn)

# --- Transactions Over Time -----------------------------------
txn_df = load_txn_status_data(start_date, end_date, timeframe)

# Stacked bar chart
fig1 = px.bar(
    txn_df,
    x="Date",
    y="Number of Txns",
    color="Status",
    title="Successful & Failed Transactions Over Time",
    barmode="stack"
)

# Normalized stacked bar chart
fig2 = px.bar(
    txn_df,
    x="Date",
    y="Number of Txns",
    color="Status",
    title="% of Successful & Failed Transactions Over Time",
    barmode="relative"
)
fig2.update_layout(yaxis=dict(title="Percentage", tickformat=".0%"))

col1, col2 = st.columns(2)
col1.plotly_chart(fig1, use_container_width=True)
col2.plotly_chart(fig2, use_container_width=True)

# --- Row 3 -------------------------------------------------------------------------------------------------------------------------------------------------
@st.cache_data
def load_users_data(start_date, end_date, timeframe):
    query = f"""
        SELECT
              DATE_TRUNC('{timeframe}', block_timestamp) as "Date",
              COUNT(DISTINCT tx_from) AS "Number of Users",
              ROUND(COUNT(DISTINCT tx_id)/COUNT(DISTINCT tx_from)) AS "Avg Txn per User"
        FROM axelar.core.fact_transactions
        WHERE tx_succeeded = 'TRUE'
        AND block_timestamp::date >= '{start_date}'
        AND block_timestamp::date <= '{end_date}'
        GROUP BY 1
        ORDER BY 1
    """
    return pd.read_sql(query, conn)

@st.cache_data
def load_status_pie_data(start_date, end_date):
    query = f"""
        SELECT 
        CASE WHEN tx_succeeded = 'TRUE' THEN 'Succeeded' ELSE 'Failed' END AS "Status",
        COUNT(DISTINCT tx_id) AS  "Number of Txns"
        FROM axelar.core.fact_transactions
        WHERE block_timestamp::date >= '{start_date}'
        AND block_timestamp::date <= '{end_date}'
        GROUP BY 1
    """
    return pd.read_sql(query, conn)

# --- Users & Avg Txn ----------------------------------------
users_df = load_users_data(start_date, end_date, timeframe)

fig3 = go.Figure()

fig3.add_trace(go.Bar(
    x=users_df["Date"],
    y=users_df["Number of Users"],
    name="Number of Users",
    yaxis="y1"
))

fig3.add_trace(go.Scatter(
    x=users_df["Date"],
    y=users_df["Avg Txn per User"],
    name="Avg Txn per User",
    mode="lines+markers",
    yaxis="y2"
))

fig3.update_layout(
    title="Axelar Users Over Time",
    yaxis=dict(title="Number of Users", side="left"),
    yaxis2=dict(title="Avg Txn per User", overlaying="y", side="right")
)

# Pie chart
status_pie_df = load_status_pie_data(start_date, end_date)

fig4 = px.pie(
    status_pie_df,
    names="Status",
    values="Number of Txns",
    title="Total Number of Transactions by Status",
    hole=0.4
)

col1, col2 = st.columns(2)
col1.plotly_chart(fig3, use_container_width=True)
col2.plotly_chart(fig4, use_container_width=True)


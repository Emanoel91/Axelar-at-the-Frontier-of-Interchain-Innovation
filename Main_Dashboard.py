import streamlit as st
import pandas as pd
import snowflake.connector
import plotly.express as px
import plotly.graph_objects as go

# --- Page Config: Tab Title & Icon -------------------------------------------------------------------------------------
st.set_page_config(
    page_title="Axelar at the Frontier of Interchain Innovation",
    page_icon="https://pbs.twimg.com/profile_images/1869486848646537216/rs71wCQo_400x400.jpg",
    layout="wide"
)

# --- Title with Logo ---------------------------------------------------------------------------------------------------
st.markdown(
    """
    <div style="display: flex; align-items: center; gap: 15px;">
        <img src="https://pbs.twimg.com/profile_images/1869486848646537216/rs71wCQo_400x400.jpg" alt="Axelar Logo" style="width:60px; height:60px;">
        <h1 style="margin: 0;">Axelar at the Frontier of Interchain Innovation</h1>
    </div>
    """,
    unsafe_allow_html=True
)

# --- Builder Info ---------------------------------------------------------------------------------------------------------
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

st.info("📊Charts initially display data for a default time range. Select a custom range to view results for your desired period.")

st.info("⏳On-chain data retrieval may take a few moments. Please wait while the results load.")

# --- Snowflake Connection --------------------------------------------------------------------------------------------------
conn = snowflake.connector.connect(
    user=st.secrets["snowflake"]["user"],
    password=st.secrets["snowflake"]["password"],
    account=st.secrets["snowflake"]["account"],
    warehouse="SNOWFLAKE_LEARNING_WH",
    database="AXELAR",
    schema="PUBLIC"
)

# --- Time Frame & Period Selection ---
timeframe = st.selectbox("Select Time Frame", ["week", "month", "day"])
start_date = st.date_input("Start Date", value=pd.to_datetime("2025-01-01"))
end_date = st.date_input("End Date", value=pd.to_datetime("2025-07-31"))

# --------------------------------------------------------------------------------------------------------------------------------
st.markdown(
    """
    <div style="background-color:#e6fa36; padding:1px; border-radius:10px;">
        <h2 style="color:#000000; text-align:center;">Overall Stats of Chains</h2>
    </div>
    """,
    unsafe_allow_html=True
)
# --- Row 1 -----------------------------------------------------------------------------------------------------------------------
@st.cache_data(ttl=86400) 
def load_kpi_data(start_date, end_date):
    query = f"""
    SELECT
        COUNT(DISTINCT tx_id) AS "Number of Successful Txns",
        COUNT(DISTINCT tx_from) AS "Number of Users",
        ROUND(COUNT(DISTINCT tx_id)::NUMERIC / NULLIF(COUNT(DISTINCT tx_from),0)) AS "Avg Txn per User"
    FROM axelar.core.fact_transactions
    WHERE tx_succeeded = 'TRUE'
      AND block_timestamp::date >= '{start_date}'
      AND block_timestamp::date <= '{end_date}'
    """
    return pd.read_sql(query, conn)

# --- Load Data ---
df = load_kpi_data(start_date, end_date)

# --- Display KPIs in one row ---
col1, col2, col3 = st.columns(3)

col1.metric(
    label="Number of Successful Txns",
    value=f"{df['Number of Successful Txns'][0]:,}"
)
col2.metric(
    label="Number of Users",
    value=f"{df['Number of Users'][0]:,}"
)
col3.metric(
    label="Avg Txn per User",
    value=df['Avg Txn per User'][0]
)

# --- Row 2 --------------------------------------------------------------------------------------------------------------------------------
# --- Cached Function for Chart Data ---
@st.cache_data(ttl=600)
def load_txn_status_data(start_date, end_date, timeframe):
    query = f"""
    SELECT 
        date_trunc('{timeframe}', block_timestamp) AS "Date",
        COUNT(DISTINCT tx_id) AS "Number of Txns",
        CASE WHEN tx_succeeded = 'TRUE' THEN 'Succeeded' ELSE 'Failed' END AS "Status"
    FROM axelar.core.fact_transactions
    WHERE block_timestamp::date >= '{start_date}'
      AND block_timestamp::date <= '{end_date}'
    GROUP BY 1, 3
    ORDER BY 1
    """
    return pd.read_sql(query, conn)

# --- Load Data ---
df_chart = load_txn_status_data(start_date, end_date, timeframe)

# --- Create Plots ---
fig_bar = px.bar(
    df_chart,
    x="Date",
    y="Number of Txns",
    color="Status",
    title="Successful and Failed Transaction Over Time",
    barmode="stack"
)

fig_area = px.area(
    df_chart,
    x="Date",
    y="Number of Txns",
    color="Status",
    groupnorm="fraction",  # Normalized stacked area
    title="% of Successful and Failed Transactions Over Time"
)

# --- Display Side-by-Side Charts ---
col1, col2 = st.columns(2)
col1.plotly_chart(fig_bar, use_container_width=True)
col2.plotly_chart(fig_area, use_container_width=True)

# --- Row 3 ------------------------------------------------------------------------------------------------------------------------------
# --- Cached Function for Donut Chart Data ---
@st.cache_data(ttl=86400)
def load_txn_status_totals(start_date, end_date):
    query = f"""
    SELECT 
        CASE WHEN tx_succeeded = 'TRUE' THEN 'Succeeded' ELSE 'Failed' END AS "Status",
        COUNT(DISTINCT tx_id) AS "Number of Txns"
    FROM axelar.core.fact_transactions
    WHERE block_timestamp::date >= '{start_date}'
      AND block_timestamp::date <= '{end_date}'
    GROUP BY 1
    """
    return pd.read_sql(query, conn)

# --- Cached Function for Bar-Line Chart Data ---
@st.cache_data(ttl=86400)
def load_users_over_time(start_date, end_date, timeframe):
    query = f"""
    SELECT
        date_trunc('{timeframe}', block_timestamp) AS "Date",
        COUNT(DISTINCT tx_from) AS "Number of Users",
        ROUND(COUNT(DISTINCT tx_id)::NUMERIC / NULLIF(COUNT(DISTINCT tx_from),0), 2) AS "Avg Txn per User"
    FROM axelar.core.fact_transactions
    WHERE tx_succeeded = 'TRUE'
      AND block_timestamp::date >= '{start_date}'
      AND block_timestamp::date <= '{end_date}'
    GROUP BY 1
    ORDER BY 1
    """
    return pd.read_sql(query, conn)

# --- Load Data ---
df_donut = load_txn_status_totals(start_date, end_date)
df_users = load_users_over_time(start_date, end_date, timeframe)

# --- Donut Chart ---
fig_donut = px.pie(
    df_donut,
    names="Status",
    values="Number of Txns",
    hole=0.5,
    title="Total Number of Transactions by Status"
)

# --- Bar-Line Chart ---
import plotly.graph_objects as go

fig_barline = go.Figure()

# Bar for Number of Users (left y-axis)
fig_barline.add_trace(go.Bar(
    x=df_users["Date"],
    y=df_users["Number of Users"],
    name="Number of Users",
    yaxis="y1"
))

# Line for Avg Txn per User (right y-axis)
fig_barline.add_trace(go.Scatter(
    x=df_users["Date"],
    y=df_users["Avg Txn per User"],
    name="Avg Txn per User",
    yaxis="y2",
    mode="lines+markers"
))

# Layout
fig_barline.update_layout(
    title="Axelar Users Over Time",
    xaxis=dict(title="Date"),
    yaxis=dict(
        title="Number of Users",
        side="left"
    ),
    yaxis2=dict(
        title="Avg Txn per User",
        overlaying="y",
        side="right"
    ),
    legend=dict(x=0.5, y=1.1, orientation="h")
)

# --- Display Side-by-Side ---
col1, col2 = st.columns(2)
col1.plotly_chart(fig_donut, use_container_width=True)
col2.plotly_chart(fig_barline, use_container_width=True)


import streamlit as st
import pandas as pd
import snowflake.connector
import plotly.express as px
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import serialization

# --- Page Config ------------------------------------------------------------------------------------------------------
st.set_page_config(
    page_title="Axelar's Squid Bridge",
    page_icon="https://pbs.twimg.com/profile_images/1938625911743524864/ppNPPF84_400x400.jpg",
    layout="wide"
)

# --- Title with Logo -----------------------------------------------------------------------------------------------------
st.title("📜Overall Stats")

st.info("📊Charts initially display data for a default time range. Select a custom range to view results for your desired period.")
st.info("⏳On-chain data retrieval may take a few moments. Please wait while the results load.")

# --- Sidebar Footer ---------------------------------------------------------------------------------------------------------
st.sidebar.markdown(
    """
    <style>
    .sidebar-footer {
        position: fixed;
        bottom: 20px;
        width: 250px;
        font-size: 13px;
        color: gray;
        margin-left: 5px; 
        text-align: left;  
    }
    .sidebar-footer img {
        width: 16px;
        height: 16px;
        vertical-align: middle;
        border-radius: 50%;
        margin-right: 5px;
    }
    .sidebar-footer a {
        color: gray;
        text-decoration: none;
    }
    </style>

    <div class="sidebar-footer">
        <div>
            <a href="https://x.com/axelar" target="_blank">
                <img src="https://img.cryptorank.io/coins/axelar1663924228506.png" alt="Axelar Logo">
                Powered by Axelar
            </a>
        </div>
        <div style="margin-top: 5px;">
            <a href="https://x.com/0xeman_raz" target="_blank">
                <img src="https://pbs.twimg.com/profile_images/1841479747332608000/bindDGZQ_400x400.jpg" alt="Eman Raz">
                Built by Eman Raz
            </a>
        </div>
    </div>
    """,
    unsafe_allow_html=True
)

# --- Snowflake Connection with Key Pair -------------------------------------------------------------------------
private_key_str = st.secrets["snowflake"]["private_key"]
private_key_bytes = private_key_str.encode("utf-8")

# Fix for newlines in PEM key
private_key_bytes = private_key_bytes.replace(b'\\n', b'\n')

private_key = serialization.load_pem_private_key(
    private_key_bytes,
    password=None,
    backend=default_backend()
)

conn = snowflake.connector.connect(
    user=st.secrets["snowflake"]["user"],
    account=st.secrets["snowflake"]["account"],
    private_key=private_key,
    warehouse=st.secrets["snowflake"]["warehouse"],
    database=st.secrets["snowflake"]["database"],
    schema=st.secrets["snowflake"]["schema"],
    role=st.secrets["snowflake"]["role"]
)

# --- Date Inputs ---------------------------------------------------------------------------------------------------
timeframe = st.selectbox("Select Time Frame", ["month", "week", "day"])
start_date = st.date_input("Start Date", value=pd.to_datetime("2023-01-01"))
end_date = st.date_input("End Date", value=pd.to_datetime("2025-07-31"))

# --- Query Function --------------------------------------------------------------------------------------
@st.cache_data(show_spinner=True)
def load_kpi_data(timeframe, start_date, end_date):
    start_str = start_date.strftime("%Y-%m-%d")
    end_str = end_date.strftime("%Y-%m-%d")

    query = f"""
    WITH axelar_service AS (
        SELECT 
            created_at, 
            recipient_address AS user, 
            TRY_TO_DOUBLE(data:send:amount::STRING) AS amount,
            TRY_TO_DOUBLE(data:link:price::STRING) * TRY_TO_DOUBLE(data:send:amount::STRING) AS amount_usd,
            id
        FROM axelar.axelscan.fact_transfers
        WHERE status = 'executed' AND simplified_status = 'received'
    )
    SELECT 
        COUNT(DISTINCT id) AS number_of_transfers,
        COUNT(DISTINCT user) AS number_of_users,
        ROUND(SUM(amount_usd), 2) AS volume_of_transfers,
        ROUND(AVG(amount_usd), 2) AS avg_bridges_volume
    FROM axelar_service
    WHERE created_at::date >= '{start_str}' AND created_at::date <= '{end_str}'
    """

    df = pd.read_sql(query, conn)
    return df

# --- Load Data ----------------------------------------------------------------------------------------------------
df_kpi = load_kpi_data(timeframe, start_date, end_date)

# --- KPI Row ------------------------------------------------------------------------------------------------------
col1, col2, col3, col4 = st.columns(4)

def format_value(value, unit):
    if value is None:
        value = 0
    if unit == 'B':
        return f"${value / 1_000_000_000:.2f}B"
    elif unit == 'M':
        return f"{value / 1_000_000:.2f}M Txns"
    elif unit == 'K':
        return f"{value / 1_000:.2f}K"
    return str(value)

col1.metric(
    label="Bridged Volume",
    value=format_value(df_kpi['VOLUME_OF_TRANSFERS'][0], 'B')
)

col2.metric(
    label="Bridges",
    value=format_value(df_kpi['NUMBER_OF_TRANSFERS'][0], 'M')
)

col3.metric(
    label="Bridgors",
    value=f"{df_kpi['NUMBER_OF_USERS'][0] / 1_000:.2f}K Addresses"
)

col4.metric(
    label="Avg Bridge Volume",
    value=f"${df_kpi['AVG_BRIDGES_VOLUME'][0] / 1_000:.2f}K"
)

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
st.markdown(
    """
    <div style="background-color:#0ed145; padding:1px; border-radius:10px;">
        <h2 style="color:#000000; text-align:center;">Transactions and Users on Axelar</h2>
    </div>
    """,
    unsafe_allow_html=True
)
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

# --- Normalized stacked bar chart (100% per Date) -------------------------------
totals_by_date = txn_df.groupby("Date")["Number of Txns"].transform("sum")
txn_df_pct = txn_df.copy()
txn_df_pct["Share"] = (txn_df_pct["Number of Txns"] / totals_by_date).fillna(0)

fig2 = px.bar(
    txn_df_pct,
    x="Date",
    y="Share",
    color="Status",
    title="% of Successful & Failed Transactions Over Time",
    barmode="stack" 
)

fig2.update_layout(
    yaxis=dict(title="Percentage", tickformat=".0%", range=[0, 1])
)

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

st.markdown(
    """
    <div style="background-color:#0ed145; padding:1px; border-radius:10px;">
        <h2 style="color:#000000; text-align:center;">Squid Router Bridge</h2>
    </div>
    """,
    unsafe_allow_html=True
)
# --- Row 4 -------------------------------------------------------------------------------------------------------------------------------------------------------
@st.cache_data
def load_kpi_data(timeframe, start_date, end_date):
    
    start_str = start_date.strftime("%Y-%m-%d")
    end_str = end_date.strftime("%Y-%m-%d")

    query = f"""
    WITH axelar_service AS (
        -- Token Transfers
        SELECT 
            created_at, 
            LOWER(data:send:original_source_chain) AS source_chain, 
            LOWER(data:send:original_destination_chain) AS destination_chain,
            recipient_address AS user, 
            CASE 
              WHEN IS_ARRAY(data:send:amount) THEN NULL
              WHEN IS_OBJECT(data:send:amount) THEN NULL
              WHEN TRY_TO_DOUBLE(data:send:amount::STRING) IS NOT NULL THEN TRY_TO_DOUBLE(data:send:amount::STRING)
              ELSE NULL
            END AS amount,
            CASE 
              WHEN IS_ARRAY(data:send:amount) OR IS_ARRAY(data:link:price) THEN NULL
              WHEN IS_OBJECT(data:send:amount) OR IS_OBJECT(data:link:price) THEN NULL
              WHEN TRY_TO_DOUBLE(data:send:amount::STRING) IS NOT NULL AND TRY_TO_DOUBLE(data:link:price::STRING) IS NOT NULL 
                THEN TRY_TO_DOUBLE(data:send:amount::STRING) * TRY_TO_DOUBLE(data:link:price::STRING)
              ELSE NULL
            END AS amount_usd,
            CASE 
              WHEN IS_ARRAY(data:send:fee_value) THEN NULL
              WHEN IS_OBJECT(data:send:fee_value) THEN NULL
              WHEN TRY_TO_DOUBLE(data:send:fee_value::STRING) IS NOT NULL THEN TRY_TO_DOUBLE(data:send:fee_value::STRING)
              ELSE NULL
            END AS fee,
            id, 
            'Token Transfers' AS Service, 
            data:link:asset::STRING AS raw_asset
        FROM axelar.axelscan.fact_transfers
        WHERE status = 'executed'
          AND simplified_status = 'received'
          AND (
            sender_address ilike '%0xce16F69375520ab01377ce7B88f5BA8C48F8D666%' 
            OR sender_address ilike '%0x492751eC3c57141deb205eC2da8bFcb410738630%'
            OR sender_address ilike '%0xDC3D8e1Abe590BCa428a8a2FC4CfDbD1AcF57Bd9%'
            OR sender_address ilike '%0xdf4fFDa22270c12d0b5b3788F1669D709476111E%'
            OR sender_address ilike '%0xe6B3949F9bBF168f4E3EFc82bc8FD849868CC6d8%'
          )

        UNION ALL

        -- GMP
        SELECT  
            created_at,
            data:call.chain::STRING AS source_chain,
            data:call.returnValues.destinationChain::STRING AS destination_chain,
            data:call.transaction.from::STRING AS user,
            CASE 
              WHEN IS_ARRAY(data:amount) OR IS_OBJECT(data:amount) THEN NULL
              WHEN TRY_TO_DOUBLE(data:amount::STRING) IS NOT NULL THEN TRY_TO_DOUBLE(data:amount::STRING)
              ELSE NULL
            END AS amount,
            CASE 
              WHEN IS_ARRAY(data:value) OR IS_OBJECT(data:value) THEN NULL
              WHEN TRY_TO_DOUBLE(data:value::STRING) IS NOT NULL THEN TRY_TO_DOUBLE(data:value::STRING)
              ELSE NULL
            END AS amount_usd,
            COALESCE(
              CASE 
                WHEN IS_ARRAY(data:gas:gas_used_amount) OR IS_OBJECT(data:gas:gas_used_amount) 
                  OR IS_ARRAY(data:gas_price_rate:source_token.token_price.usd) OR IS_OBJECT(data:gas_price_rate:source_token.token_price.usd) 
                THEN NULL
                WHEN TRY_TO_DOUBLE(data:gas:gas_used_amount::STRING) IS NOT NULL 
                  AND TRY_TO_DOUBLE(data:gas_price_rate:source_token.token_price.usd::STRING) IS NOT NULL 
                THEN TRY_TO_DOUBLE(data:gas:gas_used_amount::STRING) * TRY_TO_DOUBLE(data:gas_price_rate:source_token.token_price.usd::STRING)
                ELSE NULL
              END,
              CASE 
                WHEN IS_ARRAY(data:fees:express_fee_usd) OR IS_OBJECT(data:fees:express_fee_usd) THEN NULL
                WHEN TRY_TO_DOUBLE(data:fees:express_fee_usd::STRING) IS NOT NULL THEN TRY_TO_DOUBLE(data:fees:express_fee_usd::STRING)
                ELSE NULL
              END
            ) AS fee,
            id, 
            'GMP' AS Service, 
            data:symbol::STRING AS raw_asset
        FROM axelar.axelscan.fact_gmp 
        WHERE status = 'executed'
          AND simplified_status = 'received'
          AND (
            data:approved:returnValues:contractAddress ilike '%0xce16F69375520ab01377ce7B88f5BA8C48F8D666%' 
            OR data:approved:returnValues:contractAddress ilike '%0x492751eC3c57141deb205eC2da8bFcb410738630%'
            OR data:approved:returnValues:contractAddress ilike '%0xDC3D8e1Abe590BCa428a8a2FC4CfDbD1AcF57Bd9%'
            OR data:approved:returnValues:contractAddress ilike '%0xdf4fFDa22270c12d0b5b3788F1669D709476111E%'
            OR data:approved:returnValues:contractAddress ilike '%0xe6B3949F9bBF168f4E3EFc82bc8FD849868CC6d8%'
          )
    )
    SELECT 
        COUNT(DISTINCT id) AS Number_of_Transfers, 
        COUNT(DISTINCT user) AS Number_of_Users, 
        ROUND(SUM(amount_usd)) AS Volume_of_Transfers
    FROM axelar_service
    WHERE created_at::date >= '{start_str}' 
      AND created_at::date <= '{end_str}'
    """

    df = pd.read_sql(query, conn)
    return df

# --- Load Data ----------------------------------------------------------------------------------------------------
df_kpi = load_kpi_data(timeframe, start_date, end_date)

# --- KPI Row ------------------------------------------------------------------------------------------------------
col1, col2, col3 = st.columns(3)

col1.metric(
    label="Volume of Transfers",
    value=f"${df_kpi['VOLUME_OF_TRANSFERS'][0]:,}"
)

col2.metric(
    label="Number of Transfers",
    value=f"{df_kpi['NUMBER_OF_TRANSFERS'][0]:,} Txns"
)

col3.metric(
    label="Number of Users",
    value=f"{df_kpi['NUMBER_OF_USERS'][0]:,} Addresses"
)

# --- Row 5 ----------------------------------------------------------------------------------------------------------------------------------------------------------------
@st.cache_data
def load_time_series_data(timeframe, start_date, end_date):
    start_str = start_date.strftime("%Y-%m-%d")
    end_str = end_date.strftime("%Y-%m-%d")

    query = f"""
    WITH axelar_service AS (
        -- Token Transfers
        SELECT 
            created_at, 
            LOWER(data:send:original_source_chain) AS source_chain, 
            LOWER(data:send:original_destination_chain) AS destination_chain,
            recipient_address AS user, 
            CASE 
              WHEN IS_ARRAY(data:send:amount) THEN NULL
              WHEN IS_OBJECT(data:send:amount) THEN NULL
              WHEN TRY_TO_DOUBLE(data:send:amount::STRING) IS NOT NULL THEN TRY_TO_DOUBLE(data:send:amount::STRING)
              ELSE NULL
            END AS amount,
            CASE 
              WHEN IS_ARRAY(data:send:amount) OR IS_ARRAY(data:link:price) THEN NULL
              WHEN IS_OBJECT(data:send:amount) OR IS_OBJECT(data:link:price) THEN NULL
              WHEN TRY_TO_DOUBLE(data:send:amount::STRING) IS NOT NULL AND TRY_TO_DOUBLE(data:link:price::STRING) IS NOT NULL 
                THEN TRY_TO_DOUBLE(data:send:amount::STRING) * TRY_TO_DOUBLE(data:link:price::STRING)
              ELSE NULL
            END AS amount_usd,
            CASE 
              WHEN IS_ARRAY(data:send:fee_value) THEN NULL
              WHEN IS_OBJECT(data:send:fee_value) THEN NULL
              WHEN TRY_TO_DOUBLE(data:send:fee_value::STRING) IS NOT NULL THEN TRY_TO_DOUBLE(data:send:fee_value::STRING)
              ELSE NULL
            END AS fee,
            id, 
            'Token Transfers' AS Service, 
            data:link:asset::STRING AS raw_asset
        FROM axelar.axelscan.fact_transfers
        WHERE status = 'executed'
          AND simplified_status = 'received'
          AND (
            sender_address ilike '%0xce16F69375520ab01377ce7B88f5BA8C48F8D666%' 
            OR sender_address ilike '%0x492751eC3c57141deb205eC2da8bFcb410738630%'
            OR sender_address ilike '%0xDC3D8e1Abe590BCa428a8a2FC4CfDbD1AcF57Bd9%'
            OR sender_address ilike '%0xdf4fFDa22270c12d0b5b3788F1669D709476111E%'
            OR sender_address ilike '%0xe6B3949F9bBF168f4E3EFc82bc8FD849868CC6d8%'
          )

        UNION ALL

        -- GMP
        SELECT  
            created_at,
            data:call.chain::STRING AS source_chain,
            data:call.returnValues.destinationChain::STRING AS destination_chain,
            data:call.transaction.from::STRING AS user,
            CASE 
              WHEN IS_ARRAY(data:amount) OR IS_OBJECT(data:amount) THEN NULL
              WHEN TRY_TO_DOUBLE(data:amount::STRING) IS NOT NULL THEN TRY_TO_DOUBLE(data:amount::STRING)
              ELSE NULL
            END AS amount,
            CASE 
              WHEN IS_ARRAY(data:value) OR IS_OBJECT(data:value) THEN NULL
              WHEN TRY_TO_DOUBLE(data:value::STRING) IS NOT NULL THEN TRY_TO_DOUBLE(data:value::STRING)
              ELSE NULL
            END AS amount_usd,
            COALESCE(
              CASE 
                WHEN IS_ARRAY(data:gas:gas_used_amount) OR IS_OBJECT(data:gas:gas_used_amount) 
                  OR IS_ARRAY(data:gas_price_rate:source_token.token_price.usd) OR IS_OBJECT(data:gas_price_rate:source_token.token_price.usd) 
                THEN NULL
                WHEN TRY_TO_DOUBLE(data:gas:gas_used_amount::STRING) IS NOT NULL 
                  AND TRY_TO_DOUBLE(data:gas_price_rate:source_token.token_price.usd::STRING) IS NOT NULL 
                THEN TRY_TO_DOUBLE(data:gas:gas_used_amount::STRING) * TRY_TO_DOUBLE(data:gas_price_rate:source_token.token_price.usd::STRING)
                ELSE NULL
              END,
              CASE 
                WHEN IS_ARRAY(data:fees:express_fee_usd) OR IS_OBJECT(data:fees:express_fee_usd) THEN NULL
                WHEN TRY_TO_DOUBLE(data:fees:express_fee_usd::STRING) IS NOT NULL THEN TRY_TO_DOUBLE(data:fees:express_fee_usd::STRING)
                ELSE NULL
              END
            ) AS fee,
            id, 
            'GMP' AS Service, 
            data:symbol::STRING AS raw_asset
        FROM axelar.axelscan.fact_gmp 
        WHERE status = 'executed'
          AND simplified_status = 'received'
          AND (
            data:approved:returnValues:contractAddress ilike '%0xce16F69375520ab01377ce7B88f5BA8C48F8D666%' 
            OR data:approved:returnValues:contractAddress ilike '%0x492751eC3c57141deb205eC2da8bFcb410738630%'
            OR data:approved:returnValues:contractAddress ilike '%0xDC3D8e1Abe590BCa428a8a2FC4CfDbD1AcF57Bd9%'
            OR data:approved:returnValues:contractAddress ilike '%0xdf4fFDa22270c12d0b5b3788F1669D709476111E%'
            OR data:approved:returnValues:contractAddress ilike '%0xe6B3949F9bBF168f4E3EFc82bc8FD849868CC6d8%'
          )
    )
    SELECT 
        DATE_TRUNC('{timeframe}', created_at) AS Date,
        COUNT(DISTINCT id) AS Number_of_Transfers, 
        COUNT(DISTINCT user) AS Number_of_Users, 
        ROUND(SUM(amount_usd)) AS Volume_of_Transfers
    FROM axelar_service
    WHERE created_at::date >= '{start_str}' 
      AND created_at::date <= '{end_str}'
    GROUP BY 1
    ORDER BY 1
    """

    return pd.read_sql(query, conn)

# --- Load Data ----------------------------------------------------------------------------------------------------
df_ts = load_time_series_data(timeframe, start_date, end_date)

# --- Charts in One Row ---------------------------------------------------------------------------------------------
col1, col2, col3 = st.columns(3)

with col1:
    fig1 = px.bar(
        df_ts,
        x="DATE",
        y="VOLUME_OF_TRANSFERS",
        title="Squid Bridge Volume Over Time (USD)",
        labels={"VOLUME_OF_TRANSFERS": "Volume (USD)", "DATE": "Date"},
        color_discrete_sequence=["#535dfa"]
    )
    fig1.update_layout(xaxis_title="", yaxis_title="USD", bargap=0.2)
    st.plotly_chart(fig1, use_container_width=True)

with col2:
    fig2 = px.bar(
        df_ts,
        x="DATE",
        y="NUMBER_OF_TRANSFERS",
        title="Squid Bridge Transactions Over Time",
        labels={"NUMBER_OF_TRANSFERS": "Transactions", "DATE": "Date"},
        color_discrete_sequence=["#535dfa"]
    )
    fig2.update_layout(xaxis_title="", yaxis_title="Txns", bargap=0.2)
    st.plotly_chart(fig2, use_container_width=True)

with col3:
    fig3 = px.bar(
        df_ts,
        x="DATE",
        y="NUMBER_OF_USERS",
        title="Squid Bridge Users Over Time",
        labels={"NUMBER_OF_USERS": "Users", "DATE": "Date"},
        color_discrete_sequence=["#535dfa"]
    )
    fig3.update_layout(xaxis_title="", yaxis_title="Addresses", bargap=0.2)
    st.plotly_chart(fig3, use_container_width=True)


# --- Row 6: Source-Destination Overview ------------------------------------------------------------------------------------------------

@st.cache_data
def load_source_dest_data(start_date, end_date):
    query = f"""
        WITH overview AS (
            WITH axelar_service AS (
                SELECT 
                    created_at, 
                    LOWER(data:send:original_source_chain) AS source_chain, 
                    LOWER(data:send:original_destination_chain) AS destination_chain,
                    recipient_address AS user, 
                    CASE 
                      WHEN IS_ARRAY(data:send:amount) THEN NULL
                      WHEN IS_OBJECT(data:send:amount) THEN NULL
                      WHEN TRY_TO_DOUBLE(data:send:amount::STRING) IS NOT NULL THEN TRY_TO_DOUBLE(data:send:amount::STRING)
                      ELSE NULL
                    END AS amount,
                    CASE 
                      WHEN IS_ARRAY(data:send:amount) OR IS_ARRAY(data:link:price) THEN NULL
                      WHEN IS_OBJECT(data:send:amount) OR IS_OBJECT(data:link:price) THEN NULL
                      WHEN TRY_TO_DOUBLE(data:send:amount::STRING) IS NOT NULL AND TRY_TO_DOUBLE(data:link:price::STRING) IS NOT NULL 
                        THEN TRY_TO_DOUBLE(data:send:amount::STRING) * TRY_TO_DOUBLE(data:link:price::STRING)
                      ELSE NULL
                    END AS amount_usd,
                    CASE 
                      WHEN IS_ARRAY(data:send:fee_value) THEN NULL
                      WHEN IS_OBJECT(data:send:fee_value) THEN NULL
                      WHEN TRY_TO_DOUBLE(data:send:fee_value::STRING) IS NOT NULL THEN TRY_TO_DOUBLE(data:send:fee_value::STRING)
                      ELSE NULL
                    END AS fee,
                    id, 
                    'Token Transfers' AS "Service", 
                    data:link:asset::STRING AS raw_asset
                FROM axelar.axelscan.fact_transfers
                WHERE status = 'executed'
                  AND simplified_status = 'received'
                  AND (
                    sender_address ilike '%0xce16F69375520ab01377ce7B88f5BA8C48F8D666%' 
                    OR sender_address ilike '%0x492751eC3c57141deb205eC2da8bFcb410738630%' 
                    OR sender_address ilike '%0xDC3D8e1Abe590BCa428a8a2FC4CfDbD1AcF57Bd9%' 
                    OR sender_address ilike '%0xdf4fFDa22270c12d0b5b3788F1669D709476111E%' 
                    OR sender_address ilike '%0xe6B3949F9bBF168f4E3EFc82bc8FD849868CC6d8%' 
                ) 
                UNION ALL
                SELECT  
                    created_at,
                    LOWER(data:call.chain::STRING) AS source_chain,
                    LOWER(data:call.returnValues.destinationChain::STRING) AS destination_chain,
                    data:call.transaction.from::STRING AS user,
                    CASE 
                      WHEN IS_ARRAY(data:amount) OR IS_OBJECT(data:amount) THEN NULL
                      WHEN TRY_TO_DOUBLE(data:amount::STRING) IS NOT NULL THEN TRY_TO_DOUBLE(data:amount::STRING)
                      ELSE NULL
                    END AS amount,
                    CASE 
                      WHEN IS_ARRAY(data:value) OR IS_OBJECT(data:value) THEN NULL
                      WHEN TRY_TO_DOUBLE(data:value::STRING) IS NOT NULL THEN TRY_TO_DOUBLE(data:value::STRING)
                      ELSE NULL
                    END AS amount_usd,
                    COALESCE(
                      CASE 
                        WHEN IS_ARRAY(data:gas:gas_used_amount) OR IS_OBJECT(data:gas:gas_used_amount) 
                          OR IS_ARRAY(data:gas_price_rate:source_token.token_price.usd) OR IS_OBJECT(data:gas_price_rate:source_token.token_price.usd) 
                        THEN NULL
                        WHEN TRY_TO_DOUBLE(data:gas:gas_used_amount::STRING) IS NOT NULL 
                          AND TRY_TO_DOUBLE(data:gas_price_rate:source_token.token_price.usd::STRING) IS NOT NULL 
                        THEN TRY_TO_DOUBLE(data:gas:gas_used_amount::STRING) * TRY_TO_DOUBLE(data:gas_price_rate:source_token.token_price.usd::STRING)
                        ELSE NULL
                      END,
                      CASE 
                        WHEN IS_ARRAY(data:fees:express_fee_usd) OR IS_OBJECT(data:fees:express_fee_usd) THEN NULL
                        WHEN TRY_TO_DOUBLE(data:fees:express_fee_usd::STRING) IS NOT NULL THEN TRY_TO_DOUBLE(data:fees:express_fee_usd::STRING)
                        ELSE NULL
                      END
                    ) AS fee,
                    id, 
                    'GMP' AS "Service", 
                    data:symbol::STRING AS raw_asset
                FROM axelar.axelscan.fact_gmp 
                WHERE status = 'executed'
                  AND simplified_status = 'received'
                  AND (
                      data:approved:returnValues:contractAddress ilike '%0xce16F69375520ab01377ce7B88f5BA8C48F8D666%' 
                      OR data:approved:returnValues:contractAddress ilike '%0x492751eC3c57141deb205eC2da8bFcb410738630%' 
                      OR data:approved:returnValues:contractAddress ilike '%0xDC3D8e1Abe590BCa428a8a2FC4CfDbD1AcF57Bd9%' 
                      OR data:approved:returnValues:contractAddress ilike '%0xdf4fFDa22270c12d0b5b3788F1669D709476111E%' 
                      OR data:approved:returnValues:contractAddress ilike '%0xe6B3949F9bBF168f4E3EFc82bc8FD849868CC6d8%' 
                  ) 
            )
            SELECT created_at, id, user, source_chain, destination_chain,
                 "Service", amount, amount_usd, fee
            FROM axelar_service
        )
        SELECT source_chain AS "Source Chain", 
               destination_chain AS "Destination Chain",
               ROUND(SUM(amount_usd)) AS "Volume (USD)",
               COUNT(DISTINCT id) AS "Number of Transactions"
        FROM overview
        WHERE created_at::date >= '{start_date}' 
          AND created_at::date <= '{end_date}'
          AND amount_usd IS NOT NULL
        GROUP BY 1, 2
        ORDER BY 3 DESC, 4
    """
    return pd.read_sql(query, conn)

# Load Data
src_dest_df = load_source_dest_data(start_date, end_date)

# Bubble Chart 1: Volume
fig_vol = px.scatter(
    src_dest_df,
    x="Source Chain",
    y="Destination Chain",
    size="Volume (USD)",
    color="Source Chain",
    hover_data=["Volume (USD)", "Number of Transactions"],
    title="Source Chain → Destination Chain: Volume (USD)"
)

# Bubble Chart 2: Number of Transactions
fig_txns = px.scatter(
    src_dest_df,
    x="Source Chain",
    y="Destination Chain",
    size="Number of Transactions",
    color="Source Chain",
    hover_data=["Volume (USD)", "Number of Transactions"],
    title="Source Chain → Destination Chain: Number of Transactions"
)

col1, col2 = st.columns(2)
col1.plotly_chart(fig_vol, use_container_width=True)
col2.plotly_chart(fig_txns, use_container_width=True)


# --- Row 7: Satellite Bridge KPIs ------------------------------------------------------------------------------------------------
st.markdown(
    """
    <div style="background-color:#0ed145; padding:1px; border-radius:10px;">
        <h2 style="color:#000000; text-align:center;">Satellite Bridge</h2>
    </div>
    """,
    unsafe_allow_html=True
)
st.info("🛰All data related to the Satellite Bridge has been extracted considering five source chains: Ethereum, BSC, Polygon, Arbitrum, and Avalanche.")
@st.cache_data
def load_satellite_kpi(start_date, end_date):
    query = f"""
        WITH overview AS (
            WITH tab1 AS (
                SELECT block_timestamp::date AS date, tx_hash, source_chain, destination_chain, sender, token_symbol
                FROM AXELAR.DEFI.EZ_BRIDGE_SATELLITE
                WHERE block_timestamp::date >= '{start_date}' AND block_timestamp::date <= '{end_date}'
            ),
            tab2 AS (
                SELECT 
                    created_at::date AS date, 
                    LOWER(data:send:original_source_chain) AS source_chain, 
                    LOWER(data:send:original_destination_chain) AS destination_chain,
                    sender_address AS user, 
                    CASE WHEN TRY_TO_DOUBLE(data:send:amount::STRING) IS NOT NULL THEN TRY_TO_DOUBLE(data:send:amount::STRING) END AS amount,
                    CASE WHEN TRY_TO_DOUBLE(data:send:amount::STRING) IS NOT NULL AND TRY_TO_DOUBLE(data:link:price::STRING) IS NOT NULL
                         THEN TRY_TO_DOUBLE(data:send:amount::STRING) * TRY_TO_DOUBLE(data:link:price::STRING) END AS amount_usd,
                    CASE WHEN TRY_TO_DOUBLE(data:send:fee_value::STRING) IS NOT NULL THEN TRY_TO_DOUBLE(data:send:fee_value::STRING) END AS fee,
                    SPLIT_PART(id, '_', 1) AS tx_hash, data:link:asset::STRING AS raw_asset
                FROM axelar.axelscan.fact_transfers
                WHERE status = 'executed' AND simplified_status = 'received'
                  AND created_at::date >= '{start_date}' AND created_at::date <= '{end_date}'
            )
            SELECT tab1.date, tab1.tx_hash, tab1.source_chain, tab1.destination_chain, sender, token_symbol, amount, amount_usd
            FROM tab1 LEFT JOIN tab2 ON tab1.tx_hash=tab2.tx_hash
        )
        SELECT COUNT(DISTINCT tx_hash) AS "Transactions",
               COUNT(DISTINCT sender) AS "Users",
               ROUND(SUM(amount_usd)) AS "Volume (USD)"
        FROM overview
    """
    return pd.read_sql(query, conn)

sat_kpi_df = load_satellite_kpi(start_date, end_date)

col1, col2, col3 = st.columns(3)
col1.metric("Volume of Transfers", f"${sat_kpi_df['Volume (USD)'][0]:,}")
col2.metric("Number of Transfers", f"{sat_kpi_df['Transactions'][0]:,} Txns")
col3.metric("Number of Users", f"{sat_kpi_df['Users'][0]:,} Addresses")


# --- Row 8: Satellite Bridge Over Time --------------------------------------------------------------------------------------------

@st.cache_data
def load_satellite_over_time(start_date, end_date, timeframe):
    query = f"""
        WITH overview AS (
            WITH tab1 AS (
                SELECT block_timestamp::date AS date, tx_hash, source_chain, destination_chain, sender, token_symbol
                FROM AXELAR.DEFI.EZ_BRIDGE_SATELLITE
                WHERE block_timestamp::date >= '{start_date}' AND block_timestamp::date <= '{end_date}'
            ),
            tab2 AS (
                SELECT 
                    created_at::date AS date, 
                    LOWER(data:send:original_source_chain) AS source_chain, 
                    LOWER(data:send:original_destination_chain) AS destination_chain,
                    sender_address AS user, 
                    CASE WHEN TRY_TO_DOUBLE(data:send:amount::STRING) IS NOT NULL THEN TRY_TO_DOUBLE(data:send:amount::STRING) END AS amount,
                    CASE WHEN TRY_TO_DOUBLE(data:send:amount::STRING) IS NOT NULL AND TRY_TO_DOUBLE(data:link:price::STRING) IS NOT NULL
                         THEN TRY_TO_DOUBLE(data:send:amount::STRING) * TRY_TO_DOUBLE(data:link:price::STRING) END AS amount_usd,
                    SPLIT_PART(id, '_', 1) AS tx_hash
                FROM axelar.axelscan.fact_transfers
                WHERE status = 'executed' AND simplified_status = 'received'
                  AND created_at::date >= '{start_date}' AND created_at::date <= '{end_date}'
            )
            SELECT tab1.date, tab1.tx_hash, tab1.source_chain, tab1.destination_chain, sender, token_symbol, amount, amount_usd
            FROM tab1 LEFT JOIN tab2 ON tab1.tx_hash=tab2.tx_hash
        )
        SELECT DATE_TRUNC('{timeframe}', date) AS "Date",
               COUNT(DISTINCT tx_hash) AS "Transactions",
               COUNT(DISTINCT sender) AS "Users",
               ROUND(SUM(amount_usd)) AS "Volume (USD)"
        FROM overview
        GROUP BY 1
        ORDER BY 1
    """
    return pd.read_sql(query, conn)

sat_time_df = load_satellite_over_time(start_date, end_date, timeframe)

fig_vol = px.bar(sat_time_df, x="Date", y="Volume (USD)", labels={"Volume (USD)": "USD", "Date": " "}, title="Satellite Bridge Volume Over Time (USD)")
fig_txn = px.bar(sat_time_df, x="Date", y="Transactions", labels={"Transactions": "Txns", "Date": " "}, title="Satellite Bridge Transactions Over Time")
fig_users = px.bar(sat_time_df, x="Date", y="Users", labels={"Users": "Addresses", "Date": " "}, title="Satellite Bridge Users Over Time")

col1, col2, col3 = st.columns(3)
col1.plotly_chart(fig_vol, use_container_width=True)
col2.plotly_chart(fig_txn, use_container_width=True)
col3.plotly_chart(fig_users, use_container_width=True)


# --- Row 9: Satellite Bridge Source → Destination ----------------------------------------------------------------------------------

@st.cache_data
def load_satellite_src_dest(start_date, end_date):
    query = f"""
        WITH overview AS (
            WITH tab1 AS (
                SELECT block_timestamp::date AS date, tx_hash, source_chain AS "Source Chain", destination_chain AS "Destination Chain", sender, token_symbol
                FROM AXELAR.DEFI.EZ_BRIDGE_SATELLITE
                WHERE block_timestamp::date >= '{start_date}' AND block_timestamp::date <= '{end_date}'
            ),
            tab2 AS (
                SELECT 
                    created_at::date AS date, 
                    LOWER(data:send:original_source_chain) AS source_chain, 
                    LOWER(data:send:original_destination_chain) AS destination_chain,
                    sender_address AS user, 
                    CASE WHEN TRY_TO_DOUBLE(data:send:amount::STRING) IS NOT NULL THEN TRY_TO_DOUBLE(data:send:amount::STRING) END AS amount,
                    CASE WHEN TRY_TO_DOUBLE(data:send:amount::STRING) IS NOT NULL AND TRY_TO_DOUBLE(data:link:price::STRING) IS NOT NULL
                         THEN TRY_TO_DOUBLE(data:send:amount::STRING) * TRY_TO_DOUBLE(data:link:price::STRING) END AS amount_usd,
                    SPLIT_PART(id, '_', 1) AS tx_hash
                FROM axelar.axelscan.fact_transfers
                WHERE status = 'executed' AND simplified_status = 'received'
                  AND created_at::date >= '{start_date}' AND created_at::date <= '{end_date}'
            )
            SELECT tab1.date, tab1.tx_hash, tab1."Source Chain", tab1."Destination Chain", sender, token_symbol, amount, amount_usd
            FROM tab1 LEFT JOIN tab2 ON tab1.tx_hash=tab2.tx_hash
        )
        SELECT "Source Chain", "Destination Chain",
               COUNT(DISTINCT tx_hash) AS "Number of Transactions",
               ROUND(SUM(amount_usd)) AS "Volume (USD)"
        FROM overview
        where amount_usd is not null
        GROUP BY 1, 2
        ORDER BY 3 DESC, 4
    """
    return pd.read_sql(query, conn)

sat_src_dest_df = load_satellite_src_dest(start_date, end_date)

fig_bubble_vol = px.scatter(
    sat_src_dest_df,
    x="Source Chain",
    y="Destination Chain",
    size="Volume (USD)",
    color="Source Chain",
    hover_data=["Volume (USD)", "Number of Transactions"],
    title="Source Chain → Destination Chain: Volume (USD)"
)

fig_bubble_txn = px.scatter(
    sat_src_dest_df,
    x="Source Chain",
    y="Destination Chain",
    size="Number of Transactions",
    color="Source Chain",
    hover_data=["Volume (USD)", "Number of Transactions"],
    title="Source Chain → Destination Chain: Number of Transactions"
)

col1, col2 = st.columns(2)
col1.plotly_chart(fig_bubble_vol, use_container_width=True)
col2.plotly_chart(fig_bubble_txn, use_container_width=True)


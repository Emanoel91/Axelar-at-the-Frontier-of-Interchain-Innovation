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

# --- Snowflake Connection with PAT -----------------------------------------------------------------------------------------------------
conn = snowflake.connector.connect(
    user=st.secrets["snowflake"]["user"],       # نام کاربری Snowflake
    account=st.secrets["snowflake"]["account"], # اکانت Snowflake
    token=st.secrets["snowflake"]["token"],     # Programmatic Access Token
    authenticator="oauth",                      # ⚠ ضروری برای PAT
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
    <div style="background-color:#faa5fc; padding:1px; border-radius:10px;">
        <h2 style="color:#000000; text-align:center;">Transactions and Users on Axelar</h2>
    </div>
    """,
    unsafe_allow_html=True
)
# --- Row 1 -----------------------------------------------------------------------------------------------------------------------
@st.cache_data
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
@st.cache_data
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
@st.cache_data
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
@st.cache_data
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

# --------------------------------------------------------------------------------------------------------------------------------------------------------
# --------------------------------------------------------------------------------------------------------------------------------------------------------
# --------------------------------------------------------------------------------------------------------------------------------------------------------

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

# --- Query Function: Row (2) --------------------------------------------------------------------------------------
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
        color_discrete_sequence=["#006ac9"]
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
        color_discrete_sequence=["#006ac9"]
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
        color_discrete_sequence=["#006ac9"]
    )
    fig3.update_layout(xaxis_title="", yaxis_title="Addresses", bargap=0.2)
    st.plotly_chart(fig3, use_container_width=True)

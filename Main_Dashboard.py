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

# --- Snowflake Connection using PAT ------------------------------------------

    conn = snowflake.connector.connect(
    account="EVMWWMU-CBC67994",
    user="EMAN_AXELAR",
    authenticator="externalbrowser",  
    role="SYSADMIN"
    warehouse = "SNOWFLAKE_LEARNING_WH"        
    database = "AXELAR"         
    schema = "PUBLIC"           
    )
    
    st.success("✅ Connected to Snowflake successfully!")

    # --- Example query ---
    cur = conn.cursor()
    cur.execute("SELECT CURRENT_TIMESTAMP;")
    result = cur.fetchone()
    st.write("Current Snowflake timestamp:", result[0])

    # --- Example chart query ---
    cur.execute("SELECT COLUMN1, COLUMN2 FROM MY_TABLE LIMIT 10;")
    data = cur.fetchall()
    if data:
        import pandas as pd
        df = pd.DataFrame(data, columns=["COLUMN1", "COLUMN2"])
        st.line_chart(df.set_index("COLUMN1"))

    cur.close()
    conn.close()

except snowflake.connector.errors.ProgrammingError as e:
    st.error(f"Snowflake connection failed: {e}")

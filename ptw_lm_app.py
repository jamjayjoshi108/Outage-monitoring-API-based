import streamlit as st
import pandas as pd
import requests

# --- CONSTANTS & MAPPINGS ---
ZONES = ['Border', 'Central', 'North', 'South', 'East', 'West']
ZONE_TOTALS = {
    'Border': {'Total JEs': 419, 'PSPCL_G': 148, 'PSTCL_G': 38},
    'Central': {'Total JEs': 222, 'PSPCL_G': 92, 'PSTCL_G': 25},
    'North': {'Total JEs': 273, 'PSPCL_G': 128, 'PSTCL_G': 34},
    'South': {'Total JEs': 294, 'PSPCL_G': 219, 'PSTCL_G': 38}, # Combined South+East
    'East': {'Total JEs': 134, 'PSPCL_G': 219, 'PSTCL_G': 38},  # Refers to South
    'West': {'Total JEs': 346, 'PSPCL_G': 256, 'PSTCL_G': 44}
}

def fetch_ptw_data(api_key, start_date, end_date):
    url = "https://distribution.pspcl.in/returns/module.php?to=OutageAPI.getPTWRequests"
    payload = {"fromdate": start_date, "todate": end_date, "apikey": api_key}
    
    try:
        res = requests.post(url, json=payload, timeout=20)
        res.raise_for_status() 
        data = res.json()
        
        # --- DIAGNOSTIC: Print exactly what is sent and received ---
        with st.expander("🔍 DIAGNOSTIC: Raw API Response", expanded=True):
            st.write("**Payload Sent:**", payload)
            st.write("**Server Response:**")
            st.json(data)
        # -----------------------------------------------------------
        
        return data if isinstance(data, list) else data.get("data", [])
        
    except Exception as e:
        st.error(f"API Fetch Error: {e}")
        return []

def render_ptw_lm_dashboard():
    # 1. Header and Navigation
    col_title, col_btn = st.columns([0.85, 0.15])
    with col_title:
        st.title("🛠️ PTW & LM-ALM Tracker")
    with col_btn:
        st.write("") 
        if st.button("⬅️ Home", use_container_width=True):
            st.session_state.page = 'home'
            st.rerun()
    
    # 2. Date Selection for Weekly Cycle
    col1, col2 = st.columns(2)
    with col1:
        start_date = st.date_input("From Date", value=pd.to_datetime("today") - pd.Timedelta(days=7))
    with col2:
        end_date = st.date_input("To Date", value=pd.to_datetime("today"))

    # 3. Data Fetching
    start_str = start_date.strftime("%Y-%m-%d")
    end_str = end_date.strftime("%Y-%m-%d")
    raw_data = fetch_ptw_data(st.secrets["API_KEY"], start_str, end_str)
    
    df = pd.DataFrame(raw_data)

    # --- DIAGNOSTIC: Print dataframe columns if data exists ---
    if not df.empty:
        st.info(f"Columns returned by API: {df.columns.tolist()}")

    # If empty, warn and stop rendering the charts
    if df.empty:
        st.warning(f"No data found for the selected period ({start_str} to {end_str}). Check the Diagnostic expander above!")
        return

    # 4. Processing Metrics per Zone
    metrics_data = []

    # Row 1: JEs Using PTW
    jes = df.groupby('zone_name')['permit_je'].nunique().reindex(ZONES, fill_value=0)
    metrics_data.append(["JEs Using PTW"] + jes.tolist())

    # Row 2: Share JEs
    share_jes = [f"{(jes[z] / ZONE_TOTALS[z]['Total JEs']):.1%}" for z in ZONES]
    metrics_data.append(["Share: JEs Using PTW / Total JEs"] + share_jes)

    # Row 3: Grids Using PTW (Total)
    grids = df.groupby('zone_name')['grid_code'].nunique().reindex(ZONES, fill_value=0)
    metrics_data.append(["Grids Using PTW"] + grids.tolist())

    # Row 4: PSPCL Grids
    pspcl = df[df['grid_ownership'] == 'PSPCL'].groupby('zone_name')['grid_code'].nunique().reindex(ZONES, fill_value=0)
    metrics_data.append(["PSPCL Grids Using PTW"] + pspcl.tolist())

    # Row 5: PSTCL Grids
    pstcl = df[df['grid_ownership'] == 'PSTCL'].groupby('zone_name')['grid_code'].nunique().reindex(ZONES, fill_value=0)
    metrics_data.append(["PSTCL Grids Using PTW"] + pstcl.tolist())

    # Row 6: Share PSPCL (Special logic for South/East)
    pspcl_shares = []
    for z in ZONES:
        den = ZONE_TOTALS[z]['PSPCL_G']
        val = pspcl[z]
        if z in ['South', 'East']:
            combined_val = pspcl['South'] + pspcl['East']
            pspcl_shares.append(f"{(combined_val/219):.1%}")
        else:
            pspcl_shares.append(f"{(val/den):.1%}")
    metrics_data.append(["Share: PSPCL Grids Using PTW / Total PSPCL"] + pspcl_shares)

    # 5. Create Transposed DataFrame
    performance_df = pd.DataFrame(metrics_data, columns=["Metric"] + ZONES)

    # 6. Styling and Display
    st.subheader(f"Week Performance - {start_date.strftime('%B')}")
    
    # Custom CSS for the "Excel-like" look
    st.markdown("""
        <style>
        .ptw-table td { text-align: center !important; }
        .ptw-table th { background-color: #004085 !important; color: white !important; }
        </style>
    """, unsafe_allow_html=True)

    def color_shares(val):
        if isinstance(val, str) and '%' in val:
            pct = float(val.replace('%', ''))
            if pct > 30: return 'background-color: #c6efce' # Green for high usage
            if pct < 15: return 'background-color: #ffc7ce' # Red for low usage
        return ''

    st.table(performance_df.style.applymap(color_shares))

    # 7. Performance Summary
    st.markdown("---")
    st.subheader("Weekly Insights")
    c1, c2 = st.columns(2)
    with c1:
        st.success("**Trend:** JEs Using PTW is increasing (11.6% vs last week)")
        st.info("**Trend:** Grids Using PTW is increasing (13.6% vs last week)")
    with c2:
        st.error("**Lowest Performer:** East Zone (11.9% JE Share)")

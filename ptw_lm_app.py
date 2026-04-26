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
    
    # 2. Advanced Date Selection
    today = pd.to_datetime("today").date()
    
    st.write("---")
    date_preset = st.radio(
        "📅 Select Time Period:", 
        ["Custom Range", "Today", "Current Month", "Last Month", "Last 3 Months", "Last 6 Months"], 
        index=2, # Defaults to Current Month
        horizontal=True
    )
    
    if date_preset == "Today":
        start_date = end_date = today
    elif date_preset == "Current Month":
        start_date = today.replace(day=1)
        end_date = today
    elif date_preset == "Last Month":
        end_date = today.replace(day=1) - pd.Timedelta(days=1)
        start_date = end_date.replace(day=1)
    elif date_preset == "Last 3 Months":
        start_date = (today - pd.DateOffset(months=3)).date()
        end_date = today
    elif date_preset == "Last 6 Months":
        start_date = (today - pd.DateOffset(months=6)).date()
        end_date = today
    else: # Custom Range
        c1, c2 = st.columns(2)
        with c1: start_date = st.date_input("From Date", value=today - pd.Timedelta(days=7))
        with c2: end_date = st.date_input("To Date", value=today)

    # 3. Data Fetching
    start_str = start_date.strftime("%Y-%m-%d")
    end_str = end_date.strftime("%Y-%m-%d")
    raw_data = fetch_ptw_data(st.secrets["API_KEY"], start_str, end_str)
    
    df = pd.DataFrame(raw_data)

    if df.empty:
        st.warning(f"No data found for the selected period ({start_str} to {end_str}).")
        return

    # --- DATA CLEANING ---
    df['zone_name'] = df['zone_name'].astype(str).str.replace(' Zone', '', case=False).str.strip().str.title()
    if 'grid_ownership' in df.columns:
        df['grid_ownership'] = df['grid_ownership'].astype(str).str.strip().str.upper()

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

    # Row 4 & 5: PSPCL and PSTCL Grids
    pspcl = df[df['grid_ownership'] == 'PSPCL'].groupby('zone_name')['grid_code'].nunique().reindex(ZONES, fill_value=0)
    metrics_data.append(["PSPCL Grids Using PTW"] + pspcl.tolist())

    pstcl = df[df['grid_ownership'] == 'PSTCL'].groupby('zone_name')['grid_code'].nunique().reindex(ZONES, fill_value=0)
    metrics_data.append(["PSTCL Grids Using PTW"] + pstcl.tolist())

    # Row 6 & 7: Shares
    pspcl_shares, pstcl_shares = [], []
    for z in ZONES:
        pspcl_den, pstcl_den = ZONE_TOTALS[z]['PSPCL_G'], ZONE_TOTALS[z]['PSTCL_G']
        if z in ['South', 'East']:
            combined_pspcl = pspcl['South'] + pspcl['East']
            pspcl_shares.append(f"{(combined_pspcl/219):.1%}")
            combined_pstcl = pstcl['South'] + pstcl['East']
            pstcl_shares.append(f"{(combined_pstcl/38):.1%}")
        else:
            pspcl_shares.append(f"{(pspcl[z]/pspcl_den):.1%}" if pspcl_den > 0 else "0.0%")
            pstcl_shares.append(f"{(pstcl[z]/pstcl_den):.1%}" if pstcl_den > 0 else "0.0%")
            
    metrics_data.append(["Share: PSPCL Grids Using PTW / Total PSPCL"] + pspcl_shares)
    metrics_data.append(["Share: PSTCL Grids Using PTW / Total PSTCL"] + pstcl_shares)

    # 5. Create Transposed DataFrame
    performance_df = pd.DataFrame(metrics_data, columns=["Metric"] + ZONES)

    # 6. Display the Main Table
    st.write("---")
    # Dynamic header displaying exact dates selected
    st.subheader(f"📊 Performance Overview ({start_date.strftime('%d %b %Y')} to {end_date.strftime('%d %b %Y')})")
    
    def color_shares(val):
        if isinstance(val, str) and '%' in val:
            pct = float(val.replace('%', ''))
            if pct >= 30: return 'background-color: #c6efce; color: #006100;' 
            if pct < 15: return 'background-color: #ffc7ce; color: #9c0006;' 
        return ''

    st.dataframe(performance_df.style.map(color_shares), hide_index=True, use_container_width=True)

    # 7. NEW: Visual Insights Section
    st.write("---")
    st.subheader("📈 Visual Insights: Volume by Zone")
    st.caption("Compare the raw volume of JEs and Grids utilizing the PTW system across all zones.")
    
    # Create a simple DataFrame for charting
    chart_df = pd.DataFrame({
        'Zone': ZONES,
        'JEs Using PTW': jes.tolist(),
        'Grids Using PTW': grids.tolist()
    }).set_index('Zone')
    
    # Render native Streamlit bar chart
    st.bar_chart(chart_df, height=350, use_container_width=True)

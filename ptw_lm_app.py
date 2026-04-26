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

# Overall System Totals for KPIs
SYSTEM_TOTAL_JES = 1688
SYSTEM_TOTAL_GRIDS = 1022

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
    
    # 2. Advanced Date Selection (Session State UI)
    today = pd.to_datetime("today").date()
    
    if 'ptw_start' not in st.session_state:
        st.session_state.ptw_start = today.replace(day=1)
    if 'ptw_end' not in st.session_state:
        st.session_state.ptw_end = today

    st.write("---")
    st.markdown("**📅 Select Time Period**")
    
    # Quick Preset Buttons
    btn_cols = st.columns(5)
    if btn_cols[0].button("Today", use_container_width=True):
        st.session_state.ptw_start = today
        st.session_state.ptw_end = today
    if btn_cols[1].button("Current Month", use_container_width=True):
        st.session_state.ptw_start = today.replace(day=1)
        st.session_state.ptw_end = today
    if btn_cols[2].button("Last Month", use_container_width=True):
        st.session_state.ptw_end = today.replace(day=1) - pd.Timedelta(days=1)
        st.session_state.ptw_start = st.session_state.ptw_end.replace(day=1)
    if btn_cols[3].button("Last 3 Months", use_container_width=True):
        st.session_state.ptw_start = (today - pd.DateOffset(months=3)).date()
        st.session_state.ptw_end = today
    if btn_cols[4].button("Last 6 Months", use_container_width=True):
        st.session_state.ptw_start = (today - pd.DateOffset(months=6)).date()
        st.session_state.ptw_end = today

    # Always-visible Date Inputs syncing with session_state
    c1, c2 = st.columns(2)
    with c1: 
        start_date = st.date_input("From Date", key="ptw_start")
    with c2: 
        end_date = st.date_input("To Date", key="ptw_end")

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

    # --- KPI SECTION ---
    st.markdown("""
        <style>
            .kpi-card { background: linear-gradient(135deg, #004481 0%, #0066cc 100%); border-radius: 6px; padding: 1.2rem; box-shadow: 0 2px 4px rgba(0,0,0,0.08); margin-bottom: 1rem;}
            .kpi-title { color: #FFC107; font-weight: 600; font-size: 0.9rem; text-transform: uppercase; margin-bottom: 0.2rem;}
            .kpi-value { color: #FFFFFF; font-weight: 700; font-size: 2.2rem; line-height: 1.2;}
        </style>
    """, unsafe_allow_html=True)

    total_jes_active = df['permit_je'].nunique()
    total_grids_active = df['grid_code'].nunique()
    je_adoption_rate = total_jes_active / SYSTEM_TOTAL_JES
    grid_adoption_rate = total_grids_active / SYSTEM_TOTAL_GRIDS

    st.subheader(f"📊 Global Performance ({start_date.strftime('%d %b')} to {end_date.strftime('%d %b %Y')})")
    
    k1, k2, k3, k4 = st.columns(4)
    with k1: st.markdown(f'<div class="kpi-card"><div class="kpi-title">Total JEs Using PTW</div><div class="kpi-value">{total_jes_active}</div></div>', unsafe_allow_html=True)
    with k2: st.markdown(f'<div class="kpi-card"><div class="kpi-title">Overall JE Adoption</div><div class="kpi-value">{je_adoption_rate:.1%}</div></div>', unsafe_allow_html=True)
    with k3: st.markdown(f'<div class="kpi-card"><div class="kpi-title">Total Grids Using PTW</div><div class="kpi-value">{total_grids_active}</div></div>', unsafe_allow_html=True)
    with k4: st.markdown(f'<div class="kpi-card"><div class="kpi-title">Overall Grid Adoption</div><div class="kpi-value">{grid_adoption_rate:.1%}</div></div>', unsafe_allow_html=True)

    # 4. Dictionary-Based Data Processing (Bulletproof column alignment)
    jes = df.groupby('zone_name')['permit_je'].nunique().reindex(ZONES, fill_value=0)
    grids = df.groupby('zone_name')['grid_code'].nunique().reindex(ZONES, fill_value=0)
    pspcl = df[df['grid_ownership'] == 'PSPCL'].groupby('zone_name')['grid_code'].nunique().reindex(ZONES, fill_value=0)
    pstcl = df[df['grid_ownership'] == 'PSTCL'].groupby('zone_name')['grid_code'].nunique().reindex(ZONES, fill_value=0)

    # Initialize dictionary with the Metric column first
    data_dict = {
        "Metric": [
            "JEs Using PTW", 
            "Share: JEs Using PTW / Total JEs", 
            "Grids Using PTW", 
            "PSPCL Grids Using PTW", 
            "PSTCL Grids Using PTW", 
            "Share: PSPCL Grids Using PTW / Total PSPCL", 
            "Share: PSTCL Grids Using PTW / Total PSTCL"
        ]
    }

    # Populate each zone's specific data into the dictionary
    for z in ZONES:
        je_den = ZONE_TOTALS[z]['Total JEs']
        je_share = f"{(jes[z] / je_den):.1%}" if je_den > 0 else "0.0%"
        
        pspcl_den, pstcl_den = ZONE_TOTALS[z]['PSPCL_G'], ZONE_TOTALS[z]['PSTCL_G']
        
        # Exception for South & East Grids
        if z in ['South', 'East']:
            combined_pspcl = pspcl['South'] + pspcl['East']
            pspcl_share = f"{(combined_pspcl / 219):.1%}"
            
            combined_pstcl = pstcl['South'] + pstcl['East']
            pstcl_share = f"{(combined_pstcl / 38):.1%}"
        else:
            pspcl_share = f"{(pspcl[z] / pspcl_den):.1%}" if pspcl_den > 0 else "0.0%"
            pstcl_share = f"{(pstcl[z] / pstcl_den):.1%}" if pstcl_den > 0 else "0.0%"
            
        data_dict[z] = [
            int(jes[z]),
            je_share,
            int(grids[z]),
            int(pspcl[z]),
            int(pstcl[z]),
            pspcl_share,
            pstcl_share
        ]

    # 5. Create DataFrame safely from dictionary mapping
    performance_df = pd.DataFrame(data_dict)

    # 6. Safe Dynamic Excel-like Conditional Formatting
    def apply_gradient(row):
        # Pre-allocate styles array to guarantee it matches the row length to prevent Styler crashes
        styles = [''] * len(row)
        
        if "Share" in str(row.iloc[0]):
            vals = []
            # Extract numerical values specifically from column 1 onwards
            for val in row.iloc[1:]:
                try:
                    vals.append(float(str(val).strip('%')))
                except ValueError:
                    vals.append(None)
            
            valid_vals = [v for v in vals if v is not None]
            if not valid_vals:
                return styles
                
            min_val, max_val = min(valid_vals), max(valid_vals)
            
            for i, val in enumerate(vals):
                if val is not None:
                    # Normalized 0.0 to 1.0 mapping
                    norm = (val - min_val) / (max_val - min_val) if max_val > min_val else 0.5
                    
                    if norm < 0.5:
                        pct = norm / 0.5
                        r, g, b = int(248 + (255 - 248) * pct), int(105 + (235 - 105) * pct), int(107 + (132 - 107) * pct)
                    else:
                        pct = (norm - 0.5) / 0.5
                        r, g, b = int(255 + (99 - 255) * pct), int(235 + (195 - 235) * pct), int(132 + (132 - 132) * pct)
                    
                    # Apply styles starting from index 1 (skipping Metric text)
                    styles[i + 1] = f'background-color: rgba({r}, {g}, {b}, 0.6); color: #000000; font-weight: 500;'
                    
        return styles

    # 7. Display the Main Table
    st.write("---")
    st.subheader("Regional Breakdown")
    
    st.markdown("""
        <style>
        .ptw-table td { text-align: center !important; }
        .ptw-table th { background-color: #004085 !important; color: white !important; }
        </style>
    """, unsafe_allow_html=True)

    styled_df = performance_df.style.apply(apply_gradient, axis=1)
    st.dataframe(styled_df, hide_index=True, use_container_width=True)

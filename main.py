# # # =========================================================================================================================
# # # V4 - Motherduck 
# # # =========================================================================================================================
import os
import requests
import streamlit as st
import pandas as pd
from datetime import datetime, timedelta, timezone
from dateutil.relativedelta import relativedelta
from ptw_lm_app import render_ptw_lm_dashboard

# --- PAGE CONFIGURATION ---
st.set_page_config(page_title="Utility Operations Command Center", layout="wide")

# --- INITIALIZE SESSION STATE FOR NAVIGATION & DATES ---
if 'page' not in st.session_state:
    st.session_state.page = 'home'

today_init = pd.to_datetime("today").date()
if 'date_preset' not in st.session_state:
    st.session_state.date_preset = "Today"
if 'start_date' not in st.session_state:
    st.session_state.start_date = today_init
if 'end_date' not in st.session_state:
    st.session_state.end_date = today_init

# --- GLOBAL TABLE HEADER STYLING ---
HEADER_STYLES = [
    {
        'selector': 'th',
        'props': [
            ('background-color', '#004085 !important'),
            ('color', '#FFC107 !important'),
            ('font-weight', 'bold !important'),
            ('text-align', 'center !important')
        ]
    },
    {
        'selector': 'th div',
        'props': [
            ('color', '#FFC107 !important'),
            ('font-weight', 'bold !important')
        ]
    }
]

# --- API & FILE CONSTANTS ---
# Replace with the URL generated from your Cloud Run deployment
CLOUD_RUN_API_URL = "https://outages-dashboard-api-xxxxx-uc.a.run.app" 

# --- IST TIMEZONE SETUP ---
IST = timezone(timedelta(hours=5, minutes=30))

# --- API FETCH FUNCTIONS ---
def fetch_cloud_data(payload):
    try:
        res = requests.post(CLOUD_RUN_API_URL, json=payload, headers={'Content-Type': 'application/json'}, timeout=60, verify=True)
        res.raise_for_status()
        data = res.json()
        return data if isinstance(data, list) else data.get("data", [])
    except Exception as e:
        print(f"API Fetch Error for {payload.get('table')}: {e}")
        return []

# --- CORE DATA PIPELINE (Fetches from MotherDuck via Cloud Run) ---
@st.cache_data(ttl=900, show_spinner="Fetching data from MotherDuck via Cloud Run...")
def load_data_pipeline():
    now_ist = datetime.now(IST)
    end_date_str = now_ist.strftime("%Y-%m-%d")
    start_date_str = (now_ist - timedelta(days=180)).strftime("%Y-%m-%d")

    # ==========================================
    # 1. OUTAGES LOGIC
    # ==========================================
    outages_raw = fetch_cloud_data({
        "table": "outages", 
        "start_date": start_date_str, 
        "end_date": end_date_str
    })
    df_outages = pd.DataFrame(outages_raw)

    # Standardize Outages for Dashboard Consumption
    if not df_outages.empty:
        df_outages.rename(columns={
            "zone_name": "Zone", "circle_name": "Circle", "feeder_name": "Feeder", 
            "outage_type": "Type of Outage", "outage_status": "Status", 
            "start_time": "Start Time", "end_time": "End Time", "duration_minutes": "Diff in mins"
        }, inplace=True)
        
        # Deduplicate
        if {'Circle', 'Feeder', 'Start Time', 'Type of Outage', 'Status'}.issubset(df_outages.columns):
            df_outages = df_outages.drop_duplicates(
                subset=['Circle', 'Feeder', 'Start Time', 'Type of Outage', 'Status'], 
                keep='last'
            )

        if 'Type of Outage' in df_outages.columns:
            df_outages['Raw Outage Type'] = df_outages['Type of Outage'].astype(str).str.strip()
            def standardize_outage(val):
                v_lower = str(val).lower()
                if 'power off' in v_lower: return 'Power Off By PC'
                if 'unplanned' in v_lower: return 'Unplanned Outage'
                if 'planned' in v_lower: return 'Planned Outage'
                return val
            df_outages['Type of Outage'] = df_outages['Raw Outage Type'].apply(standardize_outage)

        df_outages['Start Time'] = pd.to_datetime(df_outages['Start Time'], errors='coerce')
        
        if 'Diff in mins' in df_outages.columns:
            df_outages['Diff in mins'] = pd.to_numeric(df_outages['Diff in mins'], errors='coerce')
            def assign_bucket(mins):
                if pd.isna(mins) or mins < 0: return "Active/Unknown"
                hrs = mins / 60
                if hrs <= 2: return "Up to 2 Hrs"
                elif hrs <= 4: return "2-4 Hrs"
                elif hrs <= 8: return "4-8 Hrs"
                else: return "Above 8 Hrs"
            df_outages['Duration Bucket'] = df_outages['Diff in mins'].apply(assign_bucket)
            
        if 'Status' in df_outages.columns:
            df_outages['Status_Calc'] = df_outages['Status'].apply(lambda x: 'Active' if str(x).strip().title() in ['Active', 'Open'] else 'Closed')


    # ==========================================
    # 2. PTW LOGIC
    # ==========================================
    ptw_raw = fetch_cloud_data({
        "table": "ptw", 
        "start_date": start_date_str, 
        "end_date": end_date_str
    })
    df_ptw = pd.DataFrame(ptw_raw)

    if not df_ptw.empty:
        if 'feeders' in df_ptw.columns:
            df_ptw['feeders'] = df_ptw['feeders'].apply(lambda x: ', '.join(map(str, x)) if isinstance(x, list) else str(x))
        df_ptw.rename(columns={
            "ptw_id": "PTW Request ID", "permit_no": "Permit Number", 
            "circle_name": "Circle", "feeders": "Feeder", "current_status": "Status", 
            "start_time": "Start Date", "end_time": "End Date", "creation_date": "Request Date"
        }, inplace=True)
        
        if 'PTW Request ID' in df_ptw.columns:
            df_ptw = df_ptw.drop_duplicates(subset=['PTW Request ID'], keep='last')

    fetch_time = now_ist.strftime('%d %b %Y, %I:%M %p')
    return df_outages, df_ptw, fetch_time


# --- HELPER FUNCTIONS ---
def generate_yoy_dist_expanded(df_curr, df_ly, group_col):
    def _agg(df, prefix):
        if df.empty: return pd.DataFrame({group_col: []}).set_index(group_col)
        df['Diff in mins'] = pd.to_numeric(df['Diff in mins'], errors='coerce').fillna(0)
        g = df.groupby([group_col, 'Type of Outage']).agg(Count=('Type of Outage', 'size'), TotalHrs=('Diff in mins', lambda x: round(x.sum() / 60, 2)), AvgHrs=('Diff in mins', lambda x: round(x.mean() / 60, 2))).unstack(fill_value=0)
        g.columns = [f"{prefix} {outage} ({metric})" for metric, outage in g.columns]
        return g

    c_grp = _agg(df_curr, 'Curr')
    l_grp = _agg(df_ly, 'LY')
    merged = pd.merge(c_grp, l_grp, on=group_col, how='outer').fillna(0).reset_index()
    
    expected_cols = []
    for prefix in ['Curr', 'LY']:
        for outage in ['Planned Outage', 'Power Off By PC', 'Unplanned Outage']:
            for metric in ['Count', 'TotalHrs', 'AvgHrs']:
                col_name = f"{prefix} {outage} ({metric})"
                expected_cols.append(col_name)
                if col_name not in merged.columns: merged[col_name] = 0
                    
    for col in expected_cols:
        if '(Count)' in col: merged[col] = merged[col].astype(int)
        else: merged[col] = merged[col].astype(float).round(2)
            
    merged['Curr Total (Count)'] = merged['Curr Planned Outage (Count)'] + merged['Curr Power Off By PC (Count)'] + merged['Curr Unplanned Outage (Count)']
    merged['LY Total (Count)'] = merged['LY Planned Outage (Count)'] + merged['LY Power Off By PC (Count)'] + merged['LY Unplanned Outage (Count)']
    merged['YoY Delta (Total)'] = merged['Curr Total (Count)'] - merged['LY Total (Count)']
    
    cols_order = [group_col, 
                  'Curr Planned Outage (Count)', 'Curr Planned Outage (TotalHrs)', 'Curr Planned Outage (AvgHrs)', 
                  'LY Planned Outage (Count)', 'LY Planned Outage (TotalHrs)', 'LY Planned Outage (AvgHrs)', 
                  'Curr Power Off By PC (Count)', 'Curr Power Off By PC (TotalHrs)', 'Curr Power Off By PC (AvgHrs)', 
                  'LY Power Off By PC (Count)', 'LY Power Off By PC (TotalHrs)', 'LY Power Off By PC (AvgHrs)', 
                  'Curr Unplanned Outage (Count)', 'Curr Unplanned Outage (TotalHrs)', 'Curr Unplanned Outage (AvgHrs)', 
                  'LY Unplanned Outage (Count)', 'LY Unplanned Outage (TotalHrs)', 'LY Unplanned Outage (AvgHrs)', 
                  'Curr Total (Count)', 'LY Total (Count)', 'YoY Delta (Total)']
    return merged[cols_order]

def apply_pu_gradient(styler, df):
    p_cols = [c for c in df.columns if 'Planned' in str(c) and pd.api.types.is_numeric_dtype(df[c])]
    u_cols = [c for c in df.columns if 'Unplanned' in str(c) and pd.api.types.is_numeric_dtype(df[c])]
    po_cols = [c for c in df.columns if 'Power Off' in str(c) and pd.api.types.is_numeric_dtype(df[c])]
    
    if p_cols: styler = styler.background_gradient(subset=p_cols, cmap='Blues', vmin=0)
    if u_cols: styler = styler.background_gradient(subset=u_cols, cmap='Reds', vmin=0)
    if po_cols: styler = styler.background_gradient(subset=po_cols, cmap='Greens', vmin=0)
    return styler

def highlight_delta(val):
    if isinstance(val, int):
        if val > 0: return 'color: #D32F2F; font-weight: bold;'
        elif val < 0: return 'color: #388E3C; font-weight: bold;'
    return ''

def create_bucket_pivot(df, bucket_order):
    if df.empty or 'Duration Bucket' not in df.columns or 'Circle' not in df.columns: 
        return pd.DataFrame(columns=bucket_order + ['Total'])
    pivot = pd.crosstab(df['Circle'], df['Duration Bucket'])
    pivot = pivot.reindex(columns=[c for c in bucket_order if c in pivot.columns], fill_value=0)
    pivot['Total'] = pivot.sum(axis=1)
    return pivot


# ==========================================
# PAGE 1: HOME (COMMAND CENTER)
# ==========================================
def render_home():
    st.markdown("""
        <style>
            .home-title {
                text-align: center;
                color: #004085;
                font-weight: 700;
                font-size: 2.5rem;
                margin-top: 2rem;
                margin-bottom: 0.5rem;
                font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif;
            }
            .home-subtitle {
                text-align: center;
                color: #555555;
                font-size: 1.1rem;
                margin-bottom: 3rem;
            }
            div.stButton > button {
                height: 90px;
                font-size: 1.1rem;
                font-weight: 600;
                background-color: #ffffff;
                color: #333333;
                border: 1px solid #e0e0e0;
                border-radius: 8px;
                box-shadow: 0 4px 6px rgba(0,0,0,0.05);
                transition: all 0.3s ease;
            }
            div.stButton > button:hover {
                border-color: #004085;
                box-shadow: 0 6px 12px rgba(0,0,0,0.15);
                color: #004085;
                transform: translateY(-2px);
            }
        </style>
    """, unsafe_allow_html=True)

    st.markdown("<div class='home-title'>⚡ Utility Operations Command Center</div>", unsafe_allow_html=True)
    st.markdown("<div class='home-subtitle'>Select an operational module below to access real-time dashboards and management tools.</div>", unsafe_allow_html=True)
    
    st.write("---")
    
    st.write("")
    row1_col1, row1_col2, row1_col3 = st.columns(3, gap="large")
    
    with row1_col1:
        if st.button("🛠️ PTW, LM-ALM Application", use_container_width=True):
            st.session_state.page = 'ptw_app'
            st.rerun()
            
    with row1_col2:
        if st.button("📉 Outage Reduction Plan (ORP)", use_container_width=True):
            st.toast("This module is currently offline or under development.")
            
    with row1_col3:
        if st.button("🏢 RDSS", use_container_width=True):
            st.toast("This module is currently offline or under development.")

    st.write("")
    row2_col1, row2_col2, row2_col3 = st.columns(3, gap="large")
    
    with row2_col1:
        if st.button("📡 Smart Meter", use_container_width=True):
            st.toast("This module is currently offline or under development.")
            
    with row2_col2:
        if st.button("🔌 New Connections", use_container_width=True):
            st.toast("This module is currently offline or under development.")
            
    with row2_col3:
        if st.button("🚨 Outage Monitoring", use_container_width=True):
            st.session_state.page = 'dashboard'
            st.rerun()

# ==========================================
# PAGE 2: MAIN DASHBOARD
# ==========================================
def render_dashboard():
    # Apply Dashboard styling
    st.markdown("""
        <style>
            .block-container { padding-top: 1.5rem; padding-bottom: 1.5rem; font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif; }
            p, span, div, caption, .stMarkdown { color: #000000 !important; }
            h1, h2, h3, h4, h5, h6, div.block-container h1 { color: #004085 !important; font-weight: 700 !important; font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif; }
            div.block-container h1 { text-align: center; border-bottom: 3px solid #004085 !important; padding-bottom: 10px; margin-bottom: 30px !important; font-size: 2.2rem !important; }
            h2 { font-size: 1.3rem !important; border-bottom: 2px solid #004085 !important; padding-bottom: 5px; margin-bottom: 10px !important; }
            h3 { font-size: 1.05rem !important; margin-bottom: 12px !important; text-transform: uppercase; letter-spacing: 0.5px; }
            hr { border: 0; border-top: 1px solid #004085; margin: 1.5rem 0; opacity: 0.3; }
            
            .kpi-card { background: linear-gradient(135deg, #004481 0%, #0066cc 100%); border-radius: 6px; padding: 1.2rem 1.2rem; display: flex; flex-direction: column; justify-content: space-between; height: 100%; box-shadow: 0 2px 4px rgba(0,0,0,0.08); transition: transform 0.2s ease-in-out, box-shadow 0.2s ease-in-out; border: 1px solid #003366; }
            .kpi-card:hover { transform: translateY(-4px); box-shadow: 0 8px 16px rgba(0, 68, 129, 0.2); }
            .kpi-card .kpi-title, .kpi-title { color: #FFC107 !important; font-weight: 600; font-size: 0.85rem; text-transform: uppercase; letter-spacing: 0.5px; margin-bottom: 0.4rem; }
            .kpi-card .kpi-value, .kpi-value { color: #FFFFFF !important; font-weight: 700; font-size: 2.6rem; margin-bottom: 0; line-height: 1.1; }
            .kpi-card .kpi-subtext, .kpi-subtext { color: #F8F9FA !important; font-size: 0.85rem; margin-top: 1rem; padding-top: 0.6rem; border-top: 1px solid rgba(255, 255, 255, 0.2); display: flex; justify-content: flex-start; gap: 15px; }
            
            .status-badge { background-color: rgba(0, 0, 0, 0.25); padding: 3px 8px; border-radius: 4px; font-weight: 500; color: #FFFFFF !important; }
            [data-testid="stDataFrame"] > div { border: 2px solid #004085 !important; border-radius: 6px; overflow: hidden; }
        </style>
    """, unsafe_allow_html=True)

    col1, col2 = st.columns([0.75, 0.25])
    with col1:
        st.title("⚡ Power Outage Monitoring Dashboard")
    with col2:
        st.write("")
        btn_col1, btn_col2 = st.columns(2)
        with btn_col1:
            if st.button("⬅️ Home", use_container_width=True):
                st.session_state.page = 'home'
                st.rerun()
        with btn_col2:
            with st.popover("🔄 Refresh", use_container_width=True):
                st.markdown("**Admin Access Required**")
                pwd = st.text_input("Passcode:", type="password", placeholder="Enter passcode...")
                if st.button("Confirm Refresh", use_container_width=True):
                    if pwd == "J@Y":
                        st.cache_data.clear()
                        st.rerun()
                    else:
                        st.error("Incorrect password.")

    # --- INITIATE GLOBAL DATA PIPELINE ---
    df_all_outages, df_all_ptw, last_updated = load_data_pipeline()
    
    with col2:
        st.markdown(f"<div style='text-align: right; color: #666; font-size: 0.85rem; margin-top: 4px;'>Database Synced:<br><b>{last_updated}</b></div>", unsafe_allow_html=True)

    # --- HELPER FOR INDEPENDENT DATE PRESETS ---
    def get_preset_dates(preset):
        t = pd.to_datetime("today").date()
        if preset == "Today": return t, t
        elif preset == "Current Month": return t.replace(day=1), t
        elif preset == "Last Month": 
            e = t.replace(day=1) - pd.Timedelta(days=1)
            return e.replace(day=1), e
        elif preset == "Last 3 Months": return (t - pd.DateOffset(months=3)).date(), t
        elif preset == "Last 6 Months": return (t - pd.DateOffset(months=6)).date(), t
        return t, t

    # --- CALLBACKS TO FORCE UI UPDATE ---
    def update_dates_t1():
        st.session_state.start_t1, st.session_state.end_t1 = get_preset_dates(st.session_state.preset_t1)

    def update_dates_t2():
        st.session_state.start_t2, st.session_state.end_t2 = get_preset_dates(st.session_state.preset_t2)

    def update_dates_t3():
        st.session_state.start_t3, st.session_state.end_t3 = get_preset_dates(st.session_state.preset_t3)

    # Initialize states on first load if they don't exist
    if "preset_t1" not in st.session_state: st.session_state.preset_t1 = "Today"
    if "preset_t2" not in st.session_state: st.session_state.preset_t2 = "Today"
    if "preset_t3" not in st.session_state: st.session_state.preset_t3 = "Today"
    
    if "start_t1" not in st.session_state: st.session_state.start_t1, st.session_state.end_t1 = get_preset_dates("Today")
    if "start_t2" not in st.session_state: st.session_state.start_t2, st.session_state.end_t2 = get_preset_dates("Today")
    if "start_t3" not in st.session_state: st.session_state.start_t3, st.session_state.end_t3 = get_preset_dates("Today")

    tab1, tab2, tab3 = st.tabs(["📊 Dashboard", "📈 YoY Comparison", "🛠️ PTW Frequency"])

    # ==========================================
    # TAB 1: ORIGINAL DASHBOARD
    # ==========================================
    with tab1:
        st.radio("📅 Select Time Period:", ["Today", "Current Month", "Last Month", "Last 3 Months", "Last 6 Months"], key="preset_t1", horizontal=True, on_change=update_dates_t1)
        
        c1, c2 = st.columns(2)
        start_date_1 = c1.date_input("From Date", key="start_t1")
        end_date_1 = c2.date_input("To Date", key="end_t1")
        end_str_1 = end_date_1.strftime("%Y-%m-%d")

        # Filtering logic for Tab 1
        if not df_all_outages.empty:
            df_all_outages['DateOnly'] = pd.to_datetime(df_all_outages['Start Time'], errors='coerce').dt.date
            df_5day = df_all_outages[(df_all_outages['DateOnly'] >= start_date_1) & (df_all_outages['DateOnly'] <= end_date_1)].copy() 
            df_today = df_all_outages[df_all_outages['DateOnly'] == end_date_1].copy()
        else:
            df_5day = pd.DataFrame()
            df_today = pd.DataFrame()

        if not df_5day.empty and 'Status' in df_5day.columns:
            valid_5day = df_5day[~df_5day['Status'].astype(str).str.contains('Cancel', case=False, na=False)]
        else:
            valid_5day = pd.DataFrame()

        if not df_today.empty and 'Status' in df_today.columns:
            valid_today = df_today[~df_today['Status'].astype(str).str.contains('Cancel', case=False, na=False)]
        else:
            valid_today = pd.DataFrame()

        if not valid_5day.empty and 'Type of Outage' in valid_5day.columns:
            fiveday_planned = valid_5day[valid_5day['Type of Outage'] == 'Planned Outage'] 
            fiveday_popc = valid_5day[valid_5day['Type of Outage'] == 'Power Off By PC'] 
            fiveday_unplanned = valid_5day[valid_5day['Type of Outage'] == 'Unplanned Outage'] 
        else:
            fiveday_planned = fiveday_popc = fiveday_unplanned = pd.DataFrame()

        if start_date_1 == end_date_1:
            st.header(f"📅 Outage Summary ({end_date_1.strftime('%d %b %Y')})")
        else:
            st.header(f"📅 Outage Summary ({start_date_1.strftime('%d %b')} to {end_date_1.strftime('%d %b %Y')})")
        
        kpi1, kpi2, kpi3 = st.columns(3)
        with kpi1:
            active_p = len(fiveday_planned[fiveday_planned['Status_Calc'] == 'Active']) if not fiveday_planned.empty else 0
            closed_p = len(fiveday_planned[fiveday_planned['Status_Calc'] == 'Closed']) if not fiveday_planned.empty else 0
            st.markdown(f'<div class="kpi-card"><div><div class="kpi-title">Planned Outages</div><div class="kpi-value">{len(fiveday_planned)}</div></div><div class="kpi-subtext"><span class="status-badge">🔴 Active: {active_p}</span> <span class="status-badge">🟢 Closed: {closed_p}</span></div></div>', unsafe_allow_html=True)
        with kpi2:
            active_po = len(fiveday_popc[fiveday_popc['Status_Calc'] == 'Active']) if not fiveday_popc.empty else 0
            closed_po = len(fiveday_popc[fiveday_popc['Status_Calc'] == 'Closed']) if not fiveday_popc.empty else 0
            st.markdown(f'<div class="kpi-card"><div><div class="kpi-title">Power Off By PC</div><div class="kpi-value">{len(fiveday_popc)}</div></div><div class="kpi-subtext"><span class="status-badge">🔴 Active: {active_po}</span> <span class="status-badge">🟢 Closed: {closed_po}</span></div></div>', unsafe_allow_html=True)
        with kpi3:
            active_u = len(fiveday_unplanned[fiveday_unplanned['Status_Calc'] == 'Active']) if not fiveday_unplanned.empty else 0
            closed_u = len(fiveday_unplanned[fiveday_unplanned['Status_Calc'] == 'Closed']) if not fiveday_unplanned.empty else 0
            st.markdown(f'<div class="kpi-card"><div><div class="kpi-title">Unplanned Outages</div><div class="kpi-value">{len(fiveday_unplanned)}</div></div><div class="kpi-subtext"><span class="status-badge">🔴 Active: {active_u}</span> <span class="status-badge">🟢 Closed: {closed_u}</span></div></div>', unsafe_allow_html=True)

        st.divider()
        st.subheader("Zone-wise Distribution (Selected Range)")
        if not valid_5day.empty and 'Zone' in valid_5day.columns and 'Type of Outage' in valid_5day.columns:
            zone_range = valid_5day.groupby(['Zone', 'Type of Outage']).size().unstack(fill_value=0).reset_index()
            for col in ['Planned Outage', 'Power Off By PC', 'Unplanned Outage']:
                if col not in zone_range: zone_range[col] = 0
            zone_range['Total'] = zone_range['Planned Outage'] + zone_range['Power Off By PC'] + zone_range['Unplanned Outage']
            
            styled_zone_range = apply_pu_gradient(zone_range.style, zone_range).set_table_styles(HEADER_STYLES)
            st.dataframe(styled_zone_range, width="stretch", hide_index=True)
        else: st.info(f"No data available for selected dates.")

        st.divider()
        st.header(f"🚨 Top 5 Notorious Feeders (By Outage Frequency)")
        
        # Notorious Logic mapped to Tab 1 dates
        notorious_feeders_list = pd.DataFrame()
        top_5_notorious = pd.DataFrame(columns=['Circle', 'Feeder', 'Total Outage Events', 'Total Duration (Hours)', 'Max Duration (Hours)'])
        notorious_set = set()
        
        if not valid_5day.empty:
            valid_5day['Diff in mins'] = pd.to_numeric(valid_5day['Diff in mins'], errors='coerce').fillna(0)
            valid_5day['Diff in mins'] = valid_5day['Diff in mins'].apply(lambda x: max(x, 0))
            if start_date_1 == end_date_1:
                lookback_date = end_date_1 - pd.Timedelta(days=2)
                mask = (df_all_outages['DateOnly'] >= lookback_date) & (df_all_outages['DateOnly'] <= end_date_1) & (~df_all_outages['Status'].astype(str).str.contains('Cancel', case=False, na=False))
                df_eval = df_all_outages[mask].copy()
                if not df_eval.empty:
                    days_count = df_eval.groupby(['Circle', 'Feeder'])['DateOnly'].nunique().reset_index()
                    notorious_feeders_list = days_count[days_count['DateOnly'] >= 3][['Circle', 'Feeder']]
            else:
                df_eval = valid_5day.copy()
                weekly_counts = df_eval.groupby(['Circle', 'Feeder', pd.Grouper(key='Start Time', freq='W')])['DateOnly'].nunique().reset_index()
                notorious_feeders_list = weekly_counts[weekly_counts['DateOnly'] >= 3][['Circle', 'Feeder']].drop_duplicates()

            if not notorious_feeders_list.empty:
                stats = df_eval.merge(notorious_feeders_list, on=['Circle', 'Feeder'])
                stats = stats.groupby(['Circle', 'Feeder']).agg(
                    Total_Events=('Start Time', 'size'), Total_Mins=('Diff in mins', 'sum'), Max_Mins=('Diff in mins', 'max') 
                ).reset_index()
                stats.rename(columns={'Total_Events': 'Total Outage Events'}, inplace=True)
                stats['Total Duration (Hours)'] = (stats['Total_Mins'] / 60).round(2)
                stats['Max Duration (Hours)'] = (stats['Max_Mins'] / 60).round(2) 
                notorious = stats.drop(columns=['Total_Mins', 'Max_Mins']).sort_values(by=['Circle', 'Total Outage Events', 'Total Duration (Hours)'], ascending=[True, False, False])
                top_5_notorious = notorious.groupby('Circle').head(5)
                notorious_set = set(zip(top_5_notorious['Circle'], top_5_notorious['Feeder']))

        noto_col1, noto_col2 = st.columns(2)
        with noto_col1: selected_notorious_circle = st.selectbox("Filter by Circle:", ["All Circles"] + sorted(top_5_notorious['Circle'].unique().tolist()) if not top_5_notorious.empty else ["All Circles"], index=0, key="noto_circ_1")
        with noto_col2: selected_notorious_type = st.selectbox("Filter by Outage Type:", ["All Types", "Planned Outage", "Power Off By PC", "Unplanned Outage"], index=0, key="noto_type_1")

        df_dyn = valid_5day.copy()
        if selected_notorious_type != "All Types" and not df_dyn.empty and 'Type of Outage' in df_dyn.columns: 
            df_dyn = df_dyn[df_dyn['Type of Outage'] == selected_notorious_type]

        if not df_dyn.empty:
            dyn_stats = df_dyn.groupby(['Circle', 'Feeder']).agg(Total_Events=('Start Time', 'size'), Total_Mins=('Diff in mins', 'sum'), Max_Mins=('Diff in mins', 'max')).reset_index()
            if not notorious_feeders_list.empty:
                dyn_noto = dyn_stats.merge(notorious_feeders_list, on=['Circle', 'Feeder']).drop_duplicates()
            else:
                dyn_noto = pd.DataFrame()

            if not dyn_noto.empty:
                dyn_noto.rename(columns={'Total_Events': 'Total Outage Events'}, inplace=True)
                dyn_noto['Total Duration (Hours)'] = (dyn_noto['Total_Mins'] / 60).round(2)
                dyn_noto['Max Duration (Hours)'] = (dyn_noto['Max_Mins'] / 60).round(2) 
                dyn_noto = dyn_noto.drop(columns=['Total_Mins', 'Max_Mins']).sort_values(by=['Circle', 'Total Outage Events', 'Total Duration (Hours)'], ascending=[True, False, False])
                dyn_top5 = dyn_noto.groupby('Circle').head(5)
                filtered_notorious = dyn_top5[dyn_top5['Circle'] == selected_notorious_circle] if selected_notorious_circle != "All Circles" else dyn_top5

                if not filtered_notorious.empty:
                    st.dataframe(filtered_notorious.style.format({'Max Duration (Hours)': '{:.2f}', 'Total Duration (Hours)': '{:.2f}'}).set_table_styles(HEADER_STYLES), width="stretch", hide_index=True)
                else: st.info(f"No notorious feeders found.")
            else: st.info(f"No notorious feeders identified.")
        else: st.info("No data available.")

        st.divider()
        st.header("Comprehensive Circle-wise Breakdown")
        bucket_order = ["Up to 2 Hrs", "2-4 Hrs", "4-8 Hrs", "Above 8 Hrs", "Active/Unknown"]

        # Uses safely filtered valid_today (end_date_1) and valid_5day (range)
        today_p = valid_today[valid_today['Type of Outage'] == 'Planned Outage'] if 'Type of Outage' in valid_today.columns else pd.DataFrame()
        today_po = valid_today[valid_today['Type of Outage'] == 'Power Off By PC'] if 'Type of Outage' in valid_today.columns else pd.DataFrame()
        today_u = valid_today[valid_today['Type of Outage'] == 'Unplanned Outage'] if 'Type of Outage' in valid_today.columns else pd.DataFrame()

        curr_1d_p_tab1 = create_bucket_pivot(today_p, bucket_order)
        curr_1d_po_tab1 = create_bucket_pivot(today_po, bucket_order)
        curr_1d_u_tab1 = create_bucket_pivot(today_u, bucket_order)
        curr_5d_p_tab1 = create_bucket_pivot(fiveday_planned, bucket_order)
        curr_5d_po_tab1 = create_bucket_pivot(fiveday_popc, bucket_order)
        curr_5d_u_tab1 = create_bucket_pivot(fiveday_unplanned, bucket_order)

        combined_circle = pd.concat(
            [curr_1d_p_tab1, curr_1d_po_tab1, curr_1d_u_tab1, curr_5d_p_tab1, curr_5d_po_tab1, curr_5d_u_tab1], 
            axis=1, 
            keys=['END DATE (Planned)', 'END DATE (Power Off)', 'END DATE (Unplanned)', 'RANGE (Planned)', 'RANGE (Power Off)', 'RANGE (Unplanned)']
        ).fillna(0).astype(int)

        if not combined_circle.empty:
            styled_combined = apply_pu_gradient(combined_circle.style, combined_circle).set_table_styles(HEADER_STYLES)
            selection_event = st.dataframe(styled_combined, width="stretch", on_select="rerun", selection_mode="single-row")

            if len(selection_event.selection.rows) > 0:
                selected_circle = combined_circle.index[selection_event.selection.rows[0]]
                st.subheader(f"Feeder Details for: {selected_circle}")
                
                circle_dates = sorted(list(valid_5day[valid_5day['Circle'] == selected_circle]['Outage Date'].dropna().unique()))
                selected_dates = st.multiselect("Filter Range View by Date:", options=circle_dates, default=circle_dates, format_func=lambda x: x.strftime('%d %b %Y'), key="ms_tab1")
                
                def highlight_notorious(row): return ['background-color: rgba(220, 53, 69, 0.15); color: #850000; font-weight: bold'] * len(row) if (selected_circle, row['Feeder']) in notorious_set else [''] * len(row)

                st.write("---")
                st.markdown(f"### 🔴 SINGLE DAY DRILLDOWN ({end_str_1})")
                today_left, today_mid, today_right = st.columns(3)
                with today_left:
                    st.markdown(f"**Planned Outages**")
                    fl_tp = today_p[today_p['Circle'] == selected_circle][['Feeder', 'Diff in mins', 'Status_Calc', 'Duration Bucket']].rename(columns={'Status_Calc': 'Status'}) if not today_p.empty else pd.DataFrame(columns=['Feeder', 'Diff in mins', 'Status', 'Duration Bucket'])
                    st.dataframe(fl_tp.style.apply(highlight_notorious, axis=1).set_table_styles(HEADER_STYLES), width="stretch", hide_index=True)
                with today_mid:
                    st.markdown(f"**Power Off By PC**")
                    fl_tpo = today_po[today_po['Circle'] == selected_circle][['Feeder', 'Diff in mins', 'Status_Calc', 'Duration Bucket']].rename(columns={'Status_Calc': 'Status'}) if not today_po.empty else pd.DataFrame(columns=['Feeder', 'Diff in mins', 'Status', 'Duration Bucket'])
                    st.dataframe(fl_tpo.style.apply(highlight_notorious, axis=1).set_table_styles(HEADER_STYLES), width="stretch", hide_index=True)
                with today_right:
                    st.markdown(f"**Unplanned Outages**")
                    fl_tu = today_u[today_u['Circle'] == selected_circle][['Feeder', 'Diff in mins', 'Status_Calc', 'Duration Bucket']].rename(columns={'Status_Calc': 'Status'}) if not today_u.empty else pd.DataFrame(columns=['Feeder', 'Diff in mins', 'Status', 'Duration Bucket'])
                    st.dataframe(fl_tu.style.apply(highlight_notorious, axis=1).set_table_styles(HEADER_STYLES), width="stretch", hide_index=True)
                    
                st.write("---") 
                st.markdown(f"### 🟢 SELECTED RANGE DRILLDOWN")
                fiveday_left, fiveday_mid, fiveday_right = st.columns(3)
                with fiveday_left:
                    st.markdown(f"**Planned Outages**")
                    fl_fp = fiveday_planned[(fiveday_planned['Circle'] == selected_circle) & (fiveday_planned['Outage Date'].isin(selected_dates))].copy() if not fiveday_planned.empty else pd.DataFrame()
                    if not fl_fp.empty:
                        fl_fp['Diff in Hours'] = (fl_fp['Diff in mins'] / 60).round(2)
                        st.dataframe(fl_fp[['Outage Date', 'Start Time', 'Feeder', 'Diff in Hours', 'Duration Bucket']].style.apply(highlight_notorious, axis=1).format({'Diff in Hours': '{:.2f}'}).set_table_styles(HEADER_STYLES), width="stretch", hide_index=True)
                with fiveday_mid:
                    st.markdown(f"**Power Off By PC**")
                    fl_fpo = fiveday_popc[(fiveday_popc['Circle'] == selected_circle) & (fiveday_popc['Outage Date'].isin(selected_dates))].copy() if not fiveday_popc.empty else pd.DataFrame()
                    if not fl_fpo.empty:
                        fl_fpo['Diff in Hours'] = (fl_fpo['Diff in mins'] / 60).round(2)
                        st.dataframe(fl_fpo[['Outage Date', 'Start Time', 'Feeder', 'Diff in Hours', 'Duration Bucket']].style.apply(highlight_notorious, axis=1).format({'Diff in Hours': '{:.2f}'}).set_table_styles(HEADER_STYLES), width="stretch", hide_index=True)
                with fiveday_right:
                    st.markdown(f"**Unplanned Outages**")
                    fl_fu = fiveday_unplanned[(fiveday_unplanned['Circle'] == selected_circle) & (fiveday_unplanned['Outage Date'].isin(selected_dates))].copy() if not fiveday_unplanned.empty else pd.DataFrame()
                    if not fl_fu.empty:
                        fl_fu['Diff in Hours'] = (fl_fu['Diff in mins'] / 60).round(2)
                        st.dataframe(fl_fu[['Outage Date', 'Start Time', 'Feeder', 'Diff in Hours', 'Duration Bucket']].style.apply(highlight_notorious, axis=1).format({'Diff in Hours': '{:.2f}'}).set_table_styles(HEADER_STYLES), width="stretch", hide_index=True)

    # ==========================================
    # TAB 2: YOY DRILL-DOWN
    # ==========================================
    with tab2:
        st.radio("📅 Select Time Period:", ["Today", "Current Month", "Last Month", "Last 3 Months", "Last 6 Months"], key="preset_t2", horizontal=True, on_change=update_dates_t2)
        c1, c2 = st.columns(2)
        start_date_2 = c1.date_input("From Date", key="start_t2")
        end_date_2 = c2.date_input("To Date", key="end_t2")

        st.header("📈 Historical Year-over-Year Drilldown")
        if df_all_outages.empty:
            st.error("Master Outages Data not found.")
        else:
            st.markdown(f"**Comparing Period:** {start_date_2.strftime('%d %b %Y')} to {end_date_2.strftime('%d %b %Y')}")
            st.divider()

            def get_ly_date(d):
                try: return d.replace(year=d.year - 1)
                except ValueError: return d.replace(year=d.year - 1, day=28)

            ly_start = get_ly_date(start_date_2)
            ly_end = get_ly_date(end_date_2)
            
            mask_curr = (df_all_outages['DateOnly'] >= start_date_2) & (df_all_outages['DateOnly'] <= end_date_2)
            filtered_curr = df_all_outages[mask_curr]
            
            try:
                df_ly_master = pd.read_csv("Historical_2025.csv")
                if 'Start Time' in df_ly_master.columns:
                    df_ly_master['DateOnly'] = pd.to_datetime(df_ly_master['Start Time'], errors='coerce').dt.date
                    mask_ly = (df_ly_master['DateOnly'] >= ly_start) & (df_ly_master['DateOnly'] <= ly_end)
                    filtered_ly = df_ly_master[mask_ly]
                else:
                    st.error("Column 'Start Time' is missing from Historical_2025.csv")
                    filtered_ly = pd.DataFrame()
            except FileNotFoundError:
                st.error("Historical_2025.csv not found in the directory.")
                filtered_ly = pd.DataFrame()

            if not filtered_curr.empty or not filtered_ly.empty:
                st.markdown(f"### 📍 1. Zone-wise Distribution")
                yoy_zone = generate_yoy_dist_expanded(filtered_curr, filtered_ly, 'Zone')
                zone_selection = st.dataframe(
                    yoy_zone.style.map(highlight_delta, subset=['YoY Delta (Total)']).format(precision=2).set_table_styles(HEADER_STYLES), 
                    width="stretch", 
                    hide_index=True, 
                    on_select="rerun", 
                    selection_mode="single-row",
                    key="yoy_zone_select" 
                )

                if len(zone_selection.selection.rows) > 0:
                    selected_zone = yoy_zone.iloc[zone_selection.selection.rows[0]]['Zone']
                    st.markdown(f"### 🎯 2. Circle-wise Distribution for **{selected_zone}**")
                    
                    curr_zone_df = filtered_curr[filtered_curr['Zone'] == selected_zone]
                    ly_zone_df = filtered_ly[filtered_ly['Zone'] == selected_zone]
                    
                    yoy_circle = generate_yoy_dist_expanded(curr_zone_df, ly_zone_df, 'Circle')
                    circle_selection = st.dataframe(
                        yoy_circle.style.map(highlight_delta, subset=['YoY Delta (Total)']).format(precision=2).set_table_styles(HEADER_STYLES), 
                        width="stretch", 
                        hide_index=True, 
                        on_select="rerun", 
                        selection_mode="single-row",
                        key="yoy_circle_select" 
                    )

                    if len(circle_selection.selection.rows) > 0:
                        selected_circle = yoy_circle.iloc[circle_selection.selection.rows[0]]['Circle']
                        st.markdown(f"### 🔌 3. Feeder-wise Distribution for **{selected_circle}**")
                        
                        curr_circle_df = curr_zone_df[curr_zone_df['Circle'] == selected_circle]
                        ly_circle_df = ly_zone_df[ly_zone_df['Circle'] == selected_circle]
                        
                        yoy_feeder = generate_yoy_dist_expanded(curr_circle_df, ly_circle_df, 'Feeder')
                        st.dataframe(
                            yoy_feeder.style.map(highlight_delta, subset=['YoY Delta (Total)']).format(precision=2).set_table_styles(HEADER_STYLES), 
                            width="stretch", 
                            hide_index=True,
                            key="yoy_feeder_display"
                        )
            else:
                st.info("No data available for the selected dates in both 2026 and 2025.")

    # ==========================================
    # TAB 3: PTW FREQUENCY
    # ==========================================
    with tab3:
        st.radio("📅 Select Time Period:", ["Today", "Current Month", "Last Month", "Last 3 Months", "Last 6 Months"], key="preset_t3", horizontal=True, on_change=update_dates_t3)
        c1, c2 = st.columns(2)
        start_date_3 = c1.date_input("From Date", key="start_t3")
        end_date_3 = c2.date_input("To Date", key="end_t3")
        end_str_3 = end_date_3.strftime("%Y-%m-%d")

        st.header(f"🛠️ PTW Frequency Tracker ({st.session_state.preset_t3})")

        if not df_all_ptw.empty:
            df_all_ptw['DateOnly_Start'] = pd.to_datetime(df_all_ptw['Start Date'], dayfirst=False, errors='coerce').dt.date
            df_all_ptw['DateOnly_Req'] = pd.to_datetime(df_all_ptw['Request Date'], dayfirst=False, errors='coerce').dt.date
            mask_ptw = ((df_all_ptw['DateOnly_Start'] >= start_date_3) & (df_all_ptw['DateOnly_Start'] <= end_date_3)) | \
                       ((df_all_ptw['DateOnly_Req'] >= start_date_3) & (df_all_ptw['DateOnly_Req'] <= end_date_3))
            df_ptw = df_all_ptw[mask_ptw].copy()
        else:
            df_ptw = pd.DataFrame()

        if df_ptw.empty:
            st.info("No PTW data found for the selected period.")
        else:
            ptw_col = next((c for c in df_ptw.columns if 'ptw' in c.lower() or 'request' in c.lower() or 'id' in c.lower()), None)
            feeder_col = next((c for c in df_ptw.columns if 'feeder' in c.lower()), None)
            status_col = next((c for c in df_ptw.columns if 'status' in c.lower()), None)
            circle_col = next((c for c in df_ptw.columns if 'circle' in c.lower()), None)

            if not ptw_col or not feeder_col:
                st.error("Could not dynamically map required columns from the PTW export.")
            else:
                ptw_clean = df_ptw.copy()
                if status_col:
                    ptw_clean = ptw_clean[~ptw_clean[status_col].astype(str).str.contains('Cancellation', na=False, case=False)]

                ptw_clean[feeder_col] = ptw_clean[feeder_col].astype(str).str.replace('|', ',', regex=False).str.split(',')
                ptw_clean = ptw_clean.explode(feeder_col).reset_index(drop=True)
                ptw_clean[feeder_col] = ptw_clean[feeder_col].astype(str).str.strip()
                ptw_clean = ptw_clean[(ptw_clean[feeder_col] != '') & (ptw_clean[feeder_col].str.lower() != 'nan')]

                group_cols = [feeder_col]
                if circle_col: group_cols.insert(0, circle_col)
                    
                ptw_counts = ptw_clean.groupby(group_cols).agg(Unique_PTWs=(ptw_col, 'nunique'), PTW_IDs=(ptw_col, lambda x: ', '.join(x.dropna().astype(str).unique()))).reset_index()
                repeat_feeders = ptw_counts[ptw_counts['Unique_PTWs'] >= 2].sort_values(by='Unique_PTWs', ascending=False).reset_index(drop=True)
                repeat_feeders = repeat_feeders.rename(columns={'Unique_PTWs': 'PTW Request Count', 'PTW_IDs': 'Associated PTW Request Numbers'})

                kpi1, kpi2 = st.columns(2)
                with kpi1: st.markdown(f'<div class="kpi-card"><div><div class="kpi-title">Total Active PTW Requests</div><div class="kpi-value">{df_ptw[ptw_col].nunique()}</div></div><div class="kpi-subtext"><span class="status-badge">Selected Period</span></div></div>', unsafe_allow_html=True)
                with kpi2: st.markdown(f'<div class="kpi-card"><div><div class="kpi-title">Feeders with Multiple PTWs</div><div class="kpi-value">{len(repeat_feeders)}</div></div><div class="kpi-subtext"><span class="status-badge" style="background-color: #D32F2F;">🔴 Needs Review</span></div></div>', unsafe_allow_html=True)

                st.divider()
                st.subheader("⚠️ Repeat PTW Feeders Detail View")
                if not repeat_feeders.empty:
                    st.dataframe(repeat_feeders.style.set_table_styles(HEADER_STYLES), width="stretch", hide_index=True)
                else:
                    st.success("No feeders had multiple PTWs requested against them! 🎉")

                st.divider()
                st.subheader("⏳ Specific Day PTW Requests")
                start_col_ptw = next((c for c in df_ptw.columns if ('start' in c.lower() or 'from' in c.lower()) and ('date' in c.lower() or 'time' in c.lower())), None)
                end_col_ptw = next((c for c in df_ptw.columns if ('end' in c.lower() or 'to' in c.lower()) and ('date' in c.lower() or 'time' in c.lower())), None)

                if start_col_ptw and end_col_ptw:
                    today_ptws = ptw_clean.copy()
                    today_ptws[start_col_ptw] = pd.to_datetime(today_ptws[start_col_ptw], dayfirst=False, errors='coerce')
                    today_ptws[end_col_ptw] = pd.to_datetime(today_ptws[end_col_ptw], dayfirst=False, errors='coerce')
                    req_date_col = next((c for c in df_ptw.columns if 'request' in c.lower() and ('date' in c.lower() or 'time' in c.lower())), None)
                    
                    if req_date_col:
                        today_ptws[req_date_col] = pd.to_datetime(today_ptws[req_date_col], dayfirst=False, errors='coerce')
                        mask = (today_ptws[start_col_ptw].dt.date == pd.to_datetime(end_str_3).date()) | (today_ptws[req_date_col].dt.date == pd.to_datetime(end_str_3).date())
                    else:
                        mask = (today_ptws[start_col_ptw].dt.date == pd.to_datetime(end_str_3).date())
                    
                    today_ptws = today_ptws[mask]
                    if not today_ptws.empty:
                        today_ptws['Duration (Hours)'] = (today_ptws[end_col_ptw] - today_ptws[start_col_ptw]).dt.total_seconds() / 3600.0
                        today_ptws['Duration (Hours)'] = today_ptws['Duration (Hours)'].apply(lambda x: max(x, 0)).round(2)
                        
                        def ptw_bucket(hrs):
                            if pd.isna(hrs): return "Unknown"
                            if hrs <= 2: return "0-2 Hrs"
                            elif hrs <= 4: return "2-4 Hrs"
                            elif hrs <= 8: return "4-8 Hrs"
                            else: return "Above 8 Hrs"
                        
                        today_ptws['Time Bucket'] = today_ptws['Duration (Hours)'].apply(ptw_bucket)
                        display_cols_ptw = [feeder_col, start_col_ptw, end_col_ptw, 'Duration (Hours)', 'Time Bucket']
                        if circle_col: display_cols_ptw.insert(0, circle_col)
                        
                        final_today_ptws = today_ptws[display_cols_ptw].dropna(subset=[start_col_ptw]).sort_values(by='Duration (Hours)', ascending=False).reset_index(drop=True)
                        def highlight_long_ptw(row): return ['background-color: rgba(220, 53, 69, 0.15); color: #850000; font-weight: bold'] * len(row) if pd.notna(row['Duration (Hours)']) and row['Duration (Hours)'] > 5 else [''] * len(row)
                            
                        over_5_count = final_today_ptws[final_today_ptws['Duration (Hours)'] > 5][feeder_col].nunique()
                        st.markdown(f"**Total Feeders under PTW on {end_str_3} exceeding 5 Hours:** `{over_5_count}`")
                        st.dataframe(final_today_ptws.style.apply(highlight_long_ptw, axis=1).format({'Duration (Hours)': '{:.2f}'}).set_table_styles(HEADER_STYLES), width="stretch", hide_index=True)
                    else: st.info(f"No PTW requests recorded specifically for {end_str_3}.")
                else: st.warning("Could not dynamically identify Start and End time columns in the PTW report.")

# --- ROUTER LOGIC ---
if st.session_state.page == 'home':
    render_home()
elif st.session_state.page == 'dashboard':
    render_dashboard()
elif st.session_state.page == 'ptw_app':
    render_ptw_lm_dashboard()




# # # =========================================================================================================================
# # # V3
# # # =========================================================================================================================
# import os
# import requests
# import streamlit as st
# import pandas as pd
# from datetime import datetime, timedelta, timezone
# from dateutil.relativedelta import relativedelta
# from ptw_lm_app import render_ptw_lm_dashboard

# # --- PAGE CONFIGURATION ---
# st.set_page_config(page_title="Utility Operations Command Center", layout="wide")

# # --- INITIALIZE SESSION STATE FOR NAVIGATION & DATES ---
# if 'page' not in st.session_state:
#     st.session_state.page = 'home'

# today_init = pd.to_datetime("today").date()
# if 'date_preset' not in st.session_state:
#     st.session_state.date_preset = "Today"
# if 'start_date' not in st.session_state:
#     st.session_state.start_date = today_init
# if 'end_date' not in st.session_state:
#     st.session_state.end_date = today_init

# # --- GLOBAL TABLE HEADER STYLING ---
# HEADER_STYLES = [
#     {
#         'selector': 'th',
#         'props': [
#             ('background-color', '#004085 !important'),
#             ('color', '#FFC107 !important'),
#             ('font-weight', 'bold !important'),
#             ('text-align', 'center !important')
#         ]
#     },
#     {
#         'selector': 'th div',
#         'props': [
#             ('color', '#FFC107 !important'),
#             ('font-weight', 'bold !important')
#         ]
#     }
# ]

# # --- API & FILE CONSTANTS ---
# OUTAGE_URL = "https://distribution.pspcl.in/returns/module.php?to=OutageAPI.getOutages"
# PTW_URL = "https://distribution.pspcl.in/returns/module.php?to=OutageAPI.getPTWRequests"
# HISTORICAL_CSV = "Historical_2025.csv"

# # --- IST TIMEZONE SETUP ---
# IST = timezone(timedelta(hours=5, minutes=30))

# # --- API FETCH FUNCTIONS ---
# def fetch_from_api(url, payload):
#     try:
#         res = requests.post(url, json=payload, headers={'Content-Type': 'application/json'}, timeout=60, verify=True)
#         res.raise_for_status()
#         data = res.json()
#         return data if isinstance(data, list) else data.get("data", [])
#     except Exception as e:
#         print(f"API Fetch Error for {payload['fromdate']} to {payload['todate']}: {e}")
#         return []

# def fetch_outages_chunked(start_str, end_str, api_key):
#     """Chunks the outage requests month-by-month to bypass server timeouts."""
#     start = datetime.strptime(start_str, "%Y-%m-%d")
#     end = datetime.strptime(end_str, "%Y-%m-%d")
#     all_data = []
    
#     while start <= end:
#         month_end = start + relativedelta(months=1) - timedelta(days=1)
#         if month_end > end:
#             month_end = end
            
#         chunk = fetch_from_api(OUTAGE_URL, {"fromdate": start.strftime("%Y-%m-%d"), "todate": month_end.strftime("%Y-%m-%d"), "apikey": api_key})
#         if chunk:
#             all_data.extend(chunk)
            
#         start += relativedelta(months=1)
#     return all_data

# # --- CORE DATA PIPELINE (Direct API Fetch + Historical CSV) ---
# @st.cache_data(ttl=900, show_spinner="Fetching current API data & loading Historical CSV...")
# def load_data_pipeline(start_date_str, end_date_str):
#     api_key = st.secrets["API_KEY"]
#     now_ist = datetime.now(IST)

#     curr_start = datetime.strptime(start_date_str, "%Y-%m-%d")
    
#     # Pad start fetch by 2 days to satisfy the 3-day lookback for Notorious Feeders logic
#     fetch_start = curr_start - timedelta(days=2)
#     fetch_start_str = fetch_start.strftime("%Y-%m-%d")

#     # ==========================================
#     # 1. OUTAGES LOGIC
#     # ==========================================
#     # Get Current Data from API
#     curr_outages = fetch_outages_chunked(fetch_start_str, end_date_str, api_key)
#     df_curr = pd.DataFrame(curr_outages)
    
#     if not df_curr.empty:
#         df_curr.rename(columns={
#             "zone_name": "Zone", "circle_name": "Circle", "feeder_name": "Feeder", 
#             "outage_type": "Type of Outage", "outage_status": "Status", 
#             "start_time": "Start Time", "end_time": "End Time", "duration_minutes": "Diff in mins"
#         }, inplace=True)

#     # Get Last Year's Data from Local CSV
#     if os.path.exists(HISTORICAL_CSV):
#         df_ly = pd.read_csv(HISTORICAL_CSV)
#         # Rename columns just in case the CSV is a raw API dump
#         df_ly.rename(columns={
#             "zone_name": "Zone", "circle_name": "Circle", "feeder_name": "Feeder", 
#             "outage_type": "Type of Outage", "outage_status": "Status", 
#             "start_time": "Start Time", "end_time": "End Time", "duration_minutes": "Diff in mins"
#         }, inplace=True)
#     else:
#         df_ly = pd.DataFrame()

#     # Combine API and CSV data
#     if not df_curr.empty and not df_ly.empty:
#         df_outages = pd.concat([df_curr, df_ly], ignore_index=True)
#     elif not df_curr.empty:
#         df_outages = df_curr
#     else:
#         df_outages = df_ly

#     if not df_outages.empty:
#         # Standardize Outages for Dashboard Consumption
#         if 'Type of Outage' in df_outages.columns:
#             df_outages['Raw Outage Type'] = df_outages['Type of Outage'].astype(str).str.strip()
#             def standardize_outage(val):
#                 v_lower = str(val).lower()
#                 if 'power off' in v_lower: return 'Power Off By PC'
#                 if 'unplanned' in v_lower: return 'Unplanned Outage'
#                 if 'planned' in v_lower: return 'Planned Outage'
#                 return val
#             df_outages['Type of Outage'] = df_outages['Raw Outage Type'].apply(standardize_outage)

#         df_outages['Start Time'] = pd.to_datetime(df_outages['Start Time'], errors='coerce')
        
#         if 'Diff in mins' in df_outages.columns:
#             df_outages['Diff in mins'] = pd.to_numeric(df_outages['Diff in mins'], errors='coerce')
#             def assign_bucket(mins):
#                 if pd.isna(mins) or mins < 0: return "Active/Unknown"
#                 hrs = mins / 60
#                 if hrs <= 2: return "Up to 2 Hrs"
#                 elif hrs <= 4: return "2-4 Hrs"
#                 elif hrs <= 8: return "4-8 Hrs"
#                 else: return "Above 8 Hrs"
#             df_outages['Duration Bucket'] = df_outages['Diff in mins'].apply(assign_bucket)
            
#         if 'Status' in df_outages.columns:
#             df_outages['Status_Calc'] = df_outages['Status'].apply(lambda x: 'Active' if str(x).strip().title() in ['Active', 'Open'] else 'Closed')

#         # Force identical datetime alignment by dropping seconds
#         df_outages['temp_time'] = pd.to_datetime(df_outages['Start Time'], errors='coerce').dt.floor('min')
#         df_outages = df_outages.drop_duplicates(subset=['Circle', 'Feeder', 'temp_time'], keep='last').drop(columns=['temp_time'])

#     # ==========================================
#     # 2. PTW LOGIC
#     # ==========================================
#     new_ptw = fetch_from_api(PTW_URL, {"fromdate": start_date_str, "todate": end_date_str, "apikey": api_key})
#     df_ptw = pd.DataFrame(new_ptw) if new_ptw else pd.DataFrame()

#     if not df_ptw.empty:
#         if 'feeders' in df_ptw.columns:
#             df_ptw['feeders'] = df_ptw['feeders'].apply(lambda x: ', '.join(map(str, x)) if isinstance(x, list) else str(x))
#         df_ptw.rename(columns={
#             "ptw_id": "PTW Request ID", "permit_no": "Permit Number", 
#             "circle_name": "Circle", "feeders": "Feeder", "current_status": "Status", 
#             "start_time": "Start Date", "end_time": "End Date", "creation_date": "Request Date"
#         }, inplace=True)
        
#         if 'PTW Request ID' in df_ptw.columns:
#             df_ptw = df_ptw.drop_duplicates(subset=['PTW Request ID'], keep='last')

#     fetch_time = now_ist.strftime('%d %b %Y, %I:%M %p')
#     return df_outages, df_ptw, fetch_time


# # --- HELPER FUNCTIONS ---
# def generate_yoy_dist_expanded(df_curr, df_ly, group_col):
#     def _agg(df, prefix):
#         if df.empty: return pd.DataFrame({group_col: []}).set_index(group_col)
#         df['Diff in mins'] = pd.to_numeric(df['Diff in mins'], errors='coerce').fillna(0)
#         g = df.groupby([group_col, 'Type of Outage']).agg(Count=('Type of Outage', 'size'), TotalHrs=('Diff in mins', lambda x: round(x.sum() / 60, 2)), AvgHrs=('Diff in mins', lambda x: round(x.mean() / 60, 2))).unstack(fill_value=0)
#         g.columns = [f"{prefix} {outage} ({metric})" for metric, outage in g.columns]
#         return g

#     c_grp = _agg(df_curr, 'Curr')
#     l_grp = _agg(df_ly, 'LY')
#     merged = pd.merge(c_grp, l_grp, on=group_col, how='outer').fillna(0).reset_index()
    
#     expected_cols = []
#     for prefix in ['Curr', 'LY']:
#         for outage in ['Planned Outage', 'Power Off By PC', 'Unplanned Outage']:
#             for metric in ['Count', 'TotalHrs', 'AvgHrs']:
#                 col_name = f"{prefix} {outage} ({metric})"
#                 expected_cols.append(col_name)
#                 if col_name not in merged.columns: merged[col_name] = 0
                    
#     for col in expected_cols:
#         if '(Count)' in col: merged[col] = merged[col].astype(int)
#         else: merged[col] = merged[col].astype(float).round(2)
            
#     merged['Curr Total (Count)'] = merged['Curr Planned Outage (Count)'] + merged['Curr Power Off By PC (Count)'] + merged['Curr Unplanned Outage (Count)']
#     merged['LY Total (Count)'] = merged['LY Planned Outage (Count)'] + merged['LY Power Off By PC (Count)'] + merged['LY Unplanned Outage (Count)']
#     merged['YoY Delta (Total)'] = merged['Curr Total (Count)'] - merged['LY Total (Count)']
    
#     cols_order = [group_col, 
#                   'Curr Planned Outage (Count)', 'Curr Planned Outage (TotalHrs)', 'Curr Planned Outage (AvgHrs)', 
#                   'LY Planned Outage (Count)', 'LY Planned Outage (TotalHrs)', 'LY Planned Outage (AvgHrs)', 
#                   'Curr Power Off By PC (Count)', 'Curr Power Off By PC (TotalHrs)', 'Curr Power Off By PC (AvgHrs)', 
#                   'LY Power Off By PC (Count)', 'LY Power Off By PC (TotalHrs)', 'LY Power Off By PC (AvgHrs)', 
#                   'Curr Unplanned Outage (Count)', 'Curr Unplanned Outage (TotalHrs)', 'Curr Unplanned Outage (AvgHrs)', 
#                   'LY Unplanned Outage (Count)', 'LY Unplanned Outage (TotalHrs)', 'LY Unplanned Outage (AvgHrs)', 
#                   'Curr Total (Count)', 'LY Total (Count)', 'YoY Delta (Total)']
#     return merged[cols_order]

# def apply_pu_gradient(styler, df):
#     p_cols = [c for c in df.columns if 'Planned' in str(c) and pd.api.types.is_numeric_dtype(df[c])]
#     u_cols = [c for c in df.columns if 'Unplanned' in str(c) and pd.api.types.is_numeric_dtype(df[c])]
#     po_cols = [c for c in df.columns if 'Power Off' in str(c) and pd.api.types.is_numeric_dtype(df[c])]
    
#     if p_cols: styler = styler.background_gradient(subset=p_cols, cmap='Blues', vmin=0)
#     if u_cols: styler = styler.background_gradient(subset=u_cols, cmap='Reds', vmin=0)
#     if po_cols: styler = styler.background_gradient(subset=po_cols, cmap='Greens', vmin=0)
#     return styler

# def highlight_delta(val):
#     if isinstance(val, int):
#         if val > 0: return 'color: #D32F2F; font-weight: bold;'
#         elif val < 0: return 'color: #388E3C; font-weight: bold;'
#     return ''

# def create_bucket_pivot(df, bucket_order):
#     if df.empty or 'Duration Bucket' not in df.columns or 'Circle' not in df.columns: 
#         return pd.DataFrame(columns=bucket_order + ['Total'])
#     pivot = pd.crosstab(df['Circle'], df['Duration Bucket'])
#     pivot = pivot.reindex(columns=[c for c in bucket_order if c in pivot.columns], fill_value=0)
#     pivot['Total'] = pivot.sum(axis=1)
#     return pivot


# # ==========================================
# # PAGE 1: HOME (COMMAND CENTER)
# # ==========================================
# def render_home():
#     st.markdown("""
#         <style>
#             .home-title {
#                 text-align: center;
#                 color: #004085;
#                 font-weight: 700;
#                 font-size: 2.5rem;
#                 margin-top: 2rem;
#                 margin-bottom: 0.5rem;
#                 font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif;
#             }
#             .home-subtitle {
#                 text-align: center;
#                 color: #555555;
#                 font-size: 1.1rem;
#                 margin-bottom: 3rem;
#             }
#             div.stButton > button {
#                 height: 90px;
#                 font-size: 1.1rem;
#                 font-weight: 600;
#                 background-color: #ffffff;
#                 color: #333333;
#                 border: 1px solid #e0e0e0;
#                 border-radius: 8px;
#                 box-shadow: 0 4px 6px rgba(0,0,0,0.05);
#                 transition: all 0.3s ease;
#             }
#             div.stButton > button:hover {
#                 border-color: #004085;
#                 box-shadow: 0 6px 12px rgba(0,0,0,0.15);
#                 color: #004085;
#                 transform: translateY(-2px);
#             }
#         </style>
#     """, unsafe_allow_html=True)

#     st.markdown("<div class='home-title'>⚡ Utility Operations Command Center</div>", unsafe_allow_html=True)
#     st.markdown("<div class='home-subtitle'>Select an operational module below to access real-time dashboards and management tools.</div>", unsafe_allow_html=True)
    
#     st.write("---")
    
#     st.write("")
#     row1_col1, row1_col2, row1_col3 = st.columns(3, gap="large")
    
#     with row1_col1:
#         if st.button("🛠️ PTW, LM-ALM Application", use_container_width=True):
#             st.session_state.page = 'ptw_app'
#             st.rerun()
            
#     with row1_col2:
#         if st.button("📉 Outage Reduction Plan (ORP)", use_container_width=True):
#             st.toast("This module is currently offline or under development.")
            
#     with row1_col3:
#         if st.button("🏢 RDSS", use_container_width=True):
#             st.toast("This module is currently offline or under development.")

#     st.write("")
#     row2_col1, row2_col2, row2_col3 = st.columns(3, gap="large")
    
#     with row2_col1:
#         if st.button("📡 Smart Meter", use_container_width=True):
#             st.toast("This module is currently offline or under development.")
            
#     with row2_col2:
#         if st.button("🔌 New Connections", use_container_width=True):
#             st.toast("This module is currently offline or under development.")
            
#     with row2_col3:
#         if st.button("🚨 Outage Monitoring", use_container_width=True):
#             st.session_state.page = 'dashboard'
#             st.rerun()

# # ==========================================
# # PAGE 2: MAIN DASHBOARD
# # ==========================================
# def render_dashboard():
#     # Apply Dashboard styling
#     st.markdown("""
#         <style>
#             .block-container { padding-top: 1.5rem; padding-bottom: 1.5rem; font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif; }
#             p, span, div, caption, .stMarkdown { color: #000000 !important; }
#             h1, h2, h3, h4, h5, h6, div.block-container h1 { color: #004085 !important; font-weight: 700 !important; font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif; }
#             div.block-container h1 { text-align: center; border-bottom: 3px solid #004085 !important; padding-bottom: 10px; margin-bottom: 30px !important; font-size: 2.2rem !important; }
#             h2 { font-size: 1.3rem !important; border-bottom: 2px solid #004085 !important; padding-bottom: 5px; margin-bottom: 10px !important; }
#             h3 { font-size: 1.05rem !important; margin-bottom: 12px !important; text-transform: uppercase; letter-spacing: 0.5px; }
#             hr { border: 0; border-top: 1px solid #004085; margin: 1.5rem 0; opacity: 0.3; }
            
#             .kpi-card { background: linear-gradient(135deg, #004481 0%, #0066cc 100%); border-radius: 6px; padding: 1.2rem 1.2rem; display: flex; flex-direction: column; justify-content: space-between; height: 100%; box-shadow: 0 2px 4px rgba(0,0,0,0.08); transition: transform 0.2s ease-in-out, box-shadow 0.2s ease-in-out; border: 1px solid #003366; }
#             .kpi-card:hover { transform: translateY(-4px); box-shadow: 0 8px 16px rgba(0, 68, 129, 0.2); }
#             .kpi-card .kpi-title, .kpi-title { color: #FFC107 !important; font-weight: 600; font-size: 0.85rem; text-transform: uppercase; letter-spacing: 0.5px; margin-bottom: 0.4rem; }
#             .kpi-card .kpi-value, .kpi-value { color: #FFFFFF !important; font-weight: 700; font-size: 2.6rem; margin-bottom: 0; line-height: 1.1; }
#             .kpi-card .kpi-subtext, .kpi-subtext { color: #F8F9FA !important; font-size: 0.85rem; margin-top: 1rem; padding-top: 0.6rem; border-top: 1px solid rgba(255, 255, 255, 0.2); display: flex; justify-content: flex-start; gap: 15px; }
            
#             .status-badge { background-color: rgba(0, 0, 0, 0.25); padding: 3px 8px; border-radius: 4px; font-weight: 500; color: #FFFFFF !important; }
#             [data-testid="stDataFrame"] > div { border: 2px solid #004085 !important; border-radius: 6px; overflow: hidden; }
#         </style>
#     """, unsafe_allow_html=True)

#     col1, col2 = st.columns([0.75, 0.25])
#     with col1:
#         st.title("⚡ Power Outage Monitoring Dashboard")
#     with col2:
#         st.write("")
#         btn_col1, btn_col2 = st.columns(2)
#         with btn_col1:
#             if st.button("⬅️ Home", use_container_width=True):
#                 st.session_state.page = 'home'
#                 st.rerun()
#         with btn_col2:
#             with st.popover("🔄 Refresh", use_container_width=True):
#                 st.markdown("**Admin Access Required**")
#                 pwd = st.text_input("Passcode:", type="password", placeholder="Enter passcode...")
#                 if st.button("Confirm Refresh", use_container_width=True):
#                     if pwd == "J@Y":
#                         st.cache_data.clear()
#                         st.rerun()
#                     else:
#                         st.error("Incorrect password.")

#     def update_dates():
#         preset = st.session_state.date_preset
#         t = pd.to_datetime("today").date()
#         if preset == "Today":
#             st.session_state.start_date = t
#             st.session_state.end_date = t
#         elif preset == "Current Month":
#             st.session_state.start_date = t.replace(day=1)
#             st.session_state.end_date = t
#         elif preset == "Last Month":
#             e = t.replace(day=1) - pd.Timedelta(days=1)
#             st.session_state.start_date = e.replace(day=1)
#             st.session_state.end_date = e
#         elif preset == "Last 3 Months":
#             st.session_state.start_date = (t - pd.DateOffset(months=3)).date()
#             st.session_state.end_date = t
#         elif preset == "Last 6 Months":
#             st.session_state.start_date = (t - pd.DateOffset(months=6)).date()
#             st.session_state.end_date = t

#     st.write("---")
#     st.radio(
#         "📅 Select Time Period:", 
#         ["Today", "Current Month", "Last Month", "Last 3 Months", "Last 6 Months"], 
#         key="date_preset",
#         on_change=update_dates,
#         horizontal=True
#     )
    
#     c1, c2 = st.columns(2)
#     start_date = c1.date_input("From Date", key="start_date")
#     end_date = c2.date_input("To Date", key="end_date")

#     start_str = start_date.strftime("%Y-%m-%d")
#     end_str = end_date.strftime("%Y-%m-%d")
#     preset_label = st.session_state.date_preset

#     # --- INITIATE DATA PIPELINE ---
#     df_all_outages, df_all_ptw, last_updated = load_data_pipeline(start_str, end_str)

#     if not df_all_outages.empty:
#         df_all_outages['DateOnly'] = pd.to_datetime(df_all_outages['Start Time'], errors='coerce').dt.date
#         mask_range = (df_all_outages['DateOnly'] >= start_date) & (df_all_outages['DateOnly'] <= end_date)
#         df_5day = df_all_outages[mask_range].copy() 
        
#         mask_today = (df_all_outages['DateOnly'] == end_date)
#         df_today = df_all_outages[mask_today].copy()
#     else:
#         df_5day = pd.DataFrame()
#         df_today = pd.DataFrame()

#     if not df_all_ptw.empty:
#         df_all_ptw['DateOnly_Start'] = pd.to_datetime(df_all_ptw['Start Date'], dayfirst=True, errors='coerce').dt.date
#         df_all_ptw['DateOnly_Req'] = pd.to_datetime(df_all_ptw['Request Date'], dayfirst=True, errors='coerce').dt.date
#         mask_ptw = ((df_all_ptw['DateOnly_Start'] >= start_date) & (df_all_ptw['DateOnly_Start'] <= end_date)) | \
#                    ((df_all_ptw['DateOnly_Req'] >= start_date) & (df_all_ptw['DateOnly_Req'] <= end_date))
#         df_ptw = df_all_ptw[mask_ptw].copy()
#     else:
#         df_ptw = pd.DataFrame(columns=df_all_ptw.columns) if not df_all_ptw.empty else pd.DataFrame()

#     with col2:
#         st.markdown(f"<div style='text-align: right; color: #666; font-size: 0.85rem; margin-top: 4px;'>Database Synced:<br><b>{last_updated}</b></div>", unsafe_allow_html=True)


#     # --- Pre-compute Notorious Feeders (Two-Phased Logic) ---
#     if not df_all_outages.empty and 'Status' in df_all_outages.columns:
#         valid_outages = df_all_outages[~df_all_outages['Status'].astype(str).str.contains('Cancel', case=False, na=False)].copy()
#     else:
#         valid_outages = df_all_outages.copy()

#     if not valid_outages.empty:
#         valid_outages['DateOnly'] = valid_outages['Start Time'].dt.date
#         # Force numeric and prevent negative minutes from destroying sums/maxes
#         valid_outages['Diff in mins'] = pd.to_numeric(valid_outages['Diff in mins'], errors='coerce').fillna(0)
#         valid_outages['Diff in mins'] = valid_outages['Diff in mins'].apply(lambda x: max(x, 0))
        
#         notorious_feeders_list = pd.DataFrame()

#         if start_date == end_date:
#             lookback_date = end_date - pd.Timedelta(days=2)
#             mask = (valid_outages['DateOnly'] >= lookback_date) & (valid_outages['DateOnly'] <= end_date)
#             df_eval = valid_outages[mask].copy()
            
#             if not df_eval.empty:
#                 days_count = df_eval.groupby(['Circle', 'Feeder'])['DateOnly'].nunique().reset_index()
#                 notorious_feeders_list = days_count[days_count['DateOnly'] >= 3][['Circle', 'Feeder']]
#         else:
#             mask = (valid_outages['DateOnly'] >= start_date) & (valid_outages['DateOnly'] <= end_date)
#             df_eval = valid_outages[mask].copy()
            
#             if not df_eval.empty:
#                 weekly_counts = df_eval.groupby(['Circle', 'Feeder', pd.Grouper(key='Start Time', freq='W')])['DateOnly'].nunique().reset_index()
#                 notorious_feeders_list = weekly_counts[weekly_counts['DateOnly'] >= 3][['Circle', 'Feeder']].drop_duplicates()

#         if not notorious_feeders_list.empty:
#             stats = df_eval.merge(notorious_feeders_list, on=['Circle', 'Feeder'])
#             stats = stats.groupby(['Circle', 'Feeder']).agg(
#                 Total_Events=('Start Time', 'size'), 
#                 Total_Mins=('Diff in mins', 'sum'),
#                 Max_Mins=('Diff in mins', 'max') 
#             ).reset_index()
            
#             stats.rename(columns={'Total_Events': 'Total Outage Events'}, inplace=True)
#             stats['Total Duration (Hours)'] = (stats['Total_Mins'] / 60).round(2)
#             stats['Max Duration (Hours)'] = (stats['Max_Mins'] / 60).round(2) 
#             notorious = stats.drop(columns=['Total_Mins', 'Max_Mins'])
            
#             notorious = notorious.sort_values(by=['Circle', 'Total Outage Events', 'Total Duration (Hours)'], ascending=[True, False, False])
            
#             top_5_notorious = notorious.groupby('Circle').head(5)
#             notorious_set = set(zip(top_5_notorious['Circle'], top_5_notorious['Feeder']))
#         else:
#             top_5_notorious = pd.DataFrame(columns=['Circle', 'Feeder', 'Total Outage Events', 'Total Duration (Hours)', 'Max Duration (Hours)'])
#             notorious_set = set()
#     else:
#         top_5_notorious = pd.DataFrame(columns=['Circle', 'Feeder', 'Total Outage Events', 'Total Duration (Hours)', 'Max Duration (Hours)'])
#         notorious_set = set()


#     tab1, tab2, tab3 = st.tabs(["📊 Dashboard", "📈 YoY Comparison", "🛠️ PTW Frequency"])

#     # --- TAB 3: PTW FREQUENCY ---
#     with tab3:
#         st.header(f"🛠️ PTW Frequency Tracker ({preset_label})")
#         st.markdown(f"Identifies specific feeders that had a Permit to Work (PTW) taken against them **two or more times** in separate requests over the selected period.")

#         if df_ptw.empty:
#             st.info("No PTW data found for the selected period.")
#         else:
#             ptw_col = next((c for c in df_ptw.columns if 'ptw' in c.lower() or 'request' in c.lower() or 'id' in c.lower()), None)
#             feeder_col = next((c for c in df_ptw.columns if 'feeder' in c.lower()), None)
#             status_col = next((c for c in df_ptw.columns if 'status' in c.lower()), None)
#             circle_col = next((c for c in df_ptw.columns if 'circle' in c.lower()), None)

#             if not ptw_col or not feeder_col:
#                 st.error("Could not dynamically map required columns from the PTW export.")
#             else:
#                 ptw_clean = df_ptw.copy()
#                 if status_col:
#                     ptw_clean = ptw_clean[~ptw_clean[status_col].astype(str).str.contains('Cancellation', na=False, case=False)]

#                 ptw_clean[feeder_col] = ptw_clean[feeder_col].astype(str).str.replace('|', ',', regex=False)
#                 ptw_clean[feeder_col] = ptw_clean[feeder_col].str.split(',')
                
#                 ptw_clean = ptw_clean.explode(feeder_col).reset_index(drop=True)
                
#                 # Force to string before stripping to prevent float/NaN errors
#                 ptw_clean[feeder_col] = ptw_clean[feeder_col].astype(str).str.strip()
                
#                 # Filter out empty strings AND stringified 'nan's
#                 ptw_clean = ptw_clean[(ptw_clean[feeder_col] != '') & (ptw_clean[feeder_col].str.lower() != 'nan')]

#                 group_cols = [feeder_col]
#                 if circle_col: group_cols.insert(0, circle_col)
                    
#                 ptw_counts = ptw_clean.groupby(group_cols).agg(Unique_PTWs=(ptw_col, 'nunique'), PTW_IDs=(ptw_col, lambda x: ', '.join(x.dropna().astype(str).unique()))).reset_index()
#                 repeat_feeders = ptw_counts[ptw_counts['Unique_PTWs'] >= 2].sort_values(by='Unique_PTWs', ascending=False).reset_index(drop=True)
#                 repeat_feeders = repeat_feeders.rename(columns={'Unique_PTWs': 'PTW Request Count', 'PTW_IDs': 'Associated PTW Request Numbers'})

#                 kpi1, kpi2 = st.columns(2)
#                 with kpi1: st.markdown(f'<div class="kpi-card"><div><div class="kpi-title">Total Active PTW Requests</div><div class="kpi-value">{df_ptw[ptw_col].nunique()}</div></div><div class="kpi-subtext"><span class="status-badge">Selected Period</span></div></div>', unsafe_allow_html=True)
#                 with kpi2: st.markdown(f'<div class="kpi-card"><div><div class="kpi-title">Feeders with Multiple PTWs</div><div class="kpi-value">{len(repeat_feeders)}</div></div><div class="kpi-subtext"><span class="status-badge" style="background-color: #D32F2F;">🔴 Needs Review</span></div></div>', unsafe_allow_html=True)

#                 st.divider()
#                 st.subheader("⚠️ Repeat PTW Feeders Detail View")
#                 if not repeat_feeders.empty:
#                     st.dataframe(repeat_feeders.style.set_table_styles(HEADER_STYLES), width="stretch", hide_index=True)
#                 else:
#                     st.success("No feeders had multiple PTWs requested against them in the selected period! 🎉")

#                 st.divider()
#                 st.subheader("⏳ Specific Day PTW Requests (Detailed Breakdown)")
                
#                 start_col_ptw = next((c for c in df_ptw.columns if ('start' in c.lower() or 'from' in c.lower()) and ('date' in c.lower() or 'time' in c.lower())), None)
#                 end_col_ptw = next((c for c in df_ptw.columns if ('end' in c.lower() or 'to' in c.lower()) and ('date' in c.lower() or 'time' in c.lower())), None)

#                 if start_col_ptw and end_col_ptw:
#                     today_ptws = ptw_clean.copy()
#                     today_ptws[start_col_ptw] = pd.to_datetime(today_ptws[start_col_ptw], dayfirst=True, errors='coerce')
#                     today_ptws[end_col_ptw] = pd.to_datetime(today_ptws[end_col_ptw], dayfirst=True, errors='coerce')
                    
#                     req_date_col = next((c for c in df_ptw.columns if 'request' in c.lower() and ('date' in c.lower() or 'time' in c.lower())), None)
#                     if req_date_col:
#                         today_ptws[req_date_col] = pd.to_datetime(today_ptws[req_date_col], dayfirst=True, errors='coerce')
#                         mask = (today_ptws[start_col_ptw].dt.date == pd.to_datetime(end_str).date()) | \
#                                (today_ptws[req_date_col].dt.date == pd.to_datetime(end_str).date())
#                     else:
#                         mask = (today_ptws[start_col_ptw].dt.date == pd.to_datetime(end_str).date())
                    
#                     today_ptws = today_ptws[mask]
                    
#                     if not today_ptws.empty:
#                         today_ptws['Duration (Hours)'] = (today_ptws[end_col_ptw] - today_ptws[start_col_ptw]).dt.total_seconds() / 3600.0
#                         today_ptws['Duration (Hours)'] = today_ptws['Duration (Hours)'].apply(lambda x: max(x, 0)).round(2)
                        
#                         def ptw_bucket(hrs):
#                             if pd.isna(hrs): return "Unknown"
#                             if hrs <= 2: return "0-2 Hrs"
#                             elif hrs <= 4: return "2-4 Hrs"
#                             elif hrs <= 8: return "4-8 Hrs"
#                             else: return "Above 8 Hrs"
                        
#                         today_ptws['Time Bucket'] = today_ptws['Duration (Hours)'].apply(ptw_bucket)
                        
#                         display_cols_ptw = [feeder_col, start_col_ptw, end_col_ptw, 'Duration (Hours)', 'Time Bucket']
#                         if circle_col: display_cols_ptw.insert(0, circle_col)
                        
#                         final_today_ptws = today_ptws[display_cols_ptw].dropna(subset=[start_col_ptw]).sort_values(by='Duration (Hours)', ascending=False).reset_index(drop=True)
                        
#                         def highlight_long_ptw(row):
#                             if pd.notna(row['Duration (Hours)']) and row['Duration (Hours)'] > 5:
#                                 return ['background-color: rgba(220, 53, 69, 0.15); color: #850000; font-weight: bold'] * len(row)
#                             return [''] * len(row)
                            
#                         over_5_count = final_today_ptws[final_today_ptws['Duration (Hours)'] > 5][feeder_col].nunique()
#                         st.markdown(f"**Total Feeders under PTW on {end_str} exceeding 5 Hours:** `{over_5_count}`")
                        
#                         st.dataframe(final_today_ptws.style.apply(highlight_long_ptw, axis=1).format({'Duration (Hours)': '{:.2f}'}).set_table_styles(HEADER_STYLES), width="stretch", hide_index=True)
#                     else:
#                         st.info(f"No PTW requests recorded specifically for {end_str}.")
#                 else:
#                     st.warning("Could not dynamically identify Start and End time columns in the PTW report.")

#     # --- TAB 2: YOY DRILL-DOWN ---
#     with tab2:
#         st.header("📈 Historical Year-over-Year Drilldown")
        
#         # Determine YoY capability directly from the master Outages file
#         if df_all_outages.empty:
#             st.error("Master Outages Data not found.")
#         else:
#             st.markdown(f"**Comparing Period:** {start_date.strftime('%d %b %Y')} to {end_date.strftime('%d %b %Y')}")
#             st.divider()

#             def get_ly_date(d):
#                 try:
#                     return d.replace(year=d.year - 1)
#                 except ValueError:
#                     return d.replace(year=d.year - 1, day=28)

#             ly_start = get_ly_date(start_date)
#             ly_end = get_ly_date(end_date)
            
#             mask_curr = (df_all_outages['DateOnly'] >= start_date) & (df_all_outages['DateOnly'] <= end_date)
#             filtered_curr = df_all_outages[mask_curr]
            
#             mask_ly = (df_all_outages['DateOnly'] >= ly_start) & (df_all_outages['DateOnly'] <= ly_end)
#             filtered_ly = df_all_outages[mask_ly]

#             st.markdown(f"### 📍 1. Zone-wise Distribution")
#             st.caption("Includes total counts, total hours, and average hours. Click any row to drill down.")
            
#             yoy_zone = generate_yoy_dist_expanded(filtered_curr, filtered_ly, 'Zone')
            
#             zone_selection = st.dataframe(
#                 yoy_zone.style.map(highlight_delta, subset=['YoY Delta (Total)']).format(precision=2).set_table_styles(HEADER_STYLES), 
#                 width="stretch", hide_index=True, on_select="rerun", selection_mode="single-row"
#             )

#             if len(zone_selection.selection.rows) > 0:
#                 selected_zone = yoy_zone.iloc[zone_selection.selection.rows[0]]['Zone']
                
#                 st.markdown(f"### 🎯 2. Circle-wise Distribution for **{selected_zone}**")
                
#                 curr_zone_df = filtered_curr[filtered_curr['Zone'] == selected_zone]
#                 ly_zone_df = filtered_ly[filtered_ly['Zone'] == selected_zone]
                
#                 yoy_circle = generate_yoy_dist_expanded(curr_zone_df, ly_zone_df, 'Circle')
                
#                 circle_selection = st.dataframe(
#                     yoy_circle.style.map(highlight_delta, subset=['YoY Delta (Total)']).format(precision=2).set_table_styles(HEADER_STYLES), 
#                     width="stretch", hide_index=True, on_select="rerun", selection_mode="single-row"
#                 )

#                 if len(circle_selection.selection.rows) > 0:
#                     selected_circle = yoy_circle.iloc[circle_selection.selection.rows[0]]['Circle']
#                     st.markdown(f"### 🔌 3. Feeder-wise Distribution for **{selected_circle}**")
                    
#                     curr_circle_df = curr_zone_df[curr_zone_df['Circle'] == selected_circle]
#                     ly_circle_df = ly_zone_df[ly_zone_df['Circle'] == selected_circle]
                    
#                     yoy_feeder = generate_yoy_dist_expanded(curr_circle_df, ly_circle_df, 'Feeder')
#                     st.dataframe(yoy_feeder.style.map(highlight_delta, subset=['YoY Delta (Total)']).format(precision=2).set_table_styles(HEADER_STYLES), width="stretch", hide_index=True)


#     # --- TAB 1: ORIGINAL DASHBOARD ---
#     with tab1:
#         if not df_today.empty and 'Status' in df_today.columns:
#             valid_today = df_today[~df_today['Status'].astype(str).str.contains('Cancel', case=False, na=False)]
#         else:
#             valid_today = pd.DataFrame()
            
#         if not df_5day.empty and 'Status' in df_5day.columns:
#             valid_5day = df_5day[~df_5day['Status'].astype(str).str.contains('Cancel', case=False, na=False)]
#         else:
#             valid_5day = pd.DataFrame()

#         if not valid_5day.empty and 'Type of Outage' in valid_5day.columns:
#             fiveday_planned = valid_5day[valid_5day['Type of Outage'] == 'Planned Outage'] 
#             fiveday_popc = valid_5day[valid_5day['Type of Outage'] == 'Power Off By PC'] 
#             fiveday_unplanned = valid_5day[valid_5day['Type of Outage'] == 'Unplanned Outage'] 
#         else:
#             fiveday_planned = fiveday_popc = fiveday_unplanned = pd.DataFrame()

#         if start_date == end_date:
#             st.header(f"📅 Outage Summary ({pd.to_datetime(end_str).strftime('%d %b %Y')})")
#         else:
#             st.header(f"📅 Outage Summary ({pd.to_datetime(start_str).strftime('%d %b')} to {pd.to_datetime(end_str).strftime('%d %b %Y')})")
        
#         if not valid_today.empty and 'Type of Outage' in valid_today.columns:
#             today_planned = valid_today[valid_today['Type of Outage'] == 'Planned Outage'] 
#             today_popc = valid_today[valid_today['Type of Outage'] == 'Power Off By PC'] 
#             today_unplanned = valid_today[valid_today['Type of Outage'] == 'Unplanned Outage'] 
#         else:
#             today_planned = today_popc = today_unplanned = pd.DataFrame()
        
#         kpi1, kpi2, kpi3 = st.columns(3)
#         with kpi1:
#             active_p, closed_p = (len(today_planned[today_planned['Status_Calc'] == 'Active']), len(today_planned[today_planned['Status_Calc'] == 'Closed'])) if not today_planned.empty else (0,0)
#             st.markdown(f'<div class="kpi-card"><div><div class="kpi-title">Planned Outages</div><div class="kpi-value">{len(today_planned)}</div></div><div class="kpi-subtext"><span class="status-badge">🔴 Active: {active_p}</span> <span class="status-badge">🟢 Closed: {closed_p}</span></div></div>', unsafe_allow_html=True)
#         with kpi2:
#             active_po, closed_po = (len(today_popc[today_popc['Status_Calc'] == 'Active']), len(today_popc[today_popc['Status_Calc'] == 'Closed'])) if not today_popc.empty else (0,0)
#             st.markdown(f'<div class="kpi-card"><div><div class="kpi-title">Power Off By PC</div><div class="kpi-value">{len(today_popc)}</div></div><div class="kpi-subtext"><span class="status-badge">🔴 Active: {active_po}</span> <span class="status-badge">🟢 Closed: {closed_po}</span></div></div>', unsafe_allow_html=True)
#         with kpi3:
#             active_u, closed_u = (len(today_unplanned[today_unplanned['Status_Calc'] == 'Active']), len(today_unplanned[today_unplanned['Status_Calc'] == 'Closed'])) if not today_unplanned.empty else (0,0)
#             st.markdown(f'<div class="kpi-card"><div><div class="kpi-title">Unplanned Outages</div><div class="kpi-value">{len(today_unplanned)}</div></div><div class="kpi-subtext"><span class="status-badge">🔴 Active: {active_u}</span> <span class="status-badge">🟢 Closed: {closed_u}</span></div></div>', unsafe_allow_html=True)

#         st.divider()
#         st.subheader("Zone-wise Distribution")
#         if not valid_today.empty and 'Zone' in valid_today.columns and 'Type of Outage' in valid_today.columns:
#             zone_today = valid_today.groupby(['Zone', 'Type of Outage']).size().unstack(fill_value=0).reset_index()
#             for col in ['Planned Outage', 'Power Off By PC', 'Unplanned Outage']:
#                 if col not in zone_today: zone_today[col] = 0
#             zone_today['Total'] = zone_today['Planned Outage'] + zone_today['Power Off By PC'] + zone_today['Unplanned Outage']
            
#             styled_zone_today = apply_pu_gradient(zone_today.style, zone_today).set_table_styles(HEADER_STYLES)
#             st.dataframe(styled_zone_today, width="stretch", hide_index=True)
#         else: st.info(f"No data available for {end_str}.")

#         st.divider()
#         st.header(f"🚨 Top 5 Notorious Feeders (By Outage Frequency)")
        
#         if start_date == end_date:
#             st.caption("Top 5 worst-performing feeders per circle based on outages across a 3-Day Window ending today.")
#         else:
#             st.caption("Top 5 worst-performing feeders per circle based on consistent outages (3+ days per week) over the selected period.")

#         noto_col1, noto_col2 = st.columns(2)
#         with noto_col1: selected_notorious_circle = st.selectbox("Filter by Circle:", ["All Circles"] + sorted(top_5_notorious['Circle'].unique().tolist()) if not top_5_notorious.empty else ["All Circles"], index=0)
#         with noto_col2: selected_notorious_type = st.selectbox("Filter by Outage Type:", ["All Types", "Planned Outage", "Power Off By PC", "Unplanned Outage"], index=0)

#         df_dyn = valid_5day.copy()
#         if selected_notorious_type != "All Types" and not df_dyn.empty and 'Type of Outage' in df_dyn.columns: 
#             df_dyn = df_dyn[df_dyn['Type of Outage'] == selected_notorious_type]

#         if not df_dyn.empty:
#             dyn_stats = df_dyn.groupby(['Circle', 'Feeder']).agg(
#                 Total_Events=('Start Time', 'size'), 
#                 Total_Mins=('Diff in mins', 'sum'),
#                 Max_Mins=('Diff in mins', 'max') 
#             ).reset_index()
            
#             if not notorious_feeders_list.empty:
#                 dyn_noto = dyn_stats.merge(notorious_feeders_list, on=['Circle', 'Feeder']).drop_duplicates()
#             else:
#                 dyn_noto = pd.DataFrame()

#             if not dyn_noto.empty:
#                 dyn_noto.rename(columns={'Total_Events': 'Total Outage Events'}, inplace=True)
#                 dyn_noto['Total Duration (Hours)'] = (dyn_noto['Total_Mins'] / 60).round(2)
#                 dyn_noto['Max Duration (Hours)'] = (dyn_noto['Max_Mins'] / 60).round(2) 
#                 dyn_noto = dyn_noto.drop(columns=['Total_Mins', 'Max_Mins'])

#                 dyn_noto = dyn_noto.sort_values(by=['Circle', 'Total Outage Events', 'Total Duration (Hours)'], ascending=[True, False, False])
                
#                 dyn_top5 = dyn_noto.groupby('Circle').head(5)
#                 filtered_notorious = dyn_top5[dyn_top5['Circle'] == selected_notorious_circle] if selected_notorious_circle != "All Circles" else dyn_top5

#                 if not filtered_notorious.empty:
#                     st.dataframe(filtered_notorious.style.format({'Max Duration (Hours)': '{:.2f}', 'Total Duration (Hours)': '{:.2f}'}).set_table_styles(HEADER_STYLES), width="stretch", hide_index=True)
#                 else: 
#                     st.info(f"No notorious feeders found for {selected_notorious_circle} matching the criteria.")
#             else: 
#                 st.info(f"No notorious feeders identified for {selected_notorious_type}.")
#         else: 
#             st.info("No data available for the selected criteria.")

#         st.divider()
#         st.header("Comprehensive Circle-wise Breakdown")
#         bucket_order = ["Up to 2 Hrs", "2-4 Hrs", "4-8 Hrs", "Above 8 Hrs", "Active/Unknown"]

#         curr_1d_p_tab1 = create_bucket_pivot(today_planned, bucket_order)
#         curr_1d_po_tab1 = create_bucket_pivot(today_popc, bucket_order)
#         curr_1d_u_tab1 = create_bucket_pivot(today_unplanned, bucket_order)
#         curr_5d_p_tab1 = create_bucket_pivot(fiveday_planned, bucket_order)
#         curr_5d_po_tab1 = create_bucket_pivot(fiveday_popc, bucket_order)
#         curr_5d_u_tab1 = create_bucket_pivot(fiveday_unplanned, bucket_order)

#         combined_circle = pd.concat(
#             [curr_1d_p_tab1, curr_1d_po_tab1, curr_1d_u_tab1, curr_5d_p_tab1, curr_5d_po_tab1, curr_5d_u_tab1], 
#             axis=1, 
#             keys=['END DATE (Planned)', 'END DATE (Power Off)', 'END DATE (Unplanned)', 'RANGE (Planned)', 'RANGE (Power Off)', 'RANGE (Unplanned)']
#         ).fillna(0).astype(int)

#         st.markdown(" **Click on any row inside the table below** to view the specific Feeder drill-down details.")

#         if not combined_circle.empty:
#             styled_combined = apply_pu_gradient(combined_circle.style, combined_circle).set_table_styles(HEADER_STYLES)
            
#             selection_event = st.dataframe(
#                 styled_combined, 
#                 width="stretch",
#                 on_select="rerun",
#                 selection_mode="single-row" 
#             )

#             if len(selection_event.selection.rows) > 0:
#                 selected_circle = combined_circle.index[selection_event.selection.rows[0]]
#                 st.subheader(f"Feeder Details for: {selected_circle}")
                
#                 circle_dates = sorted(list(valid_5day[valid_5day['Circle'] == selected_circle]['Outage Date'].dropna().unique()))
#                 selected_dates = st.multiselect("Filter Range View by Date:", options=circle_dates, default=circle_dates, format_func=lambda x: x.strftime('%d %b %Y'))
                
#                 def highlight_notorious(row): return ['background-color: rgba(220, 53, 69, 0.15); color: #850000; font-weight: bold'] * len(row) if (selected_circle, row['Feeder']) in notorious_set else [''] * len(row)

#                 st.write("---")
#                 st.markdown(f"### 🔴 SINGLE DAY DRILLDOWN ({end_str})")
#                 today_left, today_mid, today_right = st.columns(3)
#                 with today_left:
#                     st.markdown(f"**Planned Outages**")
#                     feeder_list_tp = today_planned[today_planned['Circle'] == selected_circle][['Feeder', 'Diff in mins', 'Status_Calc', 'Duration Bucket']].rename(columns={'Status_Calc': 'Status'}) if not today_planned.empty else pd.DataFrame(columns=['Feeder', 'Diff in mins', 'Status', 'Duration Bucket'])
#                     st.dataframe(feeder_list_tp.style.apply(highlight_notorious, axis=1).set_table_styles(HEADER_STYLES), width="stretch", hide_index=True)
#                 with today_mid:
#                     st.markdown(f"**Power Off By PC**")
#                     feeder_list_tpo = today_popc[today_popc['Circle'] == selected_circle][['Feeder', 'Diff in mins', 'Status_Calc', 'Duration Bucket']].rename(columns={'Status_Calc': 'Status'}) if not today_popc.empty else pd.DataFrame(columns=['Feeder', 'Diff in mins', 'Status', 'Duration Bucket'])
#                     st.dataframe(feeder_list_tpo.style.apply(highlight_notorious, axis=1).set_table_styles(HEADER_STYLES), width="stretch", hide_index=True)
#                 with today_right:
#                     st.markdown(f"**Unplanned Outages**")
#                     feeder_list_tu = today_unplanned[today_unplanned['Circle'] == selected_circle][['Feeder', 'Diff in mins', 'Status_Calc', 'Duration Bucket']].rename(columns={'Status_Calc': 'Status'}) if not today_unplanned.empty else pd.DataFrame(columns=['Feeder', 'Diff in mins', 'Status', 'Duration Bucket'])
#                     st.dataframe(feeder_list_tu.style.apply(highlight_notorious, axis=1).set_table_styles(HEADER_STYLES), width="stretch", hide_index=True)
                    
#                 st.write("---") 
#                 st.markdown(f"### 🟢 SELECTED RANGE DRILLDOWN")
#                 fiveday_left, fiveday_mid, fiveday_right = st.columns(3)
                
#                 with fiveday_left:
#                     st.markdown(f"**Planned Outages**")
#                     feeder_list_fp = fiveday_planned[(fiveday_planned['Circle'] == selected_circle) & (fiveday_planned['Outage Date'].isin(selected_dates))].copy() if not fiveday_planned.empty else pd.DataFrame()
#                     if not feeder_list_fp.empty:
#                         feeder_list_fp['Diff in Hours'] = (feeder_list_fp['Diff in mins'] / 60).round(2)
#                         st.dataframe(feeder_list_fp[['Outage Date', 'Start Time', 'Feeder', 'Diff in Hours', 'Duration Bucket']].style.apply(highlight_notorious, axis=1).format({'Diff in Hours': '{:.2f}'}).set_table_styles(HEADER_STYLES), width="stretch", hide_index=True)
#                     else: st.dataframe(pd.DataFrame(columns=['Outage Date', 'Start Time', 'Feeder', 'Diff in Hours', 'Duration Bucket']).style.set_table_styles(HEADER_STYLES), width="stretch", hide_index=True)
                    
#                 with fiveday_mid:
#                     st.markdown(f"**Power Off By PC**")
#                     feeder_list_fpo = fiveday_popc[(fiveday_popc['Circle'] == selected_circle) & (fiveday_popc['Outage Date'].isin(selected_dates))].copy() if not fiveday_popc.empty else pd.DataFrame()
#                     if not feeder_list_fpo.empty:
#                         feeder_list_fpo['Diff in Hours'] = (feeder_list_fpo['Diff in mins'] / 60).round(2)
#                         st.dataframe(feeder_list_fpo[['Outage Date', 'Start Time', 'Feeder', 'Diff in Hours', 'Duration Bucket']].style.apply(highlight_notorious, axis=1).format({'Diff in Hours': '{:.2f}'}).set_table_styles(HEADER_STYLES), width="stretch", hide_index=True)
#                     else: st.dataframe(pd.DataFrame(columns=['Outage Date', 'Start Time', 'Feeder', 'Diff in Hours', 'Duration Bucket']).style.set_table_styles(HEADER_STYLES), width="stretch", hide_index=True)

#                 with fiveday_right:
#                     st.markdown(f"**Unplanned Outages**")
#                     feeder_list_fu = fiveday_unplanned[(fiveday_unplanned['Circle'] == selected_circle) & (fiveday_unplanned['Outage Date'].isin(selected_dates))].copy() if not fiveday_unplanned.empty else pd.DataFrame()
#                     if not feeder_list_fu.empty:
#                         feeder_list_fu['Diff in Hours'] = (feeder_list_fu['Diff in mins'] / 60).round(2)
#                         st.dataframe(feeder_list_fu[['Outage Date', 'Start Time', 'Feeder', 'Diff in Hours', 'Duration Bucket']].style.apply(highlight_notorious, axis=1).format({'Diff in Hours': '{:.2f}'}).set_table_styles(HEADER_STYLES), width="stretch", hide_index=True)
#                     else: st.dataframe(pd.DataFrame(columns=['Outage Date', 'Start Time', 'Feeder', 'Diff in Hours', 'Duration Bucket']).style.set_table_styles(HEADER_STYLES), width="stretch", hide_index=True)
#         else: st.info("No circle data available.")

# # --- ROUTER LOGIC ---
# if st.session_state.page == 'home':
#     render_home()
# elif st.session_state.page == 'dashboard':
#     render_dashboard()
# elif st.session_state.page == 'ptw_app':
#     render_ptw_lm_dashboard()

# # =========================================================================================================================
# # V2
# # =========================================================================================================================
# import os
# import requests
# import streamlit as st
# import pandas as pd
# from datetime import datetime, timedelta, timezone
# from dateutil.relativedelta import relativedelta
# from ptw_lm_app import render_ptw_lm_dashboard

# # --- PAGE CONFIGURATION ---
# st.set_page_config(page_title="Utility Operations Command Center", layout="wide")

# # --- INITIALIZE SESSION STATE FOR NAVIGATION & DATES ---
# if 'page' not in st.session_state:
#     st.session_state.page = 'home'

# today_init = pd.to_datetime("today").date()
# if 'date_preset' not in st.session_state:
#     st.session_state.date_preset = "Today"
# if 'start_date' not in st.session_state:
#     st.session_state.start_date = today_init
# if 'end_date' not in st.session_state:
#     st.session_state.end_date = today_init

# # --- GLOBAL TABLE HEADER STYLING ---
# HEADER_STYLES = [
#     {
#         'selector': 'th',
#         'props': [
#             ('background-color', '#004085 !important'),
#             ('color', '#FFC107 !important'),
#             ('font-weight', 'bold !important'),
#             ('text-align', 'center !important')
#         ]
#     },
#     {
#         'selector': 'th div',
#         'props': [
#             ('color', '#FFC107 !important'),
#             ('font-weight', 'bold !important')
#         ]
#     }
# ]

# # --- API & FILE CONSTANTS ---
# OUTAGE_URL = "https://distribution.pspcl.in/returns/module.php?to=OutageAPI.getOutages"
# PTW_URL = "https://distribution.pspcl.in/returns/module.php?to=OutageAPI.getPTWRequests"
# OUTAGE_FILE = "Historical_Outages_Consolidated_Report.xlsx"
# PTW_FILE = "PTW_Report.xlsx"

# # --- IST TIMEZONE SETUP ---
# IST = timezone(timedelta(hours=5, minutes=30))

# # --- API FETCH FUNCTIONS ---
# def fetch_from_api(url, payload):
#     try:
#         res = requests.post(url, json=payload, headers={'Content-Type': 'application/json'}, timeout=60, verify=True)
#         res.raise_for_status()
#         data = res.json()
#         return data if isinstance(data, list) else data.get("data", [])
#     except Exception as e:
#         print(f"API Fetch Error for {payload['fromdate']} to {payload['todate']}: {e}")
#         return []

# def fetch_outages_chunked(start_str, end_str, api_key):
#     """Chunks the outage requests month-by-month to bypass server timeouts."""
#     start = datetime.strptime(start_str, "%Y-%m-%d")
#     end = datetime.strptime(end_str, "%Y-%m-%d")
#     all_data = []
    
#     while start <= end:
#         month_end = start + relativedelta(months=1) - timedelta(days=1)
#         if month_end > end:
#             month_end = end
            
#         chunk = fetch_from_api(OUTAGE_URL, {"fromdate": start.strftime("%Y-%m-%d"), "todate": month_end.strftime("%Y-%m-%d"), "apikey": api_key})
#         if chunk:
#             all_data.extend(chunk)
            
#         start += relativedelta(months=1)
#     return all_data

# # --- CORE DATA PIPELINE (Reads, Fetches, Appends to Excel) ---
# @st.cache_data(ttl=900, show_spinner="Syncing local files with PSPCL API...")
# def load_data_pipeline():
#     api_key = st.secrets["API_KEY"]
#     now_ist = datetime.now(IST)
#     end_date_str = now_ist.strftime("%Y-%m-%d")

#     # ==========================================
#     # 1. OUTAGES LOGIC
#     # ==========================================
#     if os.path.exists(OUTAGE_FILE):
#         df_outages = pd.read_excel(OUTAGE_FILE)
#     else:
#         df_outages = pd.DataFrame()

#     # Find the last date to append from
#     if not df_outages.empty and 'Start Time' in df_outages.columns:
#         max_outage_date = pd.to_datetime(df_outages['Start Time'], errors='coerce').max()
#         if pd.isna(max_outage_date):
#             outage_start_str = (now_ist - timedelta(days=180)).strftime("%Y-%m-%d")
#         else:
#             # We subtract 1 day to catch any late status updates for yesterday's records
#             outage_start_str = (max_outage_date - timedelta(days=1)).strftime("%Y-%m-%d")
#     else:
#         outage_start_str = (now_ist - timedelta(days=180)).strftime("%Y-%m-%d")

#     # Fetch and Append New Outages
#     new_outages = fetch_outages_chunked(outage_start_str, end_date_str, api_key)
#     if new_outages:
#         df_new_outages = pd.DataFrame(new_outages)
#         df_new_outages.rename(columns={
#             "zone_name": "Zone", "circle_name": "Circle", "feeder_name": "Feeder", 
#             "outage_type": "Type of Outage", "outage_status": "Status", 
#             "start_time": "Start Time", "end_time": "End Time", "duration_minutes": "Diff in mins"
#         }, inplace=True)
        
#         df_outages = pd.concat([df_outages, df_new_outages])
        
#         # Deduplicate ONLY if it is an exact match across all these specific fields
#         df_outages = df_outages.drop_duplicates(
#             subset=['Circle', 'Feeder', 'Start Time', 'Type of Outage', 'Status'], 
#             keep='last'
#         )
#         # Save back to local file
#         df_outages.to_excel(OUTAGE_FILE, index=False)

#     # Standardize Outages for Dashboard Consumption
#     if not df_outages.empty:
#         if 'Type of Outage' in df_outages.columns:
#             df_outages['Raw Outage Type'] = df_outages['Type of Outage'].astype(str).str.strip()
#             def standardize_outage(val):
#                 v_lower = str(val).lower()
#                 if 'power off' in v_lower: return 'Power Off By PC'
#                 if 'unplanned' in v_lower: return 'Unplanned Outage'
#                 if 'planned' in v_lower: return 'Planned Outage'
#                 return val
#             df_outages['Type of Outage'] = df_outages['Raw Outage Type'].apply(standardize_outage)

#         df_outages['Start Time'] = pd.to_datetime(df_outages['Start Time'], errors='coerce')
        
#         if 'Diff in mins' in df_outages.columns:
#             df_outages['Diff in mins'] = pd.to_numeric(df_outages['Diff in mins'], errors='coerce')
#             def assign_bucket(mins):
#                 if pd.isna(mins) or mins < 0: return "Active/Unknown"
#                 hrs = mins / 60
#                 if hrs <= 2: return "Up to 2 Hrs"
#                 elif hrs <= 4: return "2-4 Hrs"
#                 elif hrs <= 8: return "4-8 Hrs"
#                 else: return "Above 8 Hrs"
#             df_outages['Duration Bucket'] = df_outages['Diff in mins'].apply(assign_bucket)
            
#         if 'Status' in df_outages.columns:
#             df_outages['Status_Calc'] = df_outages['Status'].apply(lambda x: 'Active' if str(x).strip().title() in ['Active', 'Open'] else 'Closed')


#     # ==========================================
#     # 2. PTW LOGIC
#     # ==========================================
#     if os.path.exists(PTW_FILE):
#         df_ptw = pd.read_excel(PTW_FILE)
#     else:
#         df_ptw = pd.DataFrame()

#     # Find the last date to append from
#     if not df_ptw.empty:
#         date_col = 'Request Date' if 'Request Date' in df_ptw.columns else 'creation_date' if 'creation_date' in df_ptw.columns else None
#         if date_col:
#             max_ptw_date = pd.to_datetime(df_ptw[date_col], errors='coerce').max()
#             if pd.isna(max_ptw_date):
#                 ptw_start_str = (now_ist - timedelta(days=180)).strftime("%Y-%m-%d")
#             else:
#                 ptw_start_str = (max_ptw_date - timedelta(days=1)).strftime("%Y-%m-%d")
#         else:
#             ptw_start_str = (now_ist - timedelta(days=180)).strftime("%Y-%m-%d")
#     else:
#         ptw_start_str = (now_ist - timedelta(days=180)).strftime("%Y-%m-%d")

#     # Fetch and Append New PTWs
#     new_ptw = fetch_from_api(PTW_URL, {"fromdate": ptw_start_str, "todate": end_date_str, "apikey": api_key})
#     if new_ptw:
#         df_new_ptw = pd.DataFrame(new_ptw)
#         if 'feeders' in df_new_ptw.columns:
#             df_new_ptw['feeders'] = df_new_ptw['feeders'].apply(lambda x: ', '.join(map(str, x)) if isinstance(x, list) else str(x))
#         df_new_ptw.rename(columns={
#             "ptw_id": "PTW Request ID", "permit_no": "Permit Number", 
#             "circle_name": "Circle", "feeders": "Feeder", "current_status": "Status", 
#             "start_time": "Start Date", "end_time": "End Date", "creation_date": "Request Date"
#         }, inplace=True)
        
#         df_ptw = pd.concat([df_ptw, df_new_ptw])
#         if 'PTW Request ID' in df_ptw.columns:
#             df_ptw = df_ptw.drop_duplicates(subset=['PTW Request ID'], keep='last')
        
#         # Save back to local file
#         df_ptw.to_excel(PTW_FILE, index=False)

#     fetch_time = now_ist.strftime('%d %b %Y, %I:%M %p')
#     return df_outages, df_ptw, fetch_time


# # --- HELPER FUNCTIONS ---
# def generate_yoy_dist_expanded(df_curr, df_ly, group_col):
#     def _agg(df, prefix):
#         if df.empty: return pd.DataFrame({group_col: []}).set_index(group_col)
#         df['Diff in mins'] = pd.to_numeric(df['Diff in mins'], errors='coerce').fillna(0)
#         g = df.groupby([group_col, 'Type of Outage']).agg(Count=('Type of Outage', 'size'), TotalHrs=('Diff in mins', lambda x: round(x.sum() / 60, 2)), AvgHrs=('Diff in mins', lambda x: round(x.mean() / 60, 2))).unstack(fill_value=0)
#         g.columns = [f"{prefix} {outage} ({metric})" for metric, outage in g.columns]
#         return g

#     c_grp = _agg(df_curr, 'Curr')
#     l_grp = _agg(df_ly, 'LY')
#     merged = pd.merge(c_grp, l_grp, on=group_col, how='outer').fillna(0).reset_index()
    
#     expected_cols = []
#     for prefix in ['Curr', 'LY']:
#         for outage in ['Planned Outage', 'Power Off By PC', 'Unplanned Outage']:
#             for metric in ['Count', 'TotalHrs', 'AvgHrs']:
#                 col_name = f"{prefix} {outage} ({metric})"
#                 expected_cols.append(col_name)
#                 if col_name not in merged.columns: merged[col_name] = 0
                    
#     for col in expected_cols:
#         if '(Count)' in col: merged[col] = merged[col].astype(int)
#         else: merged[col] = merged[col].astype(float).round(2)
            
#     merged['Curr Total (Count)'] = merged['Curr Planned Outage (Count)'] + merged['Curr Power Off By PC (Count)'] + merged['Curr Unplanned Outage (Count)']
#     merged['LY Total (Count)'] = merged['LY Planned Outage (Count)'] + merged['LY Power Off By PC (Count)'] + merged['LY Unplanned Outage (Count)']
#     merged['YoY Delta (Total)'] = merged['Curr Total (Count)'] - merged['LY Total (Count)']
    
#     cols_order = [group_col, 
#                   'Curr Planned Outage (Count)', 'Curr Planned Outage (TotalHrs)', 'Curr Planned Outage (AvgHrs)', 
#                   'LY Planned Outage (Count)', 'LY Planned Outage (TotalHrs)', 'LY Planned Outage (AvgHrs)', 
#                   'Curr Power Off By PC (Count)', 'Curr Power Off By PC (TotalHrs)', 'Curr Power Off By PC (AvgHrs)', 
#                   'LY Power Off By PC (Count)', 'LY Power Off By PC (TotalHrs)', 'LY Power Off By PC (AvgHrs)', 
#                   'Curr Unplanned Outage (Count)', 'Curr Unplanned Outage (TotalHrs)', 'Curr Unplanned Outage (AvgHrs)', 
#                   'LY Unplanned Outage (Count)', 'LY Unplanned Outage (TotalHrs)', 'LY Unplanned Outage (AvgHrs)', 
#                   'Curr Total (Count)', 'LY Total (Count)', 'YoY Delta (Total)']
#     return merged[cols_order]

# def apply_pu_gradient(styler, df):
#     p_cols = [c for c in df.columns if 'Planned' in str(c) and pd.api.types.is_numeric_dtype(df[c])]
#     u_cols = [c for c in df.columns if 'Unplanned' in str(c) and pd.api.types.is_numeric_dtype(df[c])]
#     po_cols = [c for c in df.columns if 'Power Off' in str(c) and pd.api.types.is_numeric_dtype(df[c])]
    
#     if p_cols: styler = styler.background_gradient(subset=p_cols, cmap='Blues', vmin=0)
#     if u_cols: styler = styler.background_gradient(subset=u_cols, cmap='Reds', vmin=0)
#     if po_cols: styler = styler.background_gradient(subset=po_cols, cmap='Greens', vmin=0)
#     return styler

# def highlight_delta(val):
#     if isinstance(val, int):
#         if val > 0: return 'color: #D32F2F; font-weight: bold;'
#         elif val < 0: return 'color: #388E3C; font-weight: bold;'
#     return ''

# def create_bucket_pivot(df, bucket_order):
#     if df.empty or 'Duration Bucket' not in df.columns or 'Circle' not in df.columns: 
#         return pd.DataFrame(columns=bucket_order + ['Total'])
#     pivot = pd.crosstab(df['Circle'], df['Duration Bucket'])
#     pivot = pivot.reindex(columns=[c for c in bucket_order if c in pivot.columns], fill_value=0)
#     pivot['Total'] = pivot.sum(axis=1)
#     return pivot


# # ==========================================
# # PAGE 1: HOME (COMMAND CENTER)
# # ==========================================
# def render_home():
#     st.markdown("""
#         <style>
#             .home-title {
#                 text-align: center;
#                 color: #004085;
#                 font-weight: 700;
#                 font-size: 2.5rem;
#                 margin-top: 2rem;
#                 margin-bottom: 0.5rem;
#                 font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif;
#             }
#             .home-subtitle {
#                 text-align: center;
#                 color: #555555;
#                 font-size: 1.1rem;
#                 margin-bottom: 3rem;
#             }
#             div.stButton > button {
#                 height: 90px;
#                 font-size: 1.1rem;
#                 font-weight: 600;
#                 background-color: #ffffff;
#                 color: #333333;
#                 border: 1px solid #e0e0e0;
#                 border-radius: 8px;
#                 box-shadow: 0 4px 6px rgba(0,0,0,0.05);
#                 transition: all 0.3s ease;
#             }
#             div.stButton > button:hover {
#                 border-color: #004085;
#                 box-shadow: 0 6px 12px rgba(0,0,0,0.15);
#                 color: #004085;
#                 transform: translateY(-2px);
#             }
#         </style>
#     """, unsafe_allow_html=True)

#     st.markdown("<div class='home-title'>⚡ Utility Operations Command Center</div>", unsafe_allow_html=True)
#     st.markdown("<div class='home-subtitle'>Select an operational module below to access real-time dashboards and management tools.</div>", unsafe_allow_html=True)
    
#     st.write("---")
    
#     st.write("")
#     row1_col1, row1_col2, row1_col3 = st.columns(3, gap="large")
    
#     with row1_col1:
#         if st.button("🛠️ PTW, LM-ALM Application", use_container_width=True):
#             st.session_state.page = 'ptw_app'
#             st.rerun()
            
#     with row1_col2:
#         if st.button("📉 Outage Reduction Plan (ORP)", use_container_width=True):
#             st.toast("This module is currently offline or under development.")
            
#     with row1_col3:
#         if st.button("🏢 RDSS", use_container_width=True):
#             st.toast("This module is currently offline or under development.")

#     st.write("")
#     row2_col1, row2_col2, row2_col3 = st.columns(3, gap="large")
    
#     with row2_col1:
#         if st.button("📡 Smart Meter", use_container_width=True):
#             st.toast("This module is currently offline or under development.")
            
#     with row2_col2:
#         if st.button("🔌 New Connections", use_container_width=True):
#             st.toast("This module is currently offline or under development.")
            
#     with row2_col3:
#         if st.button("🚨 Outage Monitoring", use_container_width=True):
#             st.session_state.page = 'dashboard'
#             st.rerun()

# # ==========================================
# # PAGE 2: MAIN DASHBOARD
# # ==========================================
# def render_dashboard():
#     # Apply Dashboard styling
#     st.markdown("""
#         <style>
#             .block-container { padding-top: 1.5rem; padding-bottom: 1.5rem; font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif; }
#             p, span, div, caption, .stMarkdown { color: #000000 !important; }
#             h1, h2, h3, h4, h5, h6, div.block-container h1 { color: #004085 !important; font-weight: 700 !important; font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif; }
#             div.block-container h1 { text-align: center; border-bottom: 3px solid #004085 !important; padding-bottom: 10px; margin-bottom: 30px !important; font-size: 2.2rem !important; }
#             h2 { font-size: 1.3rem !important; border-bottom: 2px solid #004085 !important; padding-bottom: 5px; margin-bottom: 10px !important; }
#             h3 { font-size: 1.05rem !important; margin-bottom: 12px !important; text-transform: uppercase; letter-spacing: 0.5px; }
#             hr { border: 0; border-top: 1px solid #004085; margin: 1.5rem 0; opacity: 0.3; }
            
#             .kpi-card { background: linear-gradient(135deg, #004481 0%, #0066cc 100%); border-radius: 6px; padding: 1.2rem 1.2rem; display: flex; flex-direction: column; justify-content: space-between; height: 100%; box-shadow: 0 2px 4px rgba(0,0,0,0.08); transition: transform 0.2s ease-in-out, box-shadow 0.2s ease-in-out; border: 1px solid #003366; }
#             .kpi-card:hover { transform: translateY(-4px); box-shadow: 0 8px 16px rgba(0, 68, 129, 0.2); }
#             .kpi-card .kpi-title, .kpi-title { color: #FFC107 !important; font-weight: 600; font-size: 0.85rem; text-transform: uppercase; letter-spacing: 0.5px; margin-bottom: 0.4rem; }
#             .kpi-card .kpi-value, .kpi-value { color: #FFFFFF !important; font-weight: 700; font-size: 2.6rem; margin-bottom: 0; line-height: 1.1; }
#             .kpi-card .kpi-subtext, .kpi-subtext { color: #F8F9FA !important; font-size: 0.85rem; margin-top: 1rem; padding-top: 0.6rem; border-top: 1px solid rgba(255, 255, 255, 0.2); display: flex; justify-content: flex-start; gap: 15px; }
            
#             .status-badge { background-color: rgba(0, 0, 0, 0.25); padding: 3px 8px; border-radius: 4px; font-weight: 500; color: #FFFFFF !important; }
#             [data-testid="stDataFrame"] > div { border: 2px solid #004085 !important; border-radius: 6px; overflow: hidden; }
#         </style>
#     """, unsafe_allow_html=True)

#     col1, col2 = st.columns([0.75, 0.25])
#     with col1:
#         st.title("⚡ Power Outage Monitoring Dashboard")
#     with col2:
#         st.write("")
#         btn_col1, btn_col2 = st.columns(2)
#         with btn_col1:
#             if st.button("⬅️ Home", use_container_width=True):
#                 st.session_state.page = 'home'
#                 st.rerun()
#         with btn_col2:
#             with st.popover("🔄 Refresh", use_container_width=True):
#                 st.markdown("**Admin Access Required**")
#                 pwd = st.text_input("Passcode:", type="password", placeholder="Enter passcode...")
#                 if st.button("Confirm Refresh", use_container_width=True):
#                     if pwd == "J@Y":
#                         st.cache_data.clear()
#                         st.rerun()
#                     else:
#                         st.error("Incorrect password.")

#     # --- INITIATE GLOBAL DATA PIPELINE ---
#     df_all_outages, df_all_ptw, last_updated = load_data_pipeline()
    
#     with col2:
#         st.markdown(f"<div style='text-align: right; color: #666; font-size: 0.85rem; margin-top: 4px;'>Database Synced:<br><b>{last_updated}</b></div>", unsafe_allow_html=True)

#     # --- HELPER FOR INDEPENDENT DATE PRESETS ---
#     def get_preset_dates(preset):
#         t = pd.to_datetime("today").date()
#         if preset == "Today": return t, t
#         elif preset == "Current Month": return t.replace(day=1), t
#         elif preset == "Last Month": 
#             e = t.replace(day=1) - pd.Timedelta(days=1)
#             return e.replace(day=1), e
#         elif preset == "Last 3 Months": return (t - pd.DateOffset(months=3)).date(), t
#         elif preset == "Last 6 Months": return (t - pd.DateOffset(months=6)).date(), t
#         return t, t

#     # --- CALLBACKS TO FORCE UI UPDATE ---
#     def update_dates_t1():
#         st.session_state.start_t1, st.session_state.end_t1 = get_preset_dates(st.session_state.preset_t1)

#     def update_dates_t2():
#         st.session_state.start_t2, st.session_state.end_t2 = get_preset_dates(st.session_state.preset_t2)

#     def update_dates_t3():
#         st.session_state.start_t3, st.session_state.end_t3 = get_preset_dates(st.session_state.preset_t3)

#     # Initialize states on first load if they don't exist
#     if "preset_t1" not in st.session_state: st.session_state.preset_t1 = "Today"
#     if "preset_t2" not in st.session_state: st.session_state.preset_t2 = "Today"
#     if "preset_t3" not in st.session_state: st.session_state.preset_t3 = "Today"
    
#     if "start_t1" not in st.session_state: st.session_state.start_t1, st.session_state.end_t1 = get_preset_dates("Today")
#     if "start_t2" not in st.session_state: st.session_state.start_t2, st.session_state.end_t2 = get_preset_dates("Today")
#     if "start_t3" not in st.session_state: st.session_state.start_t3, st.session_state.end_t3 = get_preset_dates("Today")

#     tab1, tab2, tab3 = st.tabs(["📊 Dashboard", "📈 YoY Comparison", "🛠️ PTW Frequency"])

#     # ==========================================
#     # TAB 1: ORIGINAL DASHBOARD
#     # ==========================================
#     with tab1:
#         st.radio("📅 Select Time Period:", ["Today", "Current Month", "Last Month", "Last 3 Months", "Last 6 Months"], key="preset_t1", horizontal=True, on_change=update_dates_t1)
        
#         c1, c2 = st.columns(2)
#         start_date_1 = c1.date_input("From Date", key="start_t1")
#         end_date_1 = c2.date_input("To Date", key="end_t1")
#         end_str_1 = end_date_1.strftime("%Y-%m-%d")

#         # Filtering logic for Tab 1
#         if not df_all_outages.empty:
#             df_all_outages['DateOnly'] = pd.to_datetime(df_all_outages['Start Time'], errors='coerce').dt.date
#             df_5day = df_all_outages[(df_all_outages['DateOnly'] >= start_date_1) & (df_all_outages['DateOnly'] <= end_date_1)].copy() 
#             df_today = df_all_outages[df_all_outages['DateOnly'] == end_date_1].copy()
#         else:
#             df_5day = pd.DataFrame()
#             df_today = pd.DataFrame()

#         if not df_5day.empty and 'Status' in df_5day.columns:
#             valid_5day = df_5day[~df_5day['Status'].astype(str).str.contains('Cancel', case=False, na=False)]
#         else:
#             valid_5day = pd.DataFrame()

#         if not df_today.empty and 'Status' in df_today.columns:
#             valid_today = df_today[~df_today['Status'].astype(str).str.contains('Cancel', case=False, na=False)]
#         else:
#             valid_today = pd.DataFrame()

#         if not valid_5day.empty and 'Type of Outage' in valid_5day.columns:
#             fiveday_planned = valid_5day[valid_5day['Type of Outage'] == 'Planned Outage'] 
#             fiveday_popc = valid_5day[valid_5day['Type of Outage'] == 'Power Off By PC'] 
#             fiveday_unplanned = valid_5day[valid_5day['Type of Outage'] == 'Unplanned Outage'] 
#         else:
#             fiveday_planned = fiveday_popc = fiveday_unplanned = pd.DataFrame()

#         if start_date_1 == end_date_1:
#             st.header(f"📅 Outage Summary ({end_date_1.strftime('%d %b %Y')})")
#         else:
#             st.header(f"📅 Outage Summary ({start_date_1.strftime('%d %b')} to {end_date_1.strftime('%d %b %Y')})")
        
#         kpi1, kpi2, kpi3 = st.columns(3)
#         with kpi1:
#             active_p = len(fiveday_planned[fiveday_planned['Status_Calc'] == 'Active']) if not fiveday_planned.empty else 0
#             closed_p = len(fiveday_planned[fiveday_planned['Status_Calc'] == 'Closed']) if not fiveday_planned.empty else 0
#             st.markdown(f'<div class="kpi-card"><div><div class="kpi-title">Planned Outages</div><div class="kpi-value">{len(fiveday_planned)}</div></div><div class="kpi-subtext"><span class="status-badge">🔴 Active: {active_p}</span> <span class="status-badge">🟢 Closed: {closed_p}</span></div></div>', unsafe_allow_html=True)
#         with kpi2:
#             active_po = len(fiveday_popc[fiveday_popc['Status_Calc'] == 'Active']) if not fiveday_popc.empty else 0
#             closed_po = len(fiveday_popc[fiveday_popc['Status_Calc'] == 'Closed']) if not fiveday_popc.empty else 0
#             st.markdown(f'<div class="kpi-card"><div><div class="kpi-title">Power Off By PC</div><div class="kpi-value">{len(fiveday_popc)}</div></div><div class="kpi-subtext"><span class="status-badge">🔴 Active: {active_po}</span> <span class="status-badge">🟢 Closed: {closed_po}</span></div></div>', unsafe_allow_html=True)
#         with kpi3:
#             active_u = len(fiveday_unplanned[fiveday_unplanned['Status_Calc'] == 'Active']) if not fiveday_unplanned.empty else 0
#             closed_u = len(fiveday_unplanned[fiveday_unplanned['Status_Calc'] == 'Closed']) if not fiveday_unplanned.empty else 0
#             st.markdown(f'<div class="kpi-card"><div><div class="kpi-title">Unplanned Outages</div><div class="kpi-value">{len(fiveday_unplanned)}</div></div><div class="kpi-subtext"><span class="status-badge">🔴 Active: {active_u}</span> <span class="status-badge">🟢 Closed: {closed_u}</span></div></div>', unsafe_allow_html=True)

#         st.divider()
#         st.subheader("Zone-wise Distribution (Selected Range)")
#         if not valid_5day.empty and 'Zone' in valid_5day.columns and 'Type of Outage' in valid_5day.columns:
#             zone_range = valid_5day.groupby(['Zone', 'Type of Outage']).size().unstack(fill_value=0).reset_index()
#             for col in ['Planned Outage', 'Power Off By PC', 'Unplanned Outage']:
#                 if col not in zone_range: zone_range[col] = 0
#             zone_range['Total'] = zone_range['Planned Outage'] + zone_range['Power Off By PC'] + zone_range['Unplanned Outage']
            
#             styled_zone_range = apply_pu_gradient(zone_range.style, zone_range).set_table_styles(HEADER_STYLES)
#             st.dataframe(styled_zone_range, width="stretch", hide_index=True)
#         else: st.info(f"No data available for selected dates.")

#         st.divider()
#         st.header(f"🚨 Top 5 Notorious Feeders (By Outage Frequency)")
        
#         # Notorious Logic mapped to Tab 1 dates
#         notorious_feeders_list = pd.DataFrame()
#         top_5_notorious = pd.DataFrame(columns=['Circle', 'Feeder', 'Total Outage Events', 'Total Duration (Hours)', 'Max Duration (Hours)'])
#         notorious_set = set()
        
#         if not valid_5day.empty:
#             valid_5day['Diff in mins'] = pd.to_numeric(valid_5day['Diff in mins'], errors='coerce').fillna(0)
#             valid_5day['Diff in mins'] = valid_5day['Diff in mins'].apply(lambda x: max(x, 0))
#             if start_date_1 == end_date_1:
#                 lookback_date = end_date_1 - pd.Timedelta(days=2)
#                 mask = (df_all_outages['DateOnly'] >= lookback_date) & (df_all_outages['DateOnly'] <= end_date_1) & (~df_all_outages['Status'].astype(str).str.contains('Cancel', case=False, na=False))
#                 df_eval = df_all_outages[mask].copy()
#                 if not df_eval.empty:
#                     days_count = df_eval.groupby(['Circle', 'Feeder'])['DateOnly'].nunique().reset_index()
#                     notorious_feeders_list = days_count[days_count['DateOnly'] >= 3][['Circle', 'Feeder']]
#             else:
#                 df_eval = valid_5day.copy()
#                 weekly_counts = df_eval.groupby(['Circle', 'Feeder', pd.Grouper(key='Start Time', freq='W')])['DateOnly'].nunique().reset_index()
#                 notorious_feeders_list = weekly_counts[weekly_counts['DateOnly'] >= 3][['Circle', 'Feeder']].drop_duplicates()

#             if not notorious_feeders_list.empty:
#                 stats = df_eval.merge(notorious_feeders_list, on=['Circle', 'Feeder'])
#                 stats = stats.groupby(['Circle', 'Feeder']).agg(
#                     Total_Events=('Start Time', 'size'), Total_Mins=('Diff in mins', 'sum'), Max_Mins=('Diff in mins', 'max') 
#                 ).reset_index()
#                 stats.rename(columns={'Total_Events': 'Total Outage Events'}, inplace=True)
#                 stats['Total Duration (Hours)'] = (stats['Total_Mins'] / 60).round(2)
#                 stats['Max Duration (Hours)'] = (stats['Max_Mins'] / 60).round(2) 
#                 notorious = stats.drop(columns=['Total_Mins', 'Max_Mins']).sort_values(by=['Circle', 'Total Outage Events', 'Total Duration (Hours)'], ascending=[True, False, False])
#                 top_5_notorious = notorious.groupby('Circle').head(5)
#                 notorious_set = set(zip(top_5_notorious['Circle'], top_5_notorious['Feeder']))

#         noto_col1, noto_col2 = st.columns(2)
#         with noto_col1: selected_notorious_circle = st.selectbox("Filter by Circle:", ["All Circles"] + sorted(top_5_notorious['Circle'].unique().tolist()) if not top_5_notorious.empty else ["All Circles"], index=0, key="noto_circ_1")
#         with noto_col2: selected_notorious_type = st.selectbox("Filter by Outage Type:", ["All Types", "Planned Outage", "Power Off By PC", "Unplanned Outage"], index=0, key="noto_type_1")

#         df_dyn = valid_5day.copy()
#         if selected_notorious_type != "All Types" and not df_dyn.empty and 'Type of Outage' in df_dyn.columns: 
#             df_dyn = df_dyn[df_dyn['Type of Outage'] == selected_notorious_type]

#         if not df_dyn.empty:
#             dyn_stats = df_dyn.groupby(['Circle', 'Feeder']).agg(Total_Events=('Start Time', 'size'), Total_Mins=('Diff in mins', 'sum'), Max_Mins=('Diff in mins', 'max')).reset_index()
#             if not notorious_feeders_list.empty:
#                 dyn_noto = dyn_stats.merge(notorious_feeders_list, on=['Circle', 'Feeder']).drop_duplicates()
#             else:
#                 dyn_noto = pd.DataFrame()

#             if not dyn_noto.empty:
#                 dyn_noto.rename(columns={'Total_Events': 'Total Outage Events'}, inplace=True)
#                 dyn_noto['Total Duration (Hours)'] = (dyn_noto['Total_Mins'] / 60).round(2)
#                 dyn_noto['Max Duration (Hours)'] = (dyn_noto['Max_Mins'] / 60).round(2) 
#                 dyn_noto = dyn_noto.drop(columns=['Total_Mins', 'Max_Mins']).sort_values(by=['Circle', 'Total Outage Events', 'Total Duration (Hours)'], ascending=[True, False, False])
#                 dyn_top5 = dyn_noto.groupby('Circle').head(5)
#                 filtered_notorious = dyn_top5[dyn_top5['Circle'] == selected_notorious_circle] if selected_notorious_circle != "All Circles" else dyn_top5

#                 if not filtered_notorious.empty:
#                     st.dataframe(filtered_notorious.style.format({'Max Duration (Hours)': '{:.2f}', 'Total Duration (Hours)': '{:.2f}'}).set_table_styles(HEADER_STYLES), width="stretch", hide_index=True)
#                 else: st.info(f"No notorious feeders found.")
#             else: st.info(f"No notorious feeders identified.")
#         else: st.info("No data available.")

#         st.divider()
#         st.header("Comprehensive Circle-wise Breakdown")
#         bucket_order = ["Up to 2 Hrs", "2-4 Hrs", "4-8 Hrs", "Above 8 Hrs", "Active/Unknown"]

#         # Uses safely filtered valid_today (end_date_1) and valid_5day (range)
#         today_p = valid_today[valid_today['Type of Outage'] == 'Planned Outage'] if 'Type of Outage' in valid_today.columns else pd.DataFrame()
#         today_po = valid_today[valid_today['Type of Outage'] == 'Power Off By PC'] if 'Type of Outage' in valid_today.columns else pd.DataFrame()
#         today_u = valid_today[valid_today['Type of Outage'] == 'Unplanned Outage'] if 'Type of Outage' in valid_today.columns else pd.DataFrame()

#         curr_1d_p_tab1 = create_bucket_pivot(today_p, bucket_order)
#         curr_1d_po_tab1 = create_bucket_pivot(today_po, bucket_order)
#         curr_1d_u_tab1 = create_bucket_pivot(today_u, bucket_order)
#         curr_5d_p_tab1 = create_bucket_pivot(fiveday_planned, bucket_order)
#         curr_5d_po_tab1 = create_bucket_pivot(fiveday_popc, bucket_order)
#         curr_5d_u_tab1 = create_bucket_pivot(fiveday_unplanned, bucket_order)

#         combined_circle = pd.concat(
#             [curr_1d_p_tab1, curr_1d_po_tab1, curr_1d_u_tab1, curr_5d_p_tab1, curr_5d_po_tab1, curr_5d_u_tab1], 
#             axis=1, 
#             keys=['END DATE (Planned)', 'END DATE (Power Off)', 'END DATE (Unplanned)', 'RANGE (Planned)', 'RANGE (Power Off)', 'RANGE (Unplanned)']
#         ).fillna(0).astype(int)

#         if not combined_circle.empty:
#             styled_combined = apply_pu_gradient(combined_circle.style, combined_circle).set_table_styles(HEADER_STYLES)
#             selection_event = st.dataframe(styled_combined, width="stretch", on_select="rerun", selection_mode="single-row")

#             if len(selection_event.selection.rows) > 0:
#                 selected_circle = combined_circle.index[selection_event.selection.rows[0]]
#                 st.subheader(f"Feeder Details for: {selected_circle}")
                
#                 circle_dates = sorted(list(valid_5day[valid_5day['Circle'] == selected_circle]['Outage Date'].dropna().unique()))
#                 selected_dates = st.multiselect("Filter Range View by Date:", options=circle_dates, default=circle_dates, format_func=lambda x: x.strftime('%d %b %Y'), key="ms_tab1")
                
#                 def highlight_notorious(row): return ['background-color: rgba(220, 53, 69, 0.15); color: #850000; font-weight: bold'] * len(row) if (selected_circle, row['Feeder']) in notorious_set else [''] * len(row)

#                 st.write("---")
#                 st.markdown(f"### 🔴 SINGLE DAY DRILLDOWN ({end_str_1})")
#                 today_left, today_mid, today_right = st.columns(3)
#                 with today_left:
#                     st.markdown(f"**Planned Outages**")
#                     fl_tp = today_p[today_p['Circle'] == selected_circle][['Feeder', 'Diff in mins', 'Status_Calc', 'Duration Bucket']].rename(columns={'Status_Calc': 'Status'}) if not today_p.empty else pd.DataFrame(columns=['Feeder', 'Diff in mins', 'Status', 'Duration Bucket'])
#                     st.dataframe(fl_tp.style.apply(highlight_notorious, axis=1).set_table_styles(HEADER_STYLES), width="stretch", hide_index=True)
#                 with today_mid:
#                     st.markdown(f"**Power Off By PC**")
#                     fl_tpo = today_po[today_po['Circle'] == selected_circle][['Feeder', 'Diff in mins', 'Status_Calc', 'Duration Bucket']].rename(columns={'Status_Calc': 'Status'}) if not today_po.empty else pd.DataFrame(columns=['Feeder', 'Diff in mins', 'Status', 'Duration Bucket'])
#                     st.dataframe(fl_tpo.style.apply(highlight_notorious, axis=1).set_table_styles(HEADER_STYLES), width="stretch", hide_index=True)
#                 with today_right:
#                     st.markdown(f"**Unplanned Outages**")
#                     fl_tu = today_u[today_u['Circle'] == selected_circle][['Feeder', 'Diff in mins', 'Status_Calc', 'Duration Bucket']].rename(columns={'Status_Calc': 'Status'}) if not today_u.empty else pd.DataFrame(columns=['Feeder', 'Diff in mins', 'Status', 'Duration Bucket'])
#                     st.dataframe(fl_tu.style.apply(highlight_notorious, axis=1).set_table_styles(HEADER_STYLES), width="stretch", hide_index=True)
                    
#                 st.write("---") 
#                 st.markdown(f"### 🟢 SELECTED RANGE DRILLDOWN")
#                 fiveday_left, fiveday_mid, fiveday_right = st.columns(3)
#                 with fiveday_left:
#                     st.markdown(f"**Planned Outages**")
#                     fl_fp = fiveday_planned[(fiveday_planned['Circle'] == selected_circle) & (fiveday_planned['Outage Date'].isin(selected_dates))].copy() if not fiveday_planned.empty else pd.DataFrame()
#                     if not fl_fp.empty:
#                         fl_fp['Diff in Hours'] = (fl_fp['Diff in mins'] / 60).round(2)
#                         st.dataframe(fl_fp[['Outage Date', 'Start Time', 'Feeder', 'Diff in Hours', 'Duration Bucket']].style.apply(highlight_notorious, axis=1).format({'Diff in Hours': '{:.2f}'}).set_table_styles(HEADER_STYLES), width="stretch", hide_index=True)
#                 with fiveday_mid:
#                     st.markdown(f"**Power Off By PC**")
#                     fl_fpo = fiveday_popc[(fiveday_popc['Circle'] == selected_circle) & (fiveday_popc['Outage Date'].isin(selected_dates))].copy() if not fiveday_popc.empty else pd.DataFrame()
#                     if not fl_fpo.empty:
#                         fl_fpo['Diff in Hours'] = (fl_fpo['Diff in mins'] / 60).round(2)
#                         st.dataframe(fl_fpo[['Outage Date', 'Start Time', 'Feeder', 'Diff in Hours', 'Duration Bucket']].style.apply(highlight_notorious, axis=1).format({'Diff in Hours': '{:.2f}'}).set_table_styles(HEADER_STYLES), width="stretch", hide_index=True)
#                 with fiveday_right:
#                     st.markdown(f"**Unplanned Outages**")
#                     fl_fu = fiveday_unplanned[(fiveday_unplanned['Circle'] == selected_circle) & (fiveday_unplanned['Outage Date'].isin(selected_dates))].copy() if not fiveday_unplanned.empty else pd.DataFrame()
#                     if not fl_fu.empty:
#                         fl_fu['Diff in Hours'] = (fl_fu['Diff in mins'] / 60).round(2)
#                         st.dataframe(fl_fu[['Outage Date', 'Start Time', 'Feeder', 'Diff in Hours', 'Duration Bucket']].style.apply(highlight_notorious, axis=1).format({'Diff in Hours': '{:.2f}'}).set_table_styles(HEADER_STYLES), width="stretch", hide_index=True)

#     # ==========================================
#     # TAB 2: YOY DRILL-DOWN
#     # ==========================================
#     with tab2:
#         st.radio("📅 Select Time Period:", ["Today", "Current Month", "Last Month", "Last 3 Months", "Last 6 Months"], key="preset_t2", horizontal=True, on_change=update_dates_t2)
#         c1, c2 = st.columns(2)
#         start_date_2 = c1.date_input("From Date", key="start_t2")
#         end_date_2 = c2.date_input("To Date", key="end_t2")

#         st.header("📈 Historical Year-over-Year Drilldown")
#         if df_all_outages.empty:
#             st.error("Master Outages Data not found.")
#         else:
#             st.markdown(f"**Comparing Period:** {start_date_2.strftime('%d %b %Y')} to {end_date_2.strftime('%d %b %Y')}")
#             st.divider()

#             def get_ly_date(d):
#                 try: return d.replace(year=d.year - 1)
#                 except ValueError: return d.replace(year=d.year - 1, day=28)

#             ly_start = get_ly_date(start_date_2)
#             ly_end = get_ly_date(end_date_2)
            
#             mask_curr = (df_all_outages['DateOnly'] >= start_date_2) & (df_all_outages['DateOnly'] <= end_date_2)
#             filtered_curr = df_all_outages[mask_curr]
            
#             try:
#                 df_ly_master = pd.read_csv("Historical_2025.csv")
#                 if 'Start Time' in df_ly_master.columns:
#                     df_ly_master['DateOnly'] = pd.to_datetime(df_ly_master['Start Time'], errors='coerce').dt.date
#                     mask_ly = (df_ly_master['DateOnly'] >= ly_start) & (df_ly_master['DateOnly'] <= ly_end)
#                     filtered_ly = df_ly_master[mask_ly]
#                 else:
#                     st.error("Column 'Start Time' is missing from Historical_2025.csv")
#                     filtered_ly = pd.DataFrame()
#             except FileNotFoundError:
#                 st.error("Historical_2025.csv not found in the directory.")
#                 filtered_ly = pd.DataFrame()

#             if not filtered_curr.empty or not filtered_ly.empty:
#                 st.markdown(f"### 📍 1. Zone-wise Distribution")
#                 yoy_zone = generate_yoy_dist_expanded(filtered_curr, filtered_ly, 'Zone')
#                 zone_selection = st.dataframe(
#                     yoy_zone.style.map(highlight_delta, subset=['YoY Delta (Total)']).format(precision=2).set_table_styles(HEADER_STYLES), 
#                     width="stretch", 
#                     hide_index=True, 
#                     on_select="rerun", 
#                     selection_mode="single-row",
#                     key="yoy_zone_select" 
#                 )

#                 if len(zone_selection.selection.rows) > 0:
#                     selected_zone = yoy_zone.iloc[zone_selection.selection.rows[0]]['Zone']
#                     st.markdown(f"### 🎯 2. Circle-wise Distribution for **{selected_zone}**")
                    
#                     curr_zone_df = filtered_curr[filtered_curr['Zone'] == selected_zone]
#                     ly_zone_df = filtered_ly[filtered_ly['Zone'] == selected_zone]
                    
#                     yoy_circle = generate_yoy_dist_expanded(curr_zone_df, ly_zone_df, 'Circle')
#                     circle_selection = st.dataframe(
#                         yoy_circle.style.map(highlight_delta, subset=['YoY Delta (Total)']).format(precision=2).set_table_styles(HEADER_STYLES), 
#                         width="stretch", 
#                         hide_index=True, 
#                         on_select="rerun", 
#                         selection_mode="single-row",
#                         key="yoy_circle_select" 
#                     )

#                     if len(circle_selection.selection.rows) > 0:
#                         selected_circle = yoy_circle.iloc[circle_selection.selection.rows[0]]['Circle']
#                         st.markdown(f"### 🔌 3. Feeder-wise Distribution for **{selected_circle}**")
                        
#                         curr_circle_df = curr_zone_df[curr_zone_df['Circle'] == selected_circle]
#                         ly_circle_df = ly_zone_df[ly_zone_df['Circle'] == selected_circle]
                        
#                         yoy_feeder = generate_yoy_dist_expanded(curr_circle_df, ly_circle_df, 'Feeder')
#                         st.dataframe(
#                             yoy_feeder.style.map(highlight_delta, subset=['YoY Delta (Total)']).format(precision=2).set_table_styles(HEADER_STYLES), 
#                             width="stretch", 
#                             hide_index=True,
#                             key="yoy_feeder_display"
#                         )
#             else:
#                 st.info("No data available for the selected dates in both 2026 and 2025.")

#     # ==========================================
#     # TAB 3: PTW FREQUENCY
#     # ==========================================
#     with tab3:
#         st.radio("📅 Select Time Period:", ["Today", "Current Month", "Last Month", "Last 3 Months", "Last 6 Months"], key="preset_t3", horizontal=True, on_change=update_dates_t3)
#         c1, c2 = st.columns(2)
#         start_date_3 = c1.date_input("From Date", key="start_t3")
#         end_date_3 = c2.date_input("To Date", key="end_t3")
#         end_str_3 = end_date_3.strftime("%Y-%m-%d")

#         st.header(f"🛠️ PTW Frequency Tracker ({st.session_state.preset_t3})")

#         if not df_all_ptw.empty:
#             df_all_ptw['DateOnly_Start'] = pd.to_datetime(df_all_ptw['Start Date'], dayfirst=False, errors='coerce').dt.date
#             df_all_ptw['DateOnly_Req'] = pd.to_datetime(df_all_ptw['Request Date'], dayfirst=False, errors='coerce').dt.date
#             mask_ptw = ((df_all_ptw['DateOnly_Start'] >= start_date_3) & (df_all_ptw['DateOnly_Start'] <= end_date_3)) | \
#                        ((df_all_ptw['DateOnly_Req'] >= start_date_3) & (df_all_ptw['DateOnly_Req'] <= end_date_3))
#             df_ptw = df_all_ptw[mask_ptw].copy()
#         else:
#             df_ptw = pd.DataFrame()

#         if df_ptw.empty:
#             st.info("No PTW data found for the selected period.")
#         else:
#             ptw_col = next((c for c in df_ptw.columns if 'ptw' in c.lower() or 'request' in c.lower() or 'id' in c.lower()), None)
#             feeder_col = next((c for c in df_ptw.columns if 'feeder' in c.lower()), None)
#             status_col = next((c for c in df_ptw.columns if 'status' in c.lower()), None)
#             circle_col = next((c for c in df_ptw.columns if 'circle' in c.lower()), None)

#             if not ptw_col or not feeder_col:
#                 st.error("Could not dynamically map required columns from the PTW export.")
#             else:
#                 ptw_clean = df_ptw.copy()
#                 if status_col:
#                     ptw_clean = ptw_clean[~ptw_clean[status_col].astype(str).str.contains('Cancellation', na=False, case=False)]

#                 ptw_clean[feeder_col] = ptw_clean[feeder_col].astype(str).str.replace('|', ',', regex=False).str.split(',')
#                 ptw_clean = ptw_clean.explode(feeder_col).reset_index(drop=True)
#                 ptw_clean[feeder_col] = ptw_clean[feeder_col].astype(str).str.strip()
#                 ptw_clean = ptw_clean[(ptw_clean[feeder_col] != '') & (ptw_clean[feeder_col].str.lower() != 'nan')]

#                 group_cols = [feeder_col]
#                 if circle_col: group_cols.insert(0, circle_col)
                    
#                 ptw_counts = ptw_clean.groupby(group_cols).agg(Unique_PTWs=(ptw_col, 'nunique'), PTW_IDs=(ptw_col, lambda x: ', '.join(x.dropna().astype(str).unique()))).reset_index()
#                 repeat_feeders = ptw_counts[ptw_counts['Unique_PTWs'] >= 2].sort_values(by='Unique_PTWs', ascending=False).reset_index(drop=True)
#                 repeat_feeders = repeat_feeders.rename(columns={'Unique_PTWs': 'PTW Request Count', 'PTW_IDs': 'Associated PTW Request Numbers'})

#                 kpi1, kpi2 = st.columns(2)
#                 with kpi1: st.markdown(f'<div class="kpi-card"><div><div class="kpi-title">Total Active PTW Requests</div><div class="kpi-value">{df_ptw[ptw_col].nunique()}</div></div><div class="kpi-subtext"><span class="status-badge">Selected Period</span></div></div>', unsafe_allow_html=True)
#                 with kpi2: st.markdown(f'<div class="kpi-card"><div><div class="kpi-title">Feeders with Multiple PTWs</div><div class="kpi-value">{len(repeat_feeders)}</div></div><div class="kpi-subtext"><span class="status-badge" style="background-color: #D32F2F;">🔴 Needs Review</span></div></div>', unsafe_allow_html=True)

#                 st.divider()
#                 st.subheader("⚠️ Repeat PTW Feeders Detail View")
#                 if not repeat_feeders.empty:
#                     st.dataframe(repeat_feeders.style.set_table_styles(HEADER_STYLES), width="stretch", hide_index=True)
#                 else:
#                     st.success("No feeders had multiple PTWs requested against them! 🎉")

#                 st.divider()
#                 st.subheader("⏳ Specific Day PTW Requests")
#                 start_col_ptw = next((c for c in df_ptw.columns if ('start' in c.lower() or 'from' in c.lower()) and ('date' in c.lower() or 'time' in c.lower())), None)
#                 end_col_ptw = next((c for c in df_ptw.columns if ('end' in c.lower() or 'to' in c.lower()) and ('date' in c.lower() or 'time' in c.lower())), None)

#                 if start_col_ptw and end_col_ptw:
#                     today_ptws = ptw_clean.copy()
#                     today_ptws[start_col_ptw] = pd.to_datetime(today_ptws[start_col_ptw], dayfirst=False, errors='coerce')
#                     today_ptws[end_col_ptw] = pd.to_datetime(today_ptws[end_col_ptw], dayfirst=False, errors='coerce')
#                     req_date_col = next((c for c in df_ptw.columns if 'request' in c.lower() and ('date' in c.lower() or 'time' in c.lower())), None)
                    
#                     if req_date_col:
#                         today_ptws[req_date_col] = pd.to_datetime(today_ptws[req_date_col], dayfirst=False, errors='coerce')
#                         mask = (today_ptws[start_col_ptw].dt.date == pd.to_datetime(end_str_3).date()) | (today_ptws[req_date_col].dt.date == pd.to_datetime(end_str_3).date())
#                     else:
#                         mask = (today_ptws[start_col_ptw].dt.date == pd.to_datetime(end_str_3).date())
                    
#                     today_ptws = today_ptws[mask]
#                     if not today_ptws.empty:
#                         today_ptws['Duration (Hours)'] = (today_ptws[end_col_ptw] - today_ptws[start_col_ptw]).dt.total_seconds() / 3600.0
#                         today_ptws['Duration (Hours)'] = today_ptws['Duration (Hours)'].apply(lambda x: max(x, 0)).round(2)
                        
#                         def ptw_bucket(hrs):
#                             if pd.isna(hrs): return "Unknown"
#                             if hrs <= 2: return "0-2 Hrs"
#                             elif hrs <= 4: return "2-4 Hrs"
#                             elif hrs <= 8: return "4-8 Hrs"
#                             else: return "Above 8 Hrs"
                        
#                         today_ptws['Time Bucket'] = today_ptws['Duration (Hours)'].apply(ptw_bucket)
#                         display_cols_ptw = [feeder_col, start_col_ptw, end_col_ptw, 'Duration (Hours)', 'Time Bucket']
#                         if circle_col: display_cols_ptw.insert(0, circle_col)
                        
#                         final_today_ptws = today_ptws[display_cols_ptw].dropna(subset=[start_col_ptw]).sort_values(by='Duration (Hours)', ascending=False).reset_index(drop=True)
#                         def highlight_long_ptw(row): return ['background-color: rgba(220, 53, 69, 0.15); color: #850000; font-weight: bold'] * len(row) if pd.notna(row['Duration (Hours)']) and row['Duration (Hours)'] > 5 else [''] * len(row)
                            
#                         over_5_count = final_today_ptws[final_today_ptws['Duration (Hours)'] > 5][feeder_col].nunique()
#                         st.markdown(f"**Total Feeders under PTW on {end_str_3} exceeding 5 Hours:** `{over_5_count}`")
#                         st.dataframe(final_today_ptws.style.apply(highlight_long_ptw, axis=1).format({'Duration (Hours)': '{:.2f}'}).set_table_styles(HEADER_STYLES), width="stretch", hide_index=True)
#                     else: st.info(f"No PTW requests recorded specifically for {end_str_3}.")
#                 else: st.warning("Could not dynamically identify Start and End time columns in the PTW report.")

# # --- ROUTER LOGIC ---
# if st.session_state.page == 'home':
#     render_home()
# elif st.session_state.page == 'dashboard':
#     render_dashboard()
# elif st.session_state.page == 'ptw_app':
#     render_ptw_lm_dashboard()



# =========================================================================================================================
# V1
# =========================================================================================================================

# import os
# import requests
# import streamlit as st
# import pandas as pd
# from datetime import datetime, timedelta, timezone
# from ptw_lm_app import render_ptw_lm_dashboard

# # --- PAGE CONFIGURATION ---
# st.set_page_config(page_title="Utility Operations Command Center", layout="wide")

# # --- INITIALIZE SESSION STATE FOR NAVIGATION ---
# if 'page' not in st.session_state:
#     st.session_state.page = 'home'

# # --- GLOBAL TABLE HEADER STYLING ---
# HEADER_STYLES = [
#     {
#         'selector': 'th',
#         'props': [
#             ('background-color', '#004085 !important'),
#             ('color', '#FFC107 !important'),
#             ('font-weight', 'bold !important'),
#             ('text-align', 'center !important')
#         ]
#     },
#     {
#         'selector': 'th div',
#         'props': [
#             ('color', '#FFC107 !important'),
#             ('font-weight', 'bold !important')
#         ]
#     }
# ]

# # --- NEW VERSION 2 API ENDPOINTS ---
# OUTAGE_URL = "https://distribution.pspcl.in/returns/module.php?to=OutageAPI.getOutages"
# PTW_URL = "https://distribution.pspcl.in/returns/module.php?to=OutageAPI.getPTWRequests"

# # --- IST TIMEZONE SETUP ---
# IST = timezone(timedelta(hours=5, minutes=30))
# now_ist = datetime.now(IST)
# if now_ist.hour < 8: now_ist -= timedelta(days=1)

# # --- DATA LOADING FUNCTIONS ---
# def fetch_from_api(url, payload):
#     try:
#         res = requests.post(url, json=payload, headers={'Content-Type': 'application/json'}, timeout=20)
#         res.raise_for_status()
#         data = res.json()
#         return data if isinstance(data, list) else data.get("data", [])
#     except Exception as e:
#         st.toast(f"API Fetch warning: {e}")
#         return []

# @st.cache_data(ttl=900, show_spinner="Fetching live data from PSPCL...")
# def load_live_data_from_api(start_date_str, end_date_str):
#     api_key = st.secrets["API_KEY"]
    
#     outage_cols = ["Zone", "Circle", "Feeder", "Type of Outage", "Status", "Start Time", "End Time", "Diff in mins"]
#     ptw_cols = ["PTW Request ID", "Permit Number", "Circle", "Feeder", "Status", "Start Date", "Request Date", "End Date"]
    
#     # 1. Fetch Today's/End Date Outages (For the single day KPIs)
#     data_today = fetch_from_api(OUTAGE_URL, {"fromdate": end_date_str, "todate": end_date_str, "apikey": api_key})
#     df_today = pd.DataFrame(data_today)
#     if not df_today.empty:
#         df_today.rename(columns={
#             "zone_name": "Zone", "circle_name": "Circle", "feeder_name": "Feeder", 
#             "outage_type": "Type of Outage", "outage_status": "Status", 
#             "start_time": "Start Time", "end_time": "End Time", "duration_minutes": "Diff in mins"
#         }, inplace=True)
#     else:
#         df_today = pd.DataFrame(columns=outage_cols)

#     # 2. Fetch Selected Range Outages (Replacing 5-Day)
#     data_range = fetch_from_api(OUTAGE_URL, {"fromdate": start_date_str, "todate": end_date_str, "apikey": api_key})
#     df_range = pd.DataFrame(data_range)
#     if not df_range.empty:
#         df_range.rename(columns={
#             "zone_name": "Zone", "circle_name": "Circle", "feeder_name": "Feeder", 
#             "outage_type": "Type of Outage", "outage_status": "Status", 
#             "start_time": "Start Time", "end_time": "End Time", "duration_minutes": "Diff in mins"
#         }, inplace=True)
#     else:
#         df_range = pd.DataFrame(columns=outage_cols)

#     # 3. Fetch Selected Range PTWs (Replacing 7-Day) 
#     data_ptw = fetch_from_api(PTW_URL, {"fromdate": start_date_str, "todate": end_date_str, "apikey": api_key})
#     df_ptw = pd.DataFrame(data_ptw)
#     if not df_ptw.empty:
#         if 'feeders' in df_ptw.columns:
#             df_ptw['feeders'] = df_ptw['feeders'].apply(lambda x: ', '.join(x) if isinstance(x, list) else str(x))
#         df_ptw.rename(columns={
#             "ptw_id": "PTW Request ID", "permit_no": "Permit Number", 
#             "circle_name": "Circle", "feeders": "Feeder", "current_status": "Status", 
#             "start_time": "Start Date", "end_time": "End Date", "creation_date": "Request Date"
#         }, inplace=True)
#     else:
#         df_ptw = pd.DataFrame(columns=ptw_cols)

#     # 4. Process Data Calculations
#     time_cols = ['Start Time', 'End Time']
#     for df in [df_today, df_range]:
#         if not df.empty:
#             if 'Type of Outage' in df.columns:
#                 df['Raw Outage Type'] = df['Type of Outage'].astype(str).str.strip()
#                 def standardize_outage(val):
#                     v_lower = str(val).lower()
#                     if 'power off' in v_lower: return 'Power Off By PC'
#                     if 'unplanned' in v_lower: return 'Unplanned Outage'
#                     if 'planned' in v_lower: return 'Planned Outage'
#                     return val
#                 df['Type of Outage'] = df['Raw Outage Type'].apply(standardize_outage)

#             for col in time_cols: 
#                 if col in df.columns: df[col] = pd.to_datetime(df[col], errors='coerce')
            
#             if 'Diff in mins' in df.columns:
#                 df['Diff in mins'] = pd.to_numeric(df['Diff in mins'], errors='coerce')
                
#             if 'Status' in df.columns:
#                 df['Status_Calc'] = df['Status'].apply(lambda x: 'Active' if str(x).strip().title() in ['Active', 'Open'] else 'Closed')
            
#             def assign_bucket(mins):
#                 if pd.isna(mins) or mins < 0: return "Active/Unknown"
#                 hrs = mins / 60
#                 if hrs <= 2: return "Up to 2 Hrs"
#                 elif hrs <= 4: return "2-4 Hrs"
#                 elif hrs <= 8: return "4-8 Hrs"
#                 else: return "Above 8 Hrs"
#             df['Duration Bucket'] = df['Diff in mins'].apply(assign_bucket)
            
#     # Capture exact time data was fetched
#     fetch_time = datetime.now(IST).strftime('%d %b %Y, %I:%M %p')
#     return df_today, df_range, df_ptw, fetch_time

# @st.cache_data
# def load_historical_data():
#     if os.path.exists('Historical_2026.csv') and os.path.exists('Historical_2025.csv'):
#         df_26, df_25 = pd.read_csv('Historical_2026.csv'), pd.read_csv('Historical_2025.csv')
#         for df in [df_26, df_25]:
#             if 'Type of Outage' in df.columns:
#                 df['Raw Outage Type'] = df['Type of Outage'].astype(str).str.strip()
#                 def standardize_outage(val):
#                     v_lower = str(val).lower()
#                     if 'power off' in v_lower: return 'Power Off By PC'
#                     if 'unplanned' in v_lower: return 'Unplanned Outage'
#                     if 'planned' in v_lower: return 'Planned Outage'
#                     return val
#                 df['Type of Outage'] = df['Raw Outage Type'].apply(standardize_outage)
                
#             df['Outage Date'] = pd.to_datetime(df['Start Time'], errors='coerce').dt.date
#         return df_26, df_25
#     return pd.DataFrame(), pd.DataFrame()

# # --- HELPER FUNCTIONS ---
# def generate_yoy_dist_expanded(df_curr, df_ly, group_col):
#     def _agg(df, prefix):
#         if df.empty: return pd.DataFrame({group_col: []}).set_index(group_col)
#         df['Diff in mins'] = pd.to_numeric(df['Diff in mins'], errors='coerce').fillna(0)
#         g = df.groupby([group_col, 'Type of Outage']).agg(Count=('Type of Outage', 'size'), TotalHrs=('Diff in mins', lambda x: round(x.sum() / 60, 2)), AvgHrs=('Diff in mins', lambda x: round(x.mean() / 60, 2))).unstack(fill_value=0)
#         g.columns = [f"{prefix} {outage} ({metric})" for metric, outage in g.columns]
#         return g

#     c_grp = _agg(df_curr, 'Curr')
#     l_grp = _agg(df_ly, 'LY')
#     merged = pd.merge(c_grp, l_grp, on=group_col, how='outer').fillna(0).reset_index()
    
#     expected_cols = []
#     for prefix in ['Curr', 'LY']:
#         for outage in ['Planned Outage', 'Power Off By PC', 'Unplanned Outage']:
#             for metric in ['Count', 'TotalHrs', 'AvgHrs']:
#                 col_name = f"{prefix} {outage} ({metric})"
#                 expected_cols.append(col_name)
#                 if col_name not in merged.columns: merged[col_name] = 0
                    
#     for col in expected_cols:
#         if '(Count)' in col: merged[col] = merged[col].astype(int)
#         else: merged[col] = merged[col].astype(float).round(2)
            
#     merged['Curr Total (Count)'] = merged['Curr Planned Outage (Count)'] + merged['Curr Power Off By PC (Count)'] + merged['Curr Unplanned Outage (Count)']
#     merged['LY Total (Count)'] = merged['LY Planned Outage (Count)'] + merged['LY Power Off By PC (Count)'] + merged['LY Unplanned Outage (Count)']
#     merged['YoY Delta (Total)'] = merged['Curr Total (Count)'] - merged['LY Total (Count)']
    
#     cols_order = [group_col, 
#                   'Curr Planned Outage (Count)', 'Curr Planned Outage (TotalHrs)', 'Curr Planned Outage (AvgHrs)', 
#                   'LY Planned Outage (Count)', 'LY Planned Outage (TotalHrs)', 'LY Planned Outage (AvgHrs)', 
#                   'Curr Power Off By PC (Count)', 'Curr Power Off By PC (TotalHrs)', 'Curr Power Off By PC (AvgHrs)', 
#                   'LY Power Off By PC (Count)', 'LY Power Off By PC (TotalHrs)', 'LY Power Off By PC (AvgHrs)', 
#                   'Curr Unplanned Outage (Count)', 'Curr Unplanned Outage (TotalHrs)', 'Curr Unplanned Outage (AvgHrs)', 
#                   'LY Unplanned Outage (Count)', 'LY Unplanned Outage (TotalHrs)', 'LY Unplanned Outage (AvgHrs)', 
#                   'Curr Total (Count)', 'LY Total (Count)', 'YoY Delta (Total)']
#     return merged[cols_order]

# def apply_pu_gradient(styler, df):
#     p_cols = [c for c in df.columns if 'Planned' in str(c) and pd.api.types.is_numeric_dtype(df[c])]
#     u_cols = [c for c in df.columns if 'Unplanned' in str(c) and pd.api.types.is_numeric_dtype(df[c])]
#     po_cols = [c for c in df.columns if 'Power Off' in str(c) and pd.api.types.is_numeric_dtype(df[c])]
    
#     if p_cols: styler = styler.background_gradient(subset=p_cols, cmap='Blues', vmin=0)
#     if u_cols: styler = styler.background_gradient(subset=u_cols, cmap='Reds', vmin=0)
#     if po_cols: styler = styler.background_gradient(subset=po_cols, cmap='Greens', vmin=0)
#     return styler

# def highlight_delta(val):
#     if isinstance(val, int):
#         if val > 0: return 'color: #D32F2F; font-weight: bold;'
#         elif val < 0: return 'color: #388E3C; font-weight: bold;'
#     return ''

# def create_bucket_pivot(df, bucket_order):
#     if df.empty: return pd.DataFrame(columns=bucket_order + ['Total'])
#     pivot = pd.crosstab(df['Circle'], df['Duration Bucket'])
#     pivot = pivot.reindex(columns=[c for c in bucket_order if c in pivot.columns], fill_value=0)
#     pivot['Total'] = pivot.sum(axis=1)
#     return pivot


# # ==========================================
# # PAGE 1: HOME (COMMAND CENTER)
# # ==========================================
# def render_home():
#     st.markdown("""
#         <style>
#             .home-title {
#                 text-align: center;
#                 color: #004085;
#                 font-weight: 700;
#                 font-size: 2.5rem;
#                 margin-top: 2rem;
#                 margin-bottom: 0.5rem;
#                 font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif;
#             }
#             .home-subtitle {
#                 text-align: center;
#                 color: #555555;
#                 font-size: 1.1rem;
#                 margin-bottom: 3rem;
#             }
#             div.stButton > button {
#                 height: 90px;
#                 font-size: 1.1rem;
#                 font-weight: 600;
#                 background-color: #ffffff;
#                 color: #333333;
#                 border: 1px solid #e0e0e0;
#                 border-radius: 8px;
#                 box-shadow: 0 4px 6px rgba(0,0,0,0.05);
#                 transition: all 0.3s ease;
#             }
#             div.stButton > button:hover {
#                 border-color: #004085;
#                 box-shadow: 0 6px 12px rgba(0,0,0,0.15);
#                 color: #004085;
#                 transform: translateY(-2px);
#             }
#         </style>
#     """, unsafe_allow_html=True)

#     st.markdown("<div class='home-title'>⚡ Utility Operations Command Center</div>", unsafe_allow_html=True)
#     st.markdown("<div class='home-subtitle'>Select an operational module below to access real-time dashboards and management tools.</div>", unsafe_allow_html=True)
    
#     st.write("---")
    
#     st.write("")
#     row1_col1, row1_col2, row1_col3 = st.columns(3, gap="large")
    
#     with row1_col1:
#         if st.button("🛠️ PTW, LM-ALM Application", use_container_width=True):
#             st.session_state.page = 'ptw_app'
#             st.rerun()
            
#     with row1_col2:
#         if st.button("📉 Outage Reduction Plan (ORP)", use_container_width=True):
#             st.toast("This module is currently offline or under development.")
            
#     with row1_col3:
#         if st.button("🏢 RDSS", use_container_width=True):
#             st.toast("This module is currently offline or under development.")

#     st.write("")
#     row2_col1, row2_col2, row2_col3 = st.columns(3, gap="large")
    
#     with row2_col1:
#         if st.button("📡 Smart Meter", use_container_width=True):
#             st.toast("This module is currently offline or under development.")
            
#     with row2_col2:
#         if st.button("🔌 New Connections", use_container_width=True):
#             st.toast("This module is currently offline or under development.")
            
#     with row2_col3:
#         if st.button("🚨 Outage Monitoring", use_container_width=True):
#             st.session_state.page = 'dashboard'
#             st.rerun()

# # ==========================================
# # PAGE 2: MAIN DASHBOARD
# # ==========================================
# def render_dashboard():
#     # Apply Dashboard styling
#     st.markdown("""
#         <style>
#             .block-container { padding-top: 1.5rem; padding-bottom: 1.5rem; font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif; }
#             p, span, div, caption, .stMarkdown { color: #000000 !important; }
#             h1, h2, h3, h4, h5, h6, div.block-container h1 { color: #004085 !important; font-weight: 700 !important; font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif; }
#             div.block-container h1 { text-align: center; border-bottom: 3px solid #004085 !important; padding-bottom: 10px; margin-bottom: 30px !important; font-size: 2.2rem !important; }
#             h2 { font-size: 1.3rem !important; border-bottom: 2px solid #004085 !important; padding-bottom: 5px; margin-bottom: 10px !important; }
#             h3 { font-size: 1.05rem !important; margin-bottom: 12px !important; text-transform: uppercase; letter-spacing: 0.5px; }
#             hr { border: 0; border-top: 1px solid #004085; margin: 1.5rem 0; opacity: 0.3; }
            
#             .kpi-card { background: linear-gradient(135deg, #004481 0%, #0066cc 100%); border-radius: 6px; padding: 1.2rem 1.2rem; display: flex; flex-direction: column; justify-content: space-between; height: 100%; box-shadow: 0 2px 4px rgba(0,0,0,0.08); transition: transform 0.2s ease-in-out, box-shadow 0.2s ease-in-out; border: 1px solid #003366; }
#             .kpi-card:hover { transform: translateY(-4px); box-shadow: 0 8px 16px rgba(0, 68, 129, 0.2); }
#             .kpi-card .kpi-title, .kpi-title { color: #FFC107 !important; font-weight: 600; font-size: 0.85rem; text-transform: uppercase; letter-spacing: 0.5px; margin-bottom: 0.4rem; }
#             .kpi-card .kpi-value, .kpi-value { color: #FFFFFF !important; font-weight: 700; font-size: 2.6rem; margin-bottom: 0; line-height: 1.1; }
#             .kpi-card .kpi-subtext, .kpi-subtext { color: #F8F9FA !important; font-size: 0.85rem; margin-top: 1rem; padding-top: 0.6rem; border-top: 1px solid rgba(255, 255, 255, 0.2); display: flex; justify-content: flex-start; gap: 15px; }
            
#             .status-badge { background-color: rgba(0, 0, 0, 0.25); padding: 3px 8px; border-radius: 4px; font-weight: 500; color: #FFFFFF !important; }
#             [data-testid="stDataFrame"] > div { border: 2px solid #004085 !important; border-radius: 6px; overflow: hidden; }
#         </style>
#     """, unsafe_allow_html=True)

#     # Dashboard Header with Password Popover and Timestamp
#     col1, col2 = st.columns([0.75, 0.25])
#     with col1:
#         st.title("⚡ Power Outage Monitoring Dashboard")
#     with col2:
#         st.write("")
#         btn_col1, btn_col2 = st.columns(2)
#         with btn_col1:
#             if st.button("⬅️ Home", use_container_width=True):
#                 st.session_state.page = 'home'
#                 st.rerun()
#         with btn_col2:
#             with st.popover("🔄 Refresh", use_container_width=True):
#                 st.markdown("**Admin Access Required**")
#                 pwd = st.text_input("Passcode:", type="password", placeholder="Enter passcode...")
#                 if st.button("Confirm Refresh", use_container_width=True):
#                     if pwd == "J@Y":
#                         st.cache_data.clear()
#                         st.rerun()
#                     else:
#                         st.error("Incorrect password.")

#     # 2. Advanced Date Selection (Inserted Here)
#     today = pd.to_datetime("today").date()
    
#     st.write("---")
#     date_preset = st.radio(
#         "📅 Select Time Period:", 
#         ["Custom Range", "Today", "Current Month", "Last Month", "Last 3 Months", "Last 6 Months"], 
#         index=2, # Defaults to Current Month
#         horizontal=True
#     )
    
#     if date_preset == "Today":
#         start_date = end_date = today
#     elif date_preset == "Current Month":
#         start_date = today.replace(day=1)
#         end_date = today
#     elif date_preset == "Last Month":
#         end_date = today.replace(day=1) - pd.Timedelta(days=1)
#         start_date = end_date.replace(day=1)
#     elif date_preset == "Last 3 Months":
#         start_date = (today - pd.DateOffset(months=3)).date()
#         end_date = today
#     elif date_preset == "Last 6 Months":
#         start_date = (today - pd.DateOffset(months=6)).date()
#         end_date = today
#     else: # Custom Range
#         c1, c2 = st.columns(2)
#         with c1: start_date = st.date_input("From Date", value=today - pd.Timedelta(days=7))
#         with c2: end_date = st.date_input("To Date", value=today)

#     start_str = start_date.strftime("%Y-%m-%d")
#     end_str = end_date.strftime("%Y-%m-%d")

#     # Load data dynamically based on selection
#     df_today, df_5day, df_ptw, last_updated = load_live_data_from_api(start_str, end_str)
#     df_hist_curr, df_hist_ly = load_historical_data()

#     # Display the dynamic timestamp
#     with col2:
#         st.markdown(f"<div style='text-align: right; color: #666; font-size: 0.85rem; margin-top: 4px;'>Data Last Updated:<br><b>{last_updated}</b></div>", unsafe_allow_html=True)

#     # Pre-compute Notorious Feeders (using the selected range data now in df_5day)
#     if not df_5day.empty:
#         df_5day['Outage Date'] = df_5day['Start Time'].dt.date
#         feeder_days = df_5day.groupby(['Circle', 'Feeder'])['Outage Date'].nunique().reset_index(name='Days with Outages')
#         notorious = feeder_days[feeder_days['Days with Outages'] >= 3]

#         feeder_stats = df_5day.groupby(['Circle', 'Feeder']).agg(Total_Events=('Start Time', 'size'), Avg_Mins=('Diff in mins', 'mean'), Total_Mins=('Diff in mins', 'sum')).reset_index()
#         feeder_stats.rename(columns={'Total_Events': 'Total Outage Events'}, inplace=True)
#         feeder_stats['Total Duration (Hours)'] = (feeder_stats['Total_Mins'] / 60).round(2)
#         feeder_stats['Average Duration (Hours)'] = (feeder_stats['Avg_Mins'] / 60).round(2)
#         feeder_stats = feeder_stats.drop(columns=['Avg_Mins', 'Total_Mins'])

#         notorious = notorious.merge(feeder_stats, on=['Circle', 'Feeder']).sort_values(by=['Circle', 'Days with Outages', 'Total Outage Events'], ascending=[True, False, False])
#         top_5_notorious = notorious.groupby('Circle').head(5)
#         notorious_set = set(zip(top_5_notorious['Circle'], top_5_notorious['Feeder']))
#     else:
#         top_5_notorious = pd.DataFrame(columns=['Circle', 'Feeder'])
#         notorious_set = set()

#     tab1, tab2, tab3 = st.tabs(["📊 Dashboard", "📈 YoY Comparison", "🛠️ PTW Frequency"])

#     # --- TAB 3: PTW FREQUENCY ---
#     with tab3:
#         st.header(f"🛠️ PTW Frequency Tracker ({date_preset})")
#         st.markdown(f"Identifies specific feeders that had a Permit to Work (PTW) taken against them **two or more times** in separate requests over the selected period.")

#         if df_ptw.empty:
#             st.info("No PTW data found for the selected period.")
#         else:
#             ptw_col = next((c for c in df_ptw.columns if 'ptw' in c.lower() or 'request' in c.lower() or 'id' in c.lower()), None)
#             feeder_col = next((c for c in df_ptw.columns if 'feeder' in c.lower()), None)
#             status_col = next((c for c in df_ptw.columns if 'status' in c.lower()), None)
#             circle_col = next((c for c in df_ptw.columns if 'circle' in c.lower()), None)

#             if not ptw_col or not feeder_col:
#                 st.error("Could not dynamically map required columns from the PTW export.")
#             else:
#                 ptw_clean = df_ptw.copy()
#                 if status_col:
#                     ptw_clean = ptw_clean[~ptw_clean[status_col].astype(str).str.contains('Cancellation', na=False, case=False)]

#                 ptw_clean[feeder_col] = ptw_clean[feeder_col].astype(str).str.replace('|', ',', regex=False)
#                 ptw_clean[feeder_col] = ptw_clean[feeder_col].str.split(',')
                
#                 ptw_clean = ptw_clean.explode(feeder_col).reset_index(drop=True)
                
#                 ptw_clean[feeder_col] = ptw_clean[feeder_col].str.strip()
#                 ptw_clean = ptw_clean[ptw_clean[feeder_col] != '']

#                 group_cols = [feeder_col]
#                 if circle_col: group_cols.insert(0, circle_col)
                    
#                 ptw_counts = ptw_clean.groupby(group_cols).agg(Unique_PTWs=(ptw_col, 'nunique'), PTW_IDs=(ptw_col, lambda x: ', '.join(x.dropna().astype(str).unique()))).reset_index()
#                 repeat_feeders = ptw_counts[ptw_counts['Unique_PTWs'] >= 2].sort_values(by='Unique_PTWs', ascending=False).reset_index(drop=True)
#                 repeat_feeders = repeat_feeders.rename(columns={'Unique_PTWs': 'PTW Request Count', 'PTW_IDs': 'Associated PTW Request Numbers'})

#                 kpi1, kpi2 = st.columns(2)
#                 with kpi1: st.markdown(f'<div class="kpi-card"><div><div class="kpi-title">Total Active PTW Requests</div><div class="kpi-value">{df_ptw[ptw_col].nunique()}</div></div><div class="kpi-subtext"><span class="status-badge">Selected Period</span></div></div>', unsafe_allow_html=True)
#                 with kpi2: st.markdown(f'<div class="kpi-card"><div><div class="kpi-title">Feeders with Multiple PTWs</div><div class="kpi-value">{len(repeat_feeders)}</div></div><div class="kpi-subtext"><span class="status-badge" style="background-color: #D32F2F;">🔴 Needs Review</span></div></div>', unsafe_allow_html=True)

#                 st.divider()
#                 st.subheader("⚠️ Repeat PTW Feeders Detail View")
#                 if not repeat_feeders.empty:
#                     st.dataframe(repeat_feeders.style.set_table_styles(HEADER_STYLES), width="stretch", hide_index=True)
#                 else:
#                     st.success("No feeders had multiple PTWs requested against them in the selected period! 🎉")

#                 st.divider()
#                 st.subheader("⏳ Specific Day PTW Requests (Detailed Breakdown)")
                
#                 start_col_ptw = next((c for c in df_ptw.columns if ('start' in c.lower() or 'from' in c.lower()) and ('date' in c.lower() or 'time' in c.lower())), None)
#                 end_col_ptw = next((c for c in df_ptw.columns if ('end' in c.lower() or 'to' in c.lower()) and ('date' in c.lower() or 'time' in c.lower())), None)

#                 if start_col_ptw and end_col_ptw:
#                     today_ptws = ptw_clean.copy()
#                     today_ptws[start_col_ptw] = pd.to_datetime(today_ptws[start_col_ptw], dayfirst=True, errors='coerce')
#                     today_ptws[end_col_ptw] = pd.to_datetime(today_ptws[end_col_ptw], dayfirst=True, errors='coerce')
                    
#                     req_date_col = next((c for c in df_ptw.columns if 'request' in c.lower() and ('date' in c.lower() or 'time' in c.lower())), None)
#                     if req_date_col:
#                         today_ptws[req_date_col] = pd.to_datetime(today_ptws[req_date_col], dayfirst=True, errors='coerce')
#                         mask = (today_ptws[start_col_ptw].dt.date == pd.to_datetime(end_str).date()) | \
#                                (today_ptws[req_date_col].dt.date == pd.to_datetime(end_str).date())
#                     else:
#                         mask = (today_ptws[start_col_ptw].dt.date == pd.to_datetime(end_str).date())
                    
#                     today_ptws = today_ptws[mask]
                    
#                     if not today_ptws.empty:
#                         today_ptws['Duration (Hours)'] = (today_ptws[end_col_ptw] - today_ptws[start_col_ptw]).dt.total_seconds() / 3600.0
#                         today_ptws['Duration (Hours)'] = today_ptws['Duration (Hours)'].apply(lambda x: max(x, 0)).round(2)
                        
#                         def ptw_bucket(hrs):
#                             if pd.isna(hrs): return "Unknown"
#                             if hrs <= 2: return "0-2 Hrs"
#                             elif hrs <= 4: return "2-4 Hrs"
#                             elif hrs <= 8: return "4-8 Hrs"
#                             else: return "Above 8 Hrs"
                        
#                         today_ptws['Time Bucket'] = today_ptws['Duration (Hours)'].apply(ptw_bucket)
                        
#                         display_cols_ptw = [feeder_col, start_col_ptw, end_col_ptw, 'Duration (Hours)', 'Time Bucket']
#                         if circle_col: display_cols_ptw.insert(0, circle_col)
                        
#                         final_today_ptws = today_ptws[display_cols_ptw].dropna(subset=[start_col_ptw]).sort_values(by='Duration (Hours)', ascending=False).reset_index(drop=True)
                        
#                         def highlight_long_ptw(row):
#                             if pd.notna(row['Duration (Hours)']) and row['Duration (Hours)'] > 5:
#                                 return ['background-color: rgba(220, 53, 69, 0.15); color: #850000; font-weight: bold'] * len(row)
#                             return [''] * len(row)
                            
#                         over_5_count = final_today_ptws[final_today_ptws['Duration (Hours)'] > 5][feeder_col].nunique()
#                         st.markdown(f"**Total Feeders under PTW on {end_str} exceeding 5 Hours:** `{over_5_count}`")
                        
#                         st.dataframe(final_today_ptws.style.apply(highlight_long_ptw, axis=1).format({'Duration (Hours)': '{:.2f}'}).set_table_styles(HEADER_STYLES), width="stretch", hide_index=True)
#                     else:
#                         st.info(f"No PTW requests recorded specifically for {end_str}.")
#                 else:
#                     st.warning("Could not dynamically identify Start and End time columns in the PTW report. Check if End Date is missing from API.")

#     # --- TAB 2: YOY DRILL-DOWN ---
#     with tab2:
#         st.header("📈 Historical Year-over-Year Drilldown")
        
#         if df_hist_curr.empty or df_hist_ly.empty:
#             st.error("Historical Master Data (Historical_2025.csv & Historical_2026.csv) not found in directory.")
#         else:
#             # Display Selected Period Dynamically
#             st.markdown(f"**Comparing Period:** {start_date.strftime('%d %b %Y')} to {end_date.strftime('%d %b %Y')}")
#             st.divider()

#             # Dynamic LY Calculation (Handles leap years gracefully)
#             def get_ly_date(d):
#                 try:
#                     return d.replace(year=d.year - 1)
#                 except ValueError:
#                     return d.replace(year=d.year - 1, day=28)

#             ly_start = get_ly_date(start_date)
#             ly_end = get_ly_date(end_date)
            
#             mask_curr = (df_hist_curr['Outage Date'] >= start_date) & (df_hist_curr['Outage Date'] <= end_date)
#             filtered_curr = df_hist_curr[mask_curr]
            
#             mask_ly = (df_hist_ly['Outage Date'] >= ly_start) & (df_hist_ly['Outage Date'] <= ly_end)
#             filtered_ly = df_hist_ly[mask_ly]

#             st.markdown(f"### 📍 1. Zone-wise Distribution")
#             st.caption("Includes total counts, total hours, and average hours. Click any row to drill down.")
            
#             yoy_zone = generate_yoy_dist_expanded(filtered_curr, filtered_ly, 'Zone')
            
#             zone_selection = st.dataframe(
#                 yoy_zone.style.map(highlight_delta, subset=['YoY Delta (Total)']).format(precision=2).set_table_styles(HEADER_STYLES), 
#                 width="stretch", hide_index=True, on_select="rerun", selection_mode="single-row"
#             )

#             if len(zone_selection.selection.rows) > 0:
#                 selected_zone = yoy_zone.iloc[zone_selection.selection.rows[0]]['Zone']
                
#                 st.markdown(f"### 🎯 2. Circle-wise Distribution for **{selected_zone}**")
#                 st.caption("Click any row to drill down into Feeder-wise data.")
                
#                 curr_zone_df = filtered_curr[filtered_curr['Zone'] == selected_zone]
#                 ly_zone_df = filtered_ly[filtered_ly['Zone'] == selected_zone]
                
#                 yoy_circle = generate_yoy_dist_expanded(curr_zone_df, ly_zone_df, 'Circle')
                
#                 circle_selection = st.dataframe(
#                     yoy_circle.style.map(highlight_delta, subset=['YoY Delta (Total)']).format(precision=2).set_table_styles(HEADER_STYLES), 
#                     width="stretch", hide_index=True, on_select="rerun", selection_mode="single-row"
#                 )

#                 if len(circle_selection.selection.rows) > 0:
#                     selected_circle = yoy_circle.iloc[circle_selection.selection.rows[0]]['Circle']
#                     st.markdown(f"### 🔌 3. Feeder-wise Distribution for **{selected_circle}**")
                    
#                     curr_circle_df = curr_zone_df[curr_zone_df['Circle'] == selected_circle]
#                     ly_circle_df = ly_zone_df[ly_zone_df['Circle'] == selected_circle]
                    
#                     yoy_feeder = generate_yoy_dist_expanded(curr_circle_df, ly_circle_df, 'Feeder')
#                     st.dataframe(yoy_feeder.style.map(highlight_delta, subset=['YoY Delta (Total)']).format(precision=2).set_table_styles(HEADER_STYLES), width="stretch", hide_index=True)


#     # --- TAB 1: ORIGINAL DASHBOARD ---
#     with tab1:
#         if not df_today.empty:
#             valid_today = df_today[~df_today['Status'].astype(str).str.contains('Cancel', case=False, na=False)]
#         else:
#             valid_today = pd.DataFrame(columns=df_today.columns)
            
#         if not df_5day.empty:
#             valid_5day = df_5day[~df_5day['Status'].astype(str).str.contains('Cancel', case=False, na=False)]
#         else:
#             valid_5day = pd.DataFrame(columns=df_5day.columns)

#         col_left, col_right = st.columns(2, gap="large")

#         with col_left:
#             st.header(f"📅 Selected End Date ({pd.to_datetime(end_str).strftime('%d %b %Y')})")
            
#             today_planned = valid_today[valid_today['Type of Outage'] == 'Planned Outage'] 
#             today_popc = valid_today[valid_today['Type of Outage'] == 'Power Off By PC'] 
#             today_unplanned = valid_today[valid_today['Type of Outage'] == 'Unplanned Outage'] 
            
#             st.subheader("Outage Summary")
#             kpi1, kpi2, kpi3 = st.columns(3)
#             with kpi1:
#                 active_p, closed_p = (len(today_planned[today_planned['Status_Calc'] == 'Active']), len(today_planned[today_planned['Status_Calc'] == 'Closed'])) if not today_planned.empty else (0,0)
#                 st.markdown(f'<div class="kpi-card"><div><div class="kpi-title">Planned Outages</div><div class="kpi-value">{len(today_planned)}</div></div><div class="kpi-subtext"><span class="status-badge">🔴 Active: {active_p}</span> <span class="status-badge">🟢 Closed: {closed_p}</span></div></div>', unsafe_allow_html=True)
#             with kpi2:
#                 active_po, closed_po = (len(today_popc[today_popc['Status_Calc'] == 'Active']), len(today_popc[today_popc['Status_Calc'] == 'Closed'])) if not today_popc.empty else (0,0)
#                 st.markdown(f'<div class="kpi-card"><div><div class="kpi-title">Power Off By PC</div><div class="kpi-value">{len(today_popc)}</div></div><div class="kpi-subtext"><span class="status-badge">🔴 Active: {active_po}</span> <span class="status-badge">🟢 Closed: {closed_po}</span></div></div>', unsafe_allow_html=True)
#             with kpi3:
#                 active_u, closed_u = (len(today_unplanned[today_unplanned['Status_Calc'] == 'Active']), len(today_unplanned[today_unplanned['Status_Calc'] == 'Closed'])) if not today_unplanned.empty else (0,0)
#                 st.markdown(f'<div class="kpi-card"><div><div class="kpi-title">Unplanned Outages</div><div class="kpi-value">{len(today_unplanned)}</div></div><div class="kpi-subtext"><span class="status-badge">🔴 Active: {active_u}</span> <span class="status-badge">🟢 Closed: {closed_u}</span></div></div>', unsafe_allow_html=True)

#             st.divider()
#             st.subheader("Zone-wise Distribution")
#             if not valid_today.empty:
#                 zone_today = valid_today.groupby(['Zone', 'Type of Outage']).size().unstack(fill_value=0).reset_index()
#                 for col in ['Planned Outage', 'Power Off By PC', 'Unplanned Outage']:
#                     if col not in zone_today: zone_today[col] = 0
#                 zone_today['Total'] = zone_today['Planned Outage'] + zone_today['Power Off By PC'] + zone_today['Unplanned Outage']
                
#                 styled_zone_today = apply_pu_gradient(zone_today.style, zone_today).set_table_styles(HEADER_STYLES)
#                 st.dataframe(styled_zone_today, width="stretch", hide_index=True)
#             else: st.info(f"No data available for {end_str}.")

#         with col_right:
#             st.header(f"⏳ Trend View ({date_preset})")
            
#             fiveday_planned = valid_5day[valid_5day['Type of Outage'] == 'Planned Outage'] 
#             fiveday_popc = valid_5day[valid_5day['Type of Outage'] == 'Power Off By PC'] 
#             fiveday_unplanned = valid_5day[valid_5day['Type of Outage'] == 'Unplanned Outage'] 
            
#             st.subheader(f"Outage Summary ({date_preset})")
#             kpi4, kpi5, kpi6 = st.columns(3)
#             with kpi4: st.markdown(f'<div class="kpi-card"><div><div class="kpi-title">Planned Outages</div><div class="kpi-value">{len(fiveday_planned)}</div></div><div class="kpi-subtext" style="visibility: hidden;">Spacer</div></div>', unsafe_allow_html=True)
#             with kpi5: st.markdown(f'<div class="kpi-card"><div><div class="kpi-title">Power Off By PC</div><div class="kpi-value">{len(fiveday_popc)}</div></div><div class="kpi-subtext" style="visibility: hidden;">Spacer</div></div>', unsafe_allow_html=True)
#             with kpi6: st.markdown(f'<div class="kpi-card"><div><div class="kpi-title">Unplanned Outages</div><div class="kpi-value">{len(fiveday_unplanned)}</div></div><div class="kpi-subtext" style="visibility: hidden;">Spacer</div></div>', unsafe_allow_html=True)

#             st.divider()
#             st.subheader(f"Zone-wise Distribution ({date_preset})")
#             if not valid_5day.empty:
#                 zone_5day = valid_5day.groupby(['Zone', 'Type of Outage']).size().unstack(fill_value=0).reset_index()
#                 for col in ['Planned Outage', 'Power Off By PC', 'Unplanned Outage']:
#                     if col not in zone_5day: zone_5day[col] = 0
#                 zone_5day['Total'] = zone_5day['Planned Outage'] + zone_5day['Power Off By PC'] + zone_5day['Unplanned Outage']
                
#                 styled_zone_5day = apply_pu_gradient(zone_5day.style, zone_5day).set_table_styles(HEADER_STYLES)
#                 st.dataframe(styled_zone_5day, width="stretch", hide_index=True)
#             else: st.info("No data available for the selected period.")

#         st.divider()
#         st.header(f"🚨 Notorious Feeders (3+ Days of Outages in {date_preset})")
#         st.caption("Top 5 worst-performing feeders per circle based on continuous outage days.")

#         noto_col1, noto_col2 = st.columns(2)
#         with noto_col1: selected_notorious_circle = st.selectbox("Filter by Circle:", ["All Circles"] + sorted(top_5_notorious['Circle'].unique().tolist()) if not top_5_notorious.empty else ["All Circles"], index=0)
#         with noto_col2: selected_notorious_type = st.selectbox("Filter by Outage Type:", ["All Types", "Planned Outage", "Power Off By PC", "Unplanned Outage"], index=0)

#         df_dyn = valid_5day.copy()
#         if selected_notorious_type != "All Types" and not df_dyn.empty: 
#             df_dyn = df_dyn[df_dyn['Type of Outage'] == selected_notorious_type]

#         if not df_dyn.empty:
#             dyn_days = df_dyn.groupby(['Circle', 'Feeder'])['Outage Date'].nunique().reset_index(name='Days with Outages')
#             dyn_noto = dyn_days[dyn_days['Days with Outages'] >= 3]

#             if not dyn_noto.empty:
#                 dyn_stats = df_dyn.groupby(['Circle', 'Feeder']).agg(Total_Events=('Start Time', 'size'), Avg_Mins=('Diff in mins', 'mean'), Total_Mins=('Diff in mins', 'sum')).reset_index()
#                 dyn_stats.rename(columns={'Total_Events': 'Total Outage Events'}, inplace=True)
#                 dyn_stats['Total Duration (Hours)'] = (dyn_stats['Total_Mins'] / 60).round(2)
#                 dyn_stats['Average Duration (Hours)'] = (dyn_stats['Avg_Mins'] / 60).round(2)
#                 dyn_stats = dyn_stats.drop(columns=['Avg_Mins', 'Total_Mins'])

#                 dyn_noto = dyn_noto.merge(dyn_stats, on=['Circle', 'Feeder']).sort_values(by=['Circle', 'Days with Outages', 'Total Outage Events'], ascending=[True, False, False])
#                 dyn_top5 = dyn_noto.groupby('Circle').head(5)
#                 filtered_notorious = dyn_top5[dyn_top5['Circle'] == selected_notorious_circle] if selected_notorious_circle != "All Circles" else dyn_top5

#                 if not filtered_notorious.empty:
#                     st.dataframe(filtered_notorious.style.format({'Average Duration (Hours)': '{:.2f}', 'Total Duration (Hours)': '{:.2f}'}).set_table_styles(HEADER_STYLES), width="stretch", hide_index=True)
#                 else: st.info(f"No notorious feeders found for {selected_notorious_circle} matching the criteria.")
#             else: st.info(f"No notorious feeders identified for {selected_notorious_type}.")
#         else: st.info("No data available for the selected criteria.")

#         st.divider()
#         st.header("Comprehensive Circle-wise Breakdown")
#         bucket_order = ["Up to 2 Hrs", "2-4 Hrs", "4-8 Hrs", "Above 8 Hrs", "Active/Unknown"]

#         curr_1d_p_tab1 = create_bucket_pivot(today_planned, bucket_order)
#         curr_1d_po_tab1 = create_bucket_pivot(today_popc, bucket_order)
#         curr_1d_u_tab1 = create_bucket_pivot(today_unplanned, bucket_order)
#         curr_5d_p_tab1 = create_bucket_pivot(fiveday_planned, bucket_order)
#         curr_5d_po_tab1 = create_bucket_pivot(fiveday_popc, bucket_order)
#         curr_5d_u_tab1 = create_bucket_pivot(fiveday_unplanned, bucket_order)

#         combined_circle = pd.concat(
#             [curr_1d_p_tab1, curr_1d_po_tab1, curr_1d_u_tab1, curr_5d_p_tab1, curr_5d_po_tab1, curr_5d_u_tab1], 
#             axis=1, 
#             keys=['END DATE (Planned)', 'END DATE (Power Off)', 'END DATE (Unplanned)', 'RANGE (Planned)', 'RANGE (Power Off)', 'RANGE (Unplanned)']
#         ).fillna(0).astype(int)

#         st.markdown(" **Click on any row inside the table below** to view the specific Feeder drill-down details.")

#         if not combined_circle.empty:
#             styled_combined = apply_pu_gradient(combined_circle.style, combined_circle).set_table_styles(HEADER_STYLES)
            
#             selection_event = st.dataframe(
#                 styled_combined, 
#                 width="stretch",
#                 on_select="rerun",
#                 selection_mode="single-row" 
#             )

#             if len(selection_event.selection.rows) > 0:
#                 selected_circle = combined_circle.index[selection_event.selection.rows[0]]
#                 st.subheader(f"Feeder Details for: {selected_circle}")
                
#                 circle_dates = sorted(list(valid_5day[valid_5day['Circle'] == selected_circle]['Outage Date'].dropna().unique()))
#                 selected_dates = st.multiselect("Filter Range View by Date:", options=circle_dates, default=circle_dates, format_func=lambda x: x.strftime('%d %b %Y'))
                
#                 def highlight_notorious(row): return ['background-color: rgba(220, 53, 69, 0.15); color: #850000; font-weight: bold'] * len(row) if (selected_circle, row['Feeder']) in notorious_set else [''] * len(row)

#                 st.write("---")
#                 st.markdown(f"### 🔴 SINGLE DAY DRILLDOWN ({end_str})")
#                 today_left, today_mid, today_right = st.columns(3)
#                 with today_left:
#                     st.markdown(f"**Planned Outages**")
#                     feeder_list_tp = today_planned[today_planned['Circle'] == selected_circle][['Feeder', 'Diff in mins', 'Status_Calc', 'Duration Bucket']].rename(columns={'Status_Calc': 'Status'}) if not today_planned.empty else pd.DataFrame(columns=['Feeder', 'Diff in mins', 'Status', 'Duration Bucket'])
#                     st.dataframe(feeder_list_tp.style.apply(highlight_notorious, axis=1).set_table_styles(HEADER_STYLES), width="stretch", hide_index=True)
#                 with today_mid:
#                     st.markdown(f"**Power Off By PC**")
#                     feeder_list_tpo = today_popc[today_popc['Circle'] == selected_circle][['Feeder', 'Diff in mins', 'Status_Calc', 'Duration Bucket']].rename(columns={'Status_Calc': 'Status'}) if not today_popc.empty else pd.DataFrame(columns=['Feeder', 'Diff in mins', 'Status', 'Duration Bucket'])
#                     st.dataframe(feeder_list_tpo.style.apply(highlight_notorious, axis=1).set_table_styles(HEADER_STYLES), width="stretch", hide_index=True)
#                 with today_right:
#                     st.markdown(f"**Unplanned Outages**")
#                     feeder_list_tu = today_unplanned[today_unplanned['Circle'] == selected_circle][['Feeder', 'Diff in mins', 'Status_Calc', 'Duration Bucket']].rename(columns={'Status_Calc': 'Status'}) if not today_unplanned.empty else pd.DataFrame(columns=['Feeder', 'Diff in mins', 'Status', 'Duration Bucket'])
#                     st.dataframe(feeder_list_tu.style.apply(highlight_notorious, axis=1).set_table_styles(HEADER_STYLES), width="stretch", hide_index=True)
                    
#                 st.write("---") 
#                 st.markdown(f"### 🟢 SELECTED RANGE DRILLDOWN")
#                 fiveday_left, fiveday_mid, fiveday_right = st.columns(3)
                
#                 with fiveday_left:
#                     st.markdown(f"**Planned Outages**")
#                     feeder_list_fp = fiveday_planned[(fiveday_planned['Circle'] == selected_circle) & (fiveday_planned['Outage Date'].isin(selected_dates))].copy() if not fiveday_planned.empty else pd.DataFrame()
#                     if not feeder_list_fp.empty:
#                         feeder_list_fp['Diff in Hours'] = (feeder_list_fp['Diff in mins'] / 60).round(2)
#                         st.dataframe(feeder_list_fp[['Outage Date', 'Start Time', 'Feeder', 'Diff in Hours', 'Duration Bucket']].style.apply(highlight_notorious, axis=1).format({'Diff in Hours': '{:.2f}'}).set_table_styles(HEADER_STYLES), width="stretch", hide_index=True)
#                     else: st.dataframe(pd.DataFrame(columns=['Outage Date', 'Start Time', 'Feeder', 'Diff in Hours', 'Duration Bucket']).style.set_table_styles(HEADER_STYLES), width="stretch", hide_index=True)
                    
#                 with fiveday_mid:
#                     st.markdown(f"**Power Off By PC**")
#                     feeder_list_fpo = fiveday_popc[(fiveday_popc['Circle'] == selected_circle) & (fiveday_popc['Outage Date'].isin(selected_dates))].copy() if not fiveday_popc.empty else pd.DataFrame()
#                     if not feeder_list_fpo.empty:
#                         feeder_list_fpo['Diff in Hours'] = (feeder_list_fpo['Diff in mins'] / 60).round(2)
#                         st.dataframe(feeder_list_fpo[['Outage Date', 'Start Time', 'Feeder', 'Diff in Hours', 'Duration Bucket']].style.apply(highlight_notorious, axis=1).format({'Diff in Hours': '{:.2f}'}).set_table_styles(HEADER_STYLES), width="stretch", hide_index=True)
#                     else: st.dataframe(pd.DataFrame(columns=['Outage Date', 'Start Time', 'Feeder', 'Diff in Hours', 'Duration Bucket']).style.set_table_styles(HEADER_STYLES), width="stretch", hide_index=True)

#                 with fiveday_right:
#                     st.markdown(f"**Unplanned Outages**")
#                     feeder_list_fu = fiveday_unplanned[(fiveday_unplanned['Circle'] == selected_circle) & (fiveday_unplanned['Outage Date'].isin(selected_dates))].copy() if not fiveday_unplanned.empty else pd.DataFrame()
#                     if not feeder_list_fu.empty:
#                         feeder_list_fu['Diff in Hours'] = (feeder_list_fu['Diff in mins'] / 60).round(2)
#                         st.dataframe(feeder_list_fu[['Outage Date', 'Start Time', 'Feeder', 'Diff in Hours', 'Duration Bucket']].style.apply(highlight_notorious, axis=1).format({'Diff in Hours': '{:.2f}'}).set_table_styles(HEADER_STYLES), width="stretch", hide_index=True)
#                     else: st.dataframe(pd.DataFrame(columns=['Outage Date', 'Start Time', 'Feeder', 'Diff in Hours', 'Duration Bucket']).style.set_table_styles(HEADER_STYLES), width="stretch", hide_index=True)
#         else: st.info("No circle data available.")

# # --- ROUTER LOGIC ---
# if st.session_state.page == 'home':
#     render_home()
# elif st.session_state.page == 'dashboard':
#     render_dashboard()
# elif st.session_state.page == 'ptw_app':
#     render_ptw_lm_dashboard()

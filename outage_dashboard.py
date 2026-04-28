# # # # =========================================================================================================================
# # # # V4 - working API AWS based. 
# # # # =========================================================================================================================
import streamlit as st
import pandas as pd
from datetime import datetime, timedelta, timezone, date

# ─────────────────────────────────────────────────────────────
# PAGE CONFIGURATION
# ─────────────────────────────────────────────────────────────
st.set_page_config(page_title="Power Outage Monitoring Dashboard", layout="wide")

# ─────────────────────────────────────────────────────────────
# GLOBAL TABLE HEADER STYLING
# ─────────────────────────────────────────────────────────────
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

# ─────────────────────────────────────────────────────────────
# COLOR THEME & ENTERPRISE CSS
# ─────────────────────────────────────────────────────────────
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

# ─────────────────────────────────────────────────────────────
# IST TIMEZONE
# ─────────────────────────────────────────────────────────────
IST     = timezone(timedelta(hours=5, minutes=30))
now_ist = datetime.now(IST)

# ─────────────────────────────────────────────────────────────
# S3 CSV URLs
# ─────────────────────────────────────────────────────────────
OUTAGES_URL = "https://pspcl-dashboard-data.s3.ap-south-1.amazonaws.com/outages.csv"
PTW_URL     = "https://pspcl-dashboard-data.s3.ap-south-1.amazonaws.com/ptw_requests.csv"

OUTAGES_COLS = [
    "outage_id", "zone_name", "circle_name", "feeder_name",
    "outage_type", "outage_status", "start_time", "end_time",
    "duration_minutes", "created_time"
]
PTW_COLS = [
    "ptw_id", "circle_name", "feeders", "current_status", "creation_date"
]

# ─────────────────────────────────────────────────────────────
# DATA LOADING
# ─────────────────────────────────────────────────────────────
@st.cache_data(ttl=3600)
def load_data():
    with st.spinner("⏳ Loading data from PSPCL database..."):
        df_outages = pd.read_csv(
            OUTAGES_URL,
            usecols=OUTAGES_COLS,
            low_memory=False,
            dtype={
                "outage_id":        "str",
                "zone_name":        "category",
                "circle_name":      "category",
                "feeder_name":      "category",
                "outage_type":      "category",
                "outage_status":    "category",
                "duration_minutes": "float32",
            },
            parse_dates=["start_time", "end_time", "created_time"]
        )
        df_ptw = pd.read_csv(
            PTW_URL,
            usecols=PTW_COLS,
            low_memory=False,
            dtype={
                "ptw_id":         "str",
                "circle_name":    "category",
                "current_status": "category",
            },
            parse_dates=["creation_date"]
        )
    return df_outages, df_ptw

df_outages_raw, df_ptw_raw = load_data()

# ─────────────────────────────────────────────────────────────
# CLEAN OUTAGE DATA
# ─────────────────────────────────────────────────────────────
def clean_outage_data(df):
    if df.empty:
        return df
    df = df.copy()
    if 'outage_status' in df.columns:
        df = df[~df['outage_status'].astype(str).str.contains('Cancel', na=False, case=False)]
        df['status_calc'] = df['outage_status'].apply(
            lambda x: 'Active' if str(x).strip().upper() in ['OPEN', 'ACTIVE'] else 'Closed'
        )
    if 'duration_minutes' in df.columns:
        df['duration_minutes'] = pd.to_numeric(df['duration_minutes'], errors='coerce').fillna(0)
        def assign_bucket(mins):
            if mins < 0: return "Active/Unknown"
            hrs = mins / 60
            if hrs <= 2:   return "Up to 2 Hrs"
            elif hrs <= 4: return "2-4 Hrs"
            elif hrs <= 8: return "4-8 Hrs"
            else:          return "Above 8 Hrs"
        df['duration_bucket'] = df['duration_minutes'].apply(assign_bucket)
    if 'start_time' in df.columns:
        df['outage_date'] = pd.to_datetime(df['start_time'], errors='coerce').dt.date
    return df

df_master     = clean_outage_data(df_outages_raw)
df_ptw_master = df_ptw_raw.copy()

if not df_ptw_master.empty and 'ptw_id' in df_ptw_master.columns:
    df_ptw_master = df_ptw_master.drop_duplicates(subset=['ptw_id'], keep='last')

# ─────────────────────────────────────────────────────────────
# HELPER FUNCTIONS
# ─────────────────────────────────────────────────────────────
def safe_ly_date(dt):
    try:
        return dt.replace(year=dt.year - 1)
    except ValueError:
        return dt.replace(year=dt.year - 1, day=28)


def generate_yoy_dist_expanded(df_curr, df_ly, group_col):
    def _agg(df, prefix):
        if df.empty:
            return pd.DataFrame({group_col: []}).set_index(group_col)
        df = df.copy()
        df['duration_minutes'] = pd.to_numeric(df['duration_minutes'], errors='coerce').fillna(0)
        g = df.groupby([group_col, 'outage_type']).agg(
            Count=('outage_type', 'size'),
            TotalHrs=('duration_minutes', lambda x: round(x.sum() / 60, 2)),
            AvgHrs=('duration_minutes',   lambda x: round(x.mean() / 60, 2))
        ).unstack(fill_value=0)
        g.columns = [f"{prefix} {outage} ({metric})" for metric, outage in g.columns]
        return g

    c_grp  = _agg(df_curr, 'Curr')
    l_grp  = _agg(df_ly,   'LY')
    merged = pd.merge(c_grp, l_grp, on=group_col, how='outer').fillna(0).reset_index()

    expected_cols = []
    for prefix in ['Curr', 'LY']:
        for outage in ['Planned Outage', 'Unplanned Outage']:
            for metric in ['Count', 'TotalHrs', 'AvgHrs']:
                col_name = f"{prefix} {outage} ({metric})"
                expected_cols.append(col_name)
                if col_name not in merged.columns:
                    merged[col_name] = 0

    for col in expected_cols:
        if '(Count)' in col: merged[col] = merged[col].astype(int)
        else:                merged[col] = merged[col].astype(float).round(2)

    merged['Curr Total (Count)'] = merged['Curr Planned Outage (Count)'] + merged['Curr Unplanned Outage (Count)']
    merged['LY Total (Count)']   = merged['LY Planned Outage (Count)']   + merged['LY Unplanned Outage (Count)']
    merged['YoY Delta (Total)']  = merged['Curr Total (Count)'] - merged['LY Total (Count)']

    cols_order = [group_col,
                  'Curr Planned Outage (Count)', 'Curr Planned Outage (TotalHrs)', 'Curr Planned Outage (AvgHrs)',
                  'LY Planned Outage (Count)',   'LY Planned Outage (TotalHrs)',   'LY Planned Outage (AvgHrs)',
                  'Curr Unplanned Outage (Count)', 'Curr Unplanned Outage (TotalHrs)', 'Curr Unplanned Outage (AvgHrs)',
                  'LY Unplanned Outage (Count)', 'LY Unplanned Outage (TotalHrs)', 'LY Unplanned Outage (AvgHrs)',
                  'Curr Total (Count)', 'LY Total (Count)', 'YoY Delta (Total)']
    cols_order = [c for c in cols_order if c in merged.columns]
    merged     = merged[cols_order]

    if not merged.empty:
        gt_row = pd.Series(index=cols_order, dtype=object)
        gt_row[group_col] = 'Grand Total'
        for col in cols_order:
            if col == group_col: continue
            if '(Count)' in col or 'Delta' in col or '(TotalHrs)' in col:
                gt_row[col] = merged[col].sum()
        for prefix in ['Curr', 'LY']:
            for outage in ['Planned Outage', 'Unplanned Outage']:
                count_col = f"{prefix} {outage} (Count)"
                tot_col   = f"{prefix} {outage} (TotalHrs)"
                avg_col   = f"{prefix} {outage} (AvgHrs)"
                if all(c in cols_order for c in [count_col, tot_col, avg_col]):
                    gt_row[avg_col] = round(gt_row[tot_col] / gt_row[count_col], 2) if gt_row[count_col] > 0 else 0
        merged = pd.concat([merged, pd.DataFrame([gt_row])], ignore_index=True)

    return merged


def build_weekly_yoy_table(df_curr, df_ly, curr_yr, ly_yr):
    def _process(df, yr):
        if df.empty: return pd.DataFrame()
        d = df.copy()
        d['DateObj']     = pd.to_datetime(d['outage_date'])
        d['Month_Num']   = d['DateObj'].dt.month
        d['Week_Num']    = ((d['DateObj'].dt.day - 1) // 7) + 1
        d['Week_Label']  = d['DateObj'].dt.strftime('%b') + " w" + d['Week_Num'].astype(str)
        d['outage_type'] = d['outage_type'].astype(str).replace({'Power Off By PC': 'Power off by PC'})
        grp = d.groupby(['Month_Num', 'Week_Num', 'Week_Label', 'outage_type']).size().unstack(fill_value=0)
        return grp.rename(columns=lambda x: f"{x} ({yr})").reset_index()

    c_grp = _process(df_curr, curr_yr)
    l_grp = _process(df_ly,   ly_yr)

    if c_grp.empty and l_grp.empty: return pd.DataFrame()
    if c_grp.empty:   merged = l_grp
    elif l_grp.empty: merged = c_grp
    else: merged = pd.merge(c_grp, l_grp, on=['Month_Num', 'Week_Num', 'Week_Label'], how='outer').fillna(0)

    merged     = merged.sort_values(['Month_Num', 'Week_Num']).reset_index(drop=True)
    cols_order = ['Week_Label']
    outage_types = ['Planned Outage', 'Unplanned Outage', 'Power off by PC']
    pct_cols   = []

    for ot in outage_types:
        c_col   = f"{ot} ({curr_yr})"
        l_col   = f"{ot} ({ly_yr})"
        pct_col = f"{ot} (% Change)"
        pct_cols.append(pct_col)
        if c_col not in merged.columns: merged[c_col] = 0
        if l_col not in merged.columns: merged[l_col] = 0
        merged[c_col] = merged[c_col].astype(int)
        merged[l_col] = merged[l_col].astype(int)

        def calc_pct(row, c=c_col, l=l_col):
            cv, lv = row[c], row[l]
            if lv == 0 and cv == 0: return 0.0
            if lv == 0:             return 100.0
            return ((cv - lv) / lv) * 100.0

        merged[pct_col] = merged.apply(calc_pct, axis=1)
        cols_order.extend([c_col, l_col, pct_col])

    total_row = {'Week_Label': 'Grand Total', 'Month_Num': 99, 'Week_Num': 99}
    for ot in outage_types:
        c_col, l_col = f"{ot} ({curr_yr})", f"{ot} ({ly_yr})"
        pct_col      = f"{ot} (% Change)"
        c_sum, l_sum = merged[c_col].sum(), merged[l_col].sum()
        total_row[c_col] = c_sum
        total_row[l_col] = l_sum
        if l_sum == 0 and c_sum == 0:  total_row[pct_col] = 0.0
        elif l_sum == 0:               total_row[pct_col] = 100.0
        else:                          total_row[pct_col] = ((c_sum - l_sum) / l_sum) * 100.0

    merged = pd.concat([merged, pd.DataFrame([total_row])], ignore_index=True)
    for pct_col in pct_cols:
        merged[pct_col] = merged[pct_col].apply(lambda x: f"{x:+.1f}%" if pd.notnull(x) else "")

    return merged[cols_order].rename(columns={'Week_Label': 'Weekly'})


def apply_pu_gradient(styler, df):
    p_cols  = [c for c in df.columns if 'Planned'         in str(c) and pd.api.types.is_numeric_dtype(df[c])]
    u_cols  = [c for c in df.columns if 'Unplanned'       in str(c) and pd.api.types.is_numeric_dtype(df[c])]
    pc_cols = [c for c in df.columns if 'Power Off By PC' in str(c) and pd.api.types.is_numeric_dtype(df[c])]
    try:
        group_col = df.columns[0]
        row_idx   = df.index[:-1] if (not df.empty and df.iloc[-1][group_col] == 'Grand Total') else df.index
    except:
        row_idx = df.index
    if p_cols:  styler = styler.background_gradient(subset=pd.IndexSlice[row_idx, p_cols],  cmap='Blues',   vmin=0)
    if pc_cols: styler = styler.background_gradient(subset=pd.IndexSlice[row_idx, pc_cols], cmap='Purples', vmin=0)
    if u_cols:  styler = styler.background_gradient(subset=pd.IndexSlice[row_idx, u_cols],  cmap='Reds',    vmin=0)
    return styler


def highlight_delta(val):
    if isinstance(val, (int, float)):
        if val > 0:   return 'color: #D32F2F; font-weight: bold;'
        elif val < 0: return 'color: #388E3C; font-weight: bold;'
    return ''


def style_pct_change(val):
    if isinstance(val, str) and '%' in val:
        try:
            num = float(val.replace('%', '').replace('+', ''))
            if num > 0:   return 'color: #D32F2F; font-weight: bold;'
            elif num < 0: return 'color: #388E3C; font-weight: bold;'
        except ValueError:
            pass
    return ''


def create_bucket_pivot(df, bucket_order):
    if df.empty: return pd.DataFrame(columns=bucket_order + ['Total'])
    pivot = pd.crosstab(df['circle_name'], df['duration_bucket'])
    pivot = pivot.reindex(columns=[c for c in bucket_order if c in pivot.columns], fill_value=0)
    pivot['Total'] = pivot.sum(axis=1)
    return pivot


# ─────────────────────────────────────────────────────────────
# DATE SELECTOR WIDGET
# ─────────────────────────────────────────────────────────────
def handle_period_change(tab_key):
    period = st.session_state[f"{tab_key}_radio"]
    today  = now_ist.date()
    if period == "Today":
        st.session_state[f"{tab_key}_start_date"] = today
        st.session_state[f"{tab_key}_end_date"]   = today
    elif period == "Current Month":
        st.session_state[f"{tab_key}_start_date"] = today.replace(day=1)
        st.session_state[f"{tab_key}_end_date"]   = today
    elif period == "Last Month":
        first_of_this_month = today.replace(day=1)
        last_of_last_month  = first_of_this_month - timedelta(days=1)
        st.session_state[f"{tab_key}_start_date"] = last_of_last_month.replace(day=1)
        st.session_state[f"{tab_key}_end_date"]   = last_of_last_month
    elif period == "Last 3 Months":
        st.session_state[f"{tab_key}_start_date"] = today - timedelta(days=90)
        st.session_state[f"{tab_key}_end_date"]   = today
    elif period == "Last 6 Months":
        st.session_state[f"{tab_key}_start_date"] = today - timedelta(days=180)
        st.session_state[f"{tab_key}_end_date"]   = today


def render_date_selector(tab_key):
    st.markdown("📅 **Select Time Period:**")
    if f"{tab_key}_start_date" not in st.session_state:
        st.session_state[f"{tab_key}_start_date"] = now_ist.date()
        st.session_state[f"{tab_key}_end_date"]   = now_ist.date()

    period = st.radio(
        "Select Time Period",
        options=["Today", "Current Month", "Last Month", "Last 3 Months", "Last 6 Months", "Custom"],
        horizontal=True, label_visibility="collapsed",
        key=f"{tab_key}_radio", on_change=handle_period_change, args=(tab_key,)
    )
    col1, col2 = st.columns(2)
    with col1:
        start_date = st.date_input("From Date", format="DD/MM/YYYY",
                                   disabled=(period != "Custom"), key=f"{tab_key}_start_date")
    with col2:
        end_date   = st.date_input("To Date",   format="DD/MM/YYYY",
                                   disabled=(period != "Custom"), key=f"{tab_key}_end_date")
    return start_date, end_date


# ─────────────────────────────────────────────────────────────
# MAIN DASHBOARD
# ─────────────────────────────────────────────────────────────
st.title("⚡ Power Outage Monitoring Dashboard")
tab1, tab2, tab3 = st.tabs(["📊 Dashboard", "📈 YoY Comparison", "🛠️ PTW Frequency"])

# ==========================================
# TAB 1: DASHBOARD
# ==========================================
with tab1:
    st.header("📊 Outage Dashboard")
    start_d1, end_d1 = render_date_selector("tab1")
    st.divider()

    if not df_master.empty:
        mask_t1       = (df_master['outage_date'] >= start_d1) & (df_master['outage_date'] <= end_d1)
        filtered_tab1 = df_master[mask_t1].copy()
    else:
        filtered_tab1 = pd.DataFrame()

    if filtered_tab1.empty:
        st.info("No outage data found for the selected time period.")
    else:
        planned_df   = filtered_tab1[filtered_tab1['outage_type'] == 'Planned Outage']
        pc_df        = filtered_tab1[filtered_tab1['outage_type'] == 'Power Off By PC']
        unplanned_df = filtered_tab1[filtered_tab1['outage_type'] == 'Unplanned Outage']

        # --- KPI WIDGETS ---
        kpi1, kpi2, kpi3 = st.columns(3)
        with kpi1:
            active_p = len(planned_df[planned_df['status_calc'] == 'Active'])   if 'status_calc' in planned_df.columns   else 0
            closed_p = len(planned_df[planned_df['status_calc'] == 'Closed'])   if 'status_calc' in planned_df.columns   else len(planned_df)
            st.markdown(f'<div class="kpi-card"><div><div class="kpi-title">Planned Outages</div><div class="kpi-value">{len(planned_df)}</div></div><div class="kpi-subtext"><span class="status-badge">🔴 Active: {active_p}</span> <span class="status-badge">🟢 Closed: {closed_p}</span></div></div>', unsafe_allow_html=True)
        with kpi2:
            active_pc = len(pc_df[pc_df['status_calc'] == 'Active'])            if 'status_calc' in pc_df.columns        else 0
            closed_pc = len(pc_df[pc_df['status_calc'] == 'Closed'])            if 'status_calc' in pc_df.columns        else len(pc_df)
            st.markdown(f'<div class="kpi-card"><div><div class="kpi-title">Power Off By PC</div><div class="kpi-value">{len(pc_df)}</div></div><div class="kpi-subtext"><span class="status-badge">🔴 Active: {active_pc}</span> <span class="status-badge">🟢 Closed: {closed_pc}</span></div></div>', unsafe_allow_html=True)
        with kpi3:
            active_u  = len(unplanned_df[unplanned_df['status_calc'] == 'Active']) if 'status_calc' in unplanned_df.columns else 0
            closed_u  = len(unplanned_df[unplanned_df['status_calc'] == 'Closed']) if 'status_calc' in unplanned_df.columns else len(unplanned_df)
            st.markdown(f'<div class="kpi-card"><div><div class="kpi-title">Unplanned Outages</div><div class="kpi-value">{len(unplanned_df)}</div></div><div class="kpi-subtext"><span class="status-badge">🔴 Active: {active_u}</span> <span class="status-badge">🟢 Closed: {closed_u}</span></div></div>', unsafe_allow_html=True)

        st.divider()

        # --- ZONE-WISE DISTRIBUTION ---
        st.subheader("📍 Zone-wise Distribution")
        zone_df = filtered_tab1.groupby(['zone_name', 'outage_type']).size().unstack(fill_value=0).reset_index()
        zone_df.columns.name = None
        for col in ['Planned Outage', 'Power Off By PC', 'Unplanned Outage']:
            if col not in zone_df.columns: zone_df[col] = 0
        zone_df['Total'] = zone_df['Planned Outage'] + zone_df['Power Off By PC'] + zone_df['Unplanned Outage']
        zone_df = zone_df.rename(columns={'zone_name': 'Zone'})
        gt_row_zone         = pd.Series(zone_df.sum(numeric_only=True), name='Grand Total')
        gt_row_zone['Zone'] = 'Grand Total'
        zone_df = pd.concat([zone_df, pd.DataFrame([gt_row_zone])], ignore_index=True)
        st.dataframe(apply_pu_gradient(zone_df.style, zone_df).set_table_styles(HEADER_STYLES), width="stretch", hide_index=True)

        st.divider()

        # --- NOTORIOUS FEEDERS ---
        st.subheader("🚨 Notorious Feeders")
        total_days = (end_d1 - start_d1).days + 1

        if total_days == 1:
            noto_start_date = start_d1 - timedelta(days=2)
            noto_end_date   = end_d1
            noto_threshold  = 3
            st.caption(f"Single day selected. Showing feeders that had outages on **all 3 days** between {noto_start_date.strftime('%d %b')} and {noto_end_date.strftime('%d %b')}.")
        else:
            noto_start_date = start_d1
            noto_end_date   = end_d1
            noto_threshold  = max(3, round(total_days * (3 / 7)))
            st.caption(f"Range of {total_days} days selected. Applying 3-in-7 ratio: Feeders must have outages on at least **{noto_threshold} distinct days** to be flagged.")

        if not df_master.empty:
            mask_noto   = (df_master['outage_date'] >= noto_start_date) & (df_master['outage_date'] <= noto_end_date)
            dyn_noto_df = df_master[mask_noto].copy()
        else:
            dyn_noto_df = pd.DataFrame()

        noto_col1, noto_col2 = st.columns(2)
        all_circles = sorted(dyn_noto_df['circle_name'].dropna().unique().tolist()) if not dyn_noto_df.empty else []
        with noto_col1: selected_notorious_circle = st.selectbox("Filter by Circle:", ["All Circles"] + all_circles, index=0, key="noto_circ")
        with noto_col2: selected_notorious_type   = st.selectbox("Filter by Outage Type:", ["All Types", "Planned Outage", "Power Off By PC", "Unplanned Outage"], index=0, key="noto_type")

        if selected_notorious_type != "All Types":
            dyn_noto_df = dyn_noto_df[dyn_noto_df['outage_type'] == selected_notorious_type]

        global_notorious_set = set()
        if not dyn_noto_df.empty:
            dyn_days = dyn_noto_df.groupby(['circle_name', 'feeder_name'])['outage_date'].nunique().reset_index(name='Days with Outages')
            dyn_noto = dyn_days[dyn_days['Days with Outages'] >= noto_threshold]

            if not dyn_noto.empty:
                dyn_stats = dyn_noto_df.groupby(['circle_name', 'feeder_name']).agg(
                    Total_Events=('start_time', 'size'),
                    Max_Mins=('duration_minutes', 'max'),
                    Total_Mins=('duration_minutes', 'sum')
                ).reset_index()
                dyn_stats.rename(columns={'Total_Events': 'Total Outage Events'}, inplace=True)
                dyn_stats['Total Duration (Hours)'] = (dyn_stats['Total_Mins'] / 60).round(2)
                dyn_stats['Max Duration (Hours)']   = (dyn_stats['Max_Mins'] / 60).round(2)
                dyn_stats.drop(columns=['Max_Mins', 'Total_Mins'], inplace=True)

                dyn_noto = dyn_noto.merge(dyn_stats, on=['circle_name', 'feeder_name']).sort_values(
                    by=['circle_name', 'Days with Outages', 'Total Outage Events'], ascending=[True, False, False]
                )
                dyn_noto             = dyn_noto.rename(columns={'circle_name': 'Circle', 'feeder_name': 'Feeder'})
                dyn_top5             = dyn_noto.groupby('Circle').head(5)
                global_notorious_set = set(zip(dyn_top5['Circle'], dyn_top5['Feeder']))

                filtered_notorious = dyn_top5[dyn_top5['Circle'] == selected_notorious_circle] if selected_notorious_circle != "All Circles" else dyn_top5
                if not filtered_notorious.empty:
                    st.dataframe(filtered_notorious.style.format({'Max Duration (Hours)': '{:.2f}', 'Total Duration (Hours)': '{:.2f}'}).set_table_styles(HEADER_STYLES), width="stretch", hide_index=True)
                else:
                    st.info(f"No notorious feeders found for {selected_notorious_circle} matching the criteria.")
            else:
                st.info(f"No notorious feeders identified (no feeder hit the {noto_threshold}-day threshold). 🎉")
        else:
            st.info("No data available for the selected outage type/range.")

        # --- CIRCLE-WISE BREAKDOWN & DRILLDOWN ---
        st.subheader("🔌 Comprehensive Circle-wise Breakdown")
        st.markdown(" **Click on any row inside the table below** to view the specific Feeder drill-down details.")

        bucket_order = ["Up to 2 Hrs", "2-4 Hrs", "4-8 Hrs", "Above 8 Hrs", "Active/Unknown"]
        p_piv  = create_bucket_pivot(planned_df,   bucket_order)
        pc_piv = create_bucket_pivot(pc_df,        bucket_order)
        u_piv  = create_bucket_pivot(unplanned_df, bucket_order)

        circle_piv = pd.concat(
            [p_piv, pc_piv, u_piv], axis=1,
            keys=['Planned Outages', 'Power Off By PC', 'Unplanned Outages']
        ).fillna(0).astype(int)

        if not circle_piv.empty:
            circle_piv[('Overall Total', 'Total Events')] = circle_piv.loc[:, (slice(None), 'Total')].sum(axis=1)
            circle_piv.loc['Grand Total'] = circle_piv.sum(numeric_only=True)

            selection_circle = st.dataframe(
                apply_pu_gradient(circle_piv.style, circle_piv).set_table_styles(HEADER_STYLES),
                width="stretch", on_select="rerun", selection_mode="single-row"
            )

            if len(selection_circle.selection.rows) > 0:
                selected_circle = circle_piv.index[selection_circle.selection.rows[0]]
                if selected_circle != 'Grand Total':
                    st.markdown(f"#### 🔍 Feeder Details for: **{selected_circle}**")

                    def highlight_noto(row):
                        return ['background-color: rgba(220, 53, 69, 0.15); color: #850000; font-weight: bold'] * len(row) \
                               if (selected_circle, row['Feeder']) in global_notorious_set else [''] * len(row)

                    format_dict = {'Diff in Hours': '{:.2f}'}

                    def prep_feeder_df(df_sub):
                        if df_sub.empty:
                            return pd.DataFrame(columns=['Outage Date', 'Feeder', 'Diff in Hours', 'Status', 'Duration Bucket'])
                        res = df_sub[df_sub['circle_name'] == selected_circle][[
                            'outage_date', 'feeder_name', 'duration_minutes', 'status_calc', 'duration_bucket'
                        ]].rename(columns={
                            'outage_date':      'Outage Date',
                            'feeder_name':      'Feeder',
                            'duration_minutes': '_mins',
                            'status_calc':      'Status',
                            'duration_bucket':  'Duration Bucket'
                        }).copy()
                        res['Diff in Hours'] = (res['_mins'] / 60).round(2)
                        return res.drop(columns=['_mins'])

                    c_left, c_mid, c_right = st.columns(3)
                    with c_left:
                        st.markdown("**🔵 Planned Outages**")
                        st.dataframe(prep_feeder_df(planned_df).style.apply(highlight_noto, axis=1).format(format_dict).set_table_styles(HEADER_STYLES), width="stretch", hide_index=True)
                    with c_mid:
                        st.markdown("**🟣 Power Off By PC**")
                        st.dataframe(prep_feeder_df(pc_df).style.apply(highlight_noto, axis=1).format(format_dict).set_table_styles(HEADER_STYLES), width="stretch", hide_index=True)
                    with c_right:
                        st.markdown("**🔴 Unplanned Outages**")
                        st.dataframe(prep_feeder_df(unplanned_df).style.apply(highlight_noto, axis=1).format(format_dict).set_table_styles(HEADER_STYLES), width="stretch", hide_index=True)

# ==========================================
# TAB 2: YoY COMPARISON
# ==========================================
with tab2:
    st.header("📈 Historical Year-over-Year Comparison")
    start_d2, end_d2 = render_date_selector("tab2")
    st.divider()

    if df_master.empty:
        st.error("Outage data not available.")
    else:
        curr_year_str = str(now_ist.year)
        ly_year_str   = str(now_ist.year - 1)

        df_curr_year = df_master[pd.to_datetime(df_master['start_time'], errors='coerce').dt.year == now_ist.year]
        df_ly        = df_master[pd.to_datetime(df_master['start_time'], errors='coerce').dt.year == (now_ist.year - 1)]

        # TABLE 1: Selected date range weekly breakdown
        st.subheader("🔍 Selected Date Range Weekly Breakdown")
        ly_start_d2 = safe_ly_date(start_d2)
        ly_end_d2   = safe_ly_date(end_d2)
        mask_curr   = (df_curr_year['outage_date'] >= start_d2)    & (df_curr_year['outage_date'] <= end_d2)
        mask_ly     = (df_ly['outage_date']        >= ly_start_d2) & (df_ly['outage_date']        <= ly_end_d2)
        custom_table = build_weekly_yoy_table(df_curr_year[mask_curr], df_ly[mask_ly], curr_year_str, ly_year_str)

        if not custom_table.empty:
            pct_cols = [c for c in custom_table.columns if '% Change' in c]
            st.dataframe(custom_table.style.map(style_pct_change, subset=pct_cols).set_table_styles(HEADER_STYLES), width="stretch", hide_index=True)
        else:
            st.info("No data available for the selected date range.")

        st.divider()

        # TABLE 2: YTD auto-growing table
        st.subheader("📅 Year-to-Date Weekly Trend (Jan 1st - Today)")
        ytd_start     = date(now_ist.year, 1, 1)
        ytd_end       = now_ist.date()
        ly_ytd_start  = safe_ly_date(ytd_start)
        ly_ytd_end    = safe_ly_date(ytd_end)
        mask_ytd_curr = (df_curr_year['outage_date'] >= ytd_start)    & (df_curr_year['outage_date'] <= ytd_end)
        mask_ytd_ly   = (df_ly['outage_date']        >= ly_ytd_start) & (df_ly['outage_date']        <= ly_ytd_end)
        ytd_table     = build_weekly_yoy_table(df_curr_year[mask_ytd_curr], df_ly[mask_ytd_ly], curr_year_str, ly_year_str)

        if not ytd_table.empty:
            pct_cols = [c for c in ytd_table.columns if '% Change' in c]
            st.dataframe(ytd_table.style.map(style_pct_change, subset=pct_cols).set_table_styles(HEADER_STYLES), width="stretch", hide_index=True)
        else:
            st.info("No YTD data available.")

# ==========================================
# TAB 3: PTW FREQUENCY
# ==========================================
with tab3:
    st.header("🛠️ PTW Frequency Tracker")
    start_d3, end_d3 = render_date_selector("tab3")
    st.divider()

    if df_ptw_master.empty:
        st.info("No PTW data available in the current files.")
    else:
        df_ptw_master['Temp_Date'] = pd.to_datetime(df_ptw_master['creation_date'], errors='coerce').dt.date
        mask_ptw     = (df_ptw_master['Temp_Date'] >= start_d3) & (df_ptw_master['Temp_Date'] <= end_d3)
        filtered_ptw = df_ptw_master[mask_ptw].copy()

        if filtered_ptw.empty:
            st.warning("⚠️ No PTW data found for the selected time period. (Note: PTW data is available starting from 1st Jan 2025).")
        else:
            # ── Step 1: Remove cancelled permits ──────────────────────────
            filtered_ptw = filtered_ptw[
                ~filtered_ptw['current_status'].astype(str).str.contains('Cancellation', na=False, case=False)
            ]

            # ── Step 2: Compute KPI counts BEFORE exploding feeders ───────
            total_ptws = filtered_ptw["ptw_id"].nunique()

            truly_active = filtered_ptw[
                ~filtered_ptw['current_status'].astype(str).str.upper().isin(
                    ['CLOSED', 'COMPLETED', 'EXPIRED', 'REJECTED', 'CANCELLED', 'CANCELLATION']
                )
            ]["ptw_id"].nunique()

            # ── Step 3: Explode comma-separated feeders ───────────────────
            filtered_ptw['feeders'] = filtered_ptw['feeders'].astype(str).str.split(',')
            filtered_ptw = filtered_ptw.explode('feeders')
            filtered_ptw['feeders'] = filtered_ptw['feeders'].str.strip()
            filtered_ptw = filtered_ptw[filtered_ptw['feeders'].str.len() > 0]

            # ── Step 4: Build repeat feeders table ────────────────────────
            ptw_counts = filtered_ptw.groupby(['circle_name', 'feeders']).agg(
                Unique_PTWs=('ptw_id', 'nunique'),
                PTW_IDs=('ptw_id', lambda x: ', '.join(x.dropna().astype(str).unique()))
            ).reset_index()

            repeat_feeders = ptw_counts[ptw_counts['Unique_PTWs'] >= 2].sort_values(by='Unique_PTWs', ascending=False)
            repeat_feeders = repeat_feeders.rename(columns={
                'circle_name': 'Circle',
                'feeders':     'Feeder',
                'Unique_PTWs': 'PTW Request Count',
                'PTW_IDs':     'Associated PTW Request Numbers'
            })

            if not repeat_feeders.empty:
                gt_dict = {c: '' for c in repeat_feeders.columns}
                gt_dict['Circle']            = 'Grand Total'
                gt_dict['PTW Request Count'] = int(repeat_feeders['PTW Request Count'].sum())
                repeat_feeders = pd.concat([repeat_feeders, pd.DataFrame([gt_dict])], ignore_index=True)

            # ── Step 5: KPI Cards ─────────────────────────────────────────
            kpi1, kpi2 = st.columns(2)
            with kpi1:
                st.markdown(
                    f'<div class="kpi-card"><div>'
                    f'<div class="kpi-title">Total PTW Requests</div>'
                    f'<div class="kpi-value">{total_ptws}</div>'
                    f'</div><div class="kpi-subtext">'
                    f'<span class="status-badge">🟢 Active: {truly_active}</span>'
                    f'<span class="status-badge">⚫ Closed: {total_ptws - truly_active}</span>'
                    f'</div></div>',
                    unsafe_allow_html=True
                )
            with kpi2:
                multi_count = len(repeat_feeders) - 1 if not repeat_feeders.empty else 0
                st.markdown(
                    f'<div class="kpi-card"><div>'
                    f'<div class="kpi-title">Feeders with Multiple PTWs</div>'
                    f'<div class="kpi-value">{multi_count}</div>'
                    f'</div><div class="kpi-subtext">'
                    f'<span class="status-badge" style="background-color: #D32F2F;">🔴 Needs Review</span>'
                    f'</div></div>',
                    unsafe_allow_html=True
                )

            st.divider()
            st.subheader("⚠️ Repeat PTW Feeders Detail View")
            st.markdown("Identifies specific feeders that had a Permit to Work (PTW) taken against them **two or more times** in separate requests over the selected timeframe.")

            if not repeat_feeders.empty:
                st.dataframe(repeat_feeders.style.set_table_styles(HEADER_STYLES), width="stretch", hide_index=True)
            else:
                st.success("No feeders had multiple PTWs requested against them in the selected timeframe! 🎉")


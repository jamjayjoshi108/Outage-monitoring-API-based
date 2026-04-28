import streamlit as st
import pandas as pd
from datetime import datetime, timedelta, timezone, date

# ─────────────────────────────────────────────────────────────
# CONSTANTS & STYLING
# ─────────────────────────────────────────────────────────────
HEADER_STYLES = [
    {'selector': 'th', 'props': [
        ('background-color', '#004085 !important'),
        ('color', '#FFC107 !important'),
        ('font-weight', 'bold !important'),
        ('text-align', 'center !important')
    ]},
    {'selector': 'th div', 'props': [
        ('color', '#FFC107 !important'),
        ('font-weight', 'bold !important')
    ]}
]

IST     = timezone(timedelta(hours=5, minutes=30))

OUTAGES_URL = "https://pspcl-dashboard-data.s3.ap-south-1.amazonaws.com/outages.csv"
PTW_URL     = "https://pspcl-dashboard-data.s3.ap-south-1.amazonaws.com/ptw_requests.csv"

OUTAGES_COLS = [
    "outage_id", "zone_name", "circle_name", "feeder_name",
    "outage_type", "outage_status", "start_time", "supply_restored_time",
    "duration_minutes", "created_time"
]
PTW_COLS = [
    "ptw_id", "circle_name", "feeders", "current_status",
    "creation_date", "start_time", "end_time"
]

# ─────────────────────────────────────────────────────────────
# DATA LOADING
# ─────────────────────────────────────────────────────────────
@st.cache_data(ttl=3600)
def load_aws_data():
    with st.spinner("⏳ Loading data from AWS S3..."):
        df_outages = pd.read_csv(
            OUTAGES_URL, usecols=OUTAGES_COLS, low_memory=False,
            dtype={
                "outage_id": "str", "zone_name": "category",
                "circle_name": "category", "feeder_name": "category",
                "outage_type": "category", "outage_status": "category",
                "duration_minutes": "float32",
            },
            parse_dates=["start_time", "supply_restored_time", "created_time"]
        )
        df_ptw = pd.read_csv(
            PTW_URL, usecols=PTW_COLS, low_memory=False,
            dtype={"ptw_id": "str", "circle_name": "category", "current_status": "category"},
            parse_dates=["creation_date", "start_time", "end_time"]
        )
    return df_outages, df_ptw

# ─────────────────────────────────────────────────────────────
# ALL HELPER FUNCTIONS (keep exactly as in file:1)
# ─────────────────────────────────────────────────────────────
def clean_outage_data(df):
    # ... (paste entire function from file:1 unchanged)

def safe_ly_date(dt):
    # ... (paste from file:1 unchanged)

def generate_yoy_dist_expanded(df_curr, df_ly, group_col):
    # ... (paste from file:1 unchanged)

def build_weekly_yoy_table(df_curr, df_ly, curr_yr, ly_yr):
    # ... (paste from file:1 unchanged)

def apply_pu_gradient(styler, df):
    # ... (paste from file:1 unchanged)

def highlight_delta(val):
    # ... (paste from file:1 unchanged)

def style_pct_change(val):
    # ... (paste from file:1 unchanged)

def create_bucket_pivot(df, bucket_order):
    # ... (paste from file:1 unchanged)

def handle_period_change(tab_key):
    # ... (paste from file:1 unchanged)

def render_date_selector(tab_key):
    # ... (paste from file:1 unchanged)

# ─────────────────────────────────────────────────────────────
# MAIN ENTRY POINT — called by Command & Control app
# ─────────────────────────────────────────────────────────────
def render_aws_dashboard():
    """Entry point called by the Command & Control app."""

    now_ist = datetime.now(IST)

    # ── CSS Styling ──────────────────────────────────────────
    st.markdown("""
        <style>
            .block-container { padding-top: 1.5rem; ... }
            /* paste entire CSS block from file:1 unchanged */
        </style>
    """, unsafe_allow_html=True)

    # ── Back to Home button ──────────────────────────────────
    col_back, col_title = st.columns([0.12, 0.88])
    with col_back:
        if st.button("⬅️ Home", use_container_width=True):
            st.session_state.page = 'home'
            st.rerun()

    # ── Load Data ────────────────────────────────────────────
    df_outages_raw, df_ptw_raw = load_aws_data()
    df_master     = clean_outage_data(df_outages_raw)
    df_ptw_master = df_ptw_raw.copy()
    if not df_ptw_master.empty and 'ptw_id' in df_ptw_master.columns:
        df_ptw_master = df_ptw_master.drop_duplicates(subset=['ptw_id'], keep='last')

    # ── Title & Tabs ─────────────────────────────────────────
    st.title("⚡ Power Outage Monitoring Dashboard")
    tab1, tab2, tab3 = st.tabs(["📊 Dashboard", "📈 YoY Comparison", "🛠️ PTW Frequency"])

    # ── Paste all TAB 1, TAB 2, TAB 3 code from file:1 here ─
    with tab1:
        # ... entire Tab 1 block from file:1

    with tab2:
        # ... entire Tab 2 block from file:1

    with tab3:
        # ... entire Tab 3 block from file:1

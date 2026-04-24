import os
import requests
import pandas as pd
from datetime import datetime, timedelta, timezone

# --- CONFIGURATION ---
API_KEY = "pdc@12345"  # ⚠️ Replace this before committing!
OUTAGE_URL = "https://distribution.pspcl.in/returns/module.php?to=OutageAPI.OutageEvents"
PTW_URL = "https://distribution.pspcl.in/returns/module.php?to=OutageAPI.PTWRequests"

# --- DATE CALCULATION (Respecting 7 AM Portal Delay) ---
IST = timezone(timedelta(hours=5, minutes=30))
now_ist = datetime.now(IST)
if now_ist.hour < 8: 
    now_ist -= timedelta(days=1)

today_str = now_ist.strftime("%Y-%m-%d")
five_days_ago = (now_ist - timedelta(days=5)).strftime("%Y-%m-%d")
seven_days_ago = (now_ist - timedelta(days=7)).strftime("%Y-%m-%d")

# Desired output filenames (Matches main.py exactly)
file_today = f"{today_str}_Outages_Today.csv"
file_5day = f"{today_str}_Outages_Last_5_Days.csv"
file_ptw = f"{today_str}_PTW_Last_7_Days.csv"

def fetch_and_save_outages(start_date, end_date, filename):
    print(f"📡 Fetching Outages: {start_date} to {end_date}...")
    payload = {"fromdate": start_date, "todate": end_date, "apikey": API_KEY}
    
    try:
        response = requests.post(OUTAGE_URL, json=payload, headers={'Content-Type': 'application/json'}, timeout=30)
        response.raise_for_status()
        data = response.json()
        
        # Handle both raw list and dictionary responses
        records = data if isinstance(data, list) else data.get("data", [])
        
        if records:
            df = pd.DataFrame(records)
            
            # Map API fields to exactly what Streamlit expects
            df.rename(columns={
                "zone_name": "Zone",
                "circle_name": "Circle",
                "feeder_name": "Feeder",
                "outage_type": "Type of Outage",
                "status": "Status",
                "start_time": "Start Time",
                "end_time": "End Time",
                "duration_minutes": "Diff in mins"
            }, inplace=True)
            
            df.to_csv(filename, index=False)
            print(f"✅ Saved: {filename} ({len(df)} records)")
        else:
            pd.DataFrame(columns=["Zone", "Circle", "Feeder", "Type of Outage", "Status", "Start Time", "End Time", "Diff in mins"]).to_csv(filename, index=False)
            print(f"⚠️ No data. Saved empty template: {filename}")
            
    except Exception as e:
        print(f"❌ Failed to fetch outages. Error: {e}")

def fetch_and_save_ptw(start_date, end_date, filename):
    print(f"📡 Fetching PTWs: {start_date} to {end_date}...")
    payload = {"fromdate": start_date, "todate": end_date, "apikey": API_KEY}
    
    try:
        response = requests.post(PTW_URL, json=payload, headers={'Content-Type': 'application/json'}, timeout=30)
        response.raise_for_status()
        data = response.json()
        records = data if isinstance(data, list) else data.get("data", [])
        
        if records:
            df = pd.DataFrame(records)
            
            # Convert JSON array of feeders into a comma-separated string for dashboard compatibility
            if 'feeders' in df.columns:
                df['feeders'] = df['feeders'].apply(lambda x: ', '.join(x) if isinstance(x, list) else x)
                
            # Rename columns. Note: we map end_date here just in case they've added it!
            df.rename(columns={
                "ptw_id": "PTW Request ID",
                "circle_name": "Circle",
                "feeders": "Feeder",
                "status": "Status",
                "start_date": "Start Date", 
                "end_date": "End Date",
                "request_date": "Request Date"
            }, inplace=True)
            
            df.to_csv(filename, index=False)
            print(f"✅ Saved: {filename} ({len(df)} records)")
        else:
            pd.DataFrame(columns=["PTW Request ID", "Circle", "Feeder", "Status", "Start Date", "Request Date"]).to_csv(filename, index=False)
            print(f"⚠️ No data. Saved empty template: {filename}")
            
    except Exception as e:
        print(f"❌ Failed to fetch PTWs. Error: {e}")

# --- EXECUTION ---
if __name__ == "__main__":
    # Clean up old files from the runner space
    for file in [file_today, file_5day, file_ptw]:
        if os.path.exists(file):
            os.remove(file)

    fetch_and_save_outages(today_str, today_str, file_today)
    fetch_and_save_outages(five_days_ago, today_str, file_5day)
    fetch_and_save_ptw(seven_days_ago, today_str, file_ptw)
    
    print("\n🚀 All API fetches complete!")

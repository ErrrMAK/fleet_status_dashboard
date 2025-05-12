import streamlit as st
import pandas as pd
import psycopg2
from sqlalchemy import create_engine
from datetime import datetime, timezone
import plotly.express as px

st.set_page_config(layout="wide")

# Sidebar: Connection details
st.sidebar.title("Database Connection")
hostname = st.sidebar.text_input("Hostname", value="localhost")
database = st.sidebar.text_input("Database Name")
user = st.sidebar.text_input("Username")
password = st.sidebar.text_input("Password", type="password")
port = st.sidebar.text_input("Port", value="5432")
connect_button = st.sidebar.button("Connect")

# Top control parameters
st.title("Live Object Monitoring Dashboard")
col1, col2, col3, col4 = st.columns(4)
with col1:
    max_idle_speed = st.slider("Max Idle Speed", 0, 10, 2)
with col2:
    min_idle_detection = st.slider("Min Idle Detection (minutes)", 0, 10, 3)
with col3:
    gps_not_updated_min = st.slider("GPS Not Updated Min (minutes)", 0, 10, 2)
with col4:
    gps_not_updated_max = st.slider("GPS Not Updated Max (minutes)", gps_not_updated_min, 10, 5)

# Data fetching function
@st.cache_data(ttl=300)
def fetch_data():
    try:
        engine = create_engine(f'postgresql://{user}:{password}@{hostname}:{port}/{database}')
        query = """
        WITH latest_data AS (
            SELECT 
                tdc.device_id,
                tdc.device_time,
                tdc.latitude / 1e7::decimal AS latitude,
                tdc.longitude / 1e7::decimal AS longitude,
                tdc.speed,
                tdc.altitude,
                ROW_NUMBER() OVER (PARTITION BY tdc.device_id ORDER BY tdc.device_time DESC) AS rn
            FROM 
                raw_telematics_data.tracking_data_core AS tdc
            WHERE 
                tdc.device_time >= NOW() - INTERVAL '11 minutes'
        )
        SELECT 
            o.object_label,
            ld.device_time,
            ld.latitude,
            ld.longitude,
            ld.speed,
            ld.altitude
        FROM 
            latest_data AS ld
        JOIN 
            raw_business_data.devices AS d ON d.device_id = ld.device_id
        JOIN 
            raw_business_data.objects AS o ON o.device_id = d.device_id 
        WHERE 
            ld.rn = 1;
        """
        df = pd.read_sql(query, engine)
        return df
    except Exception as e:
        st.error(f"Connection failed: {e}")
        return pd.DataFrame()

# Main logic
if connect_button:
    df = fetch_data()

    if not df.empty:
        df['device_time'] = pd.to_datetime(df['device_time'], utc=True)  # Ensure tz-aware
        current_time = datetime.now(timezone.utc)  # Also tz-aware

        # Movement classification
        def classify_movement(row):
            speed = row['speed']
            time_diff = (current_time - row['device_time']).total_seconds() / 60
            if speed > max_idle_speed:
                return 'Moving'
            elif time_diff < min_idle_detection:
                return 'Stopped'
            else:
                return 'Parked'

        # Connection classification
        def classify_connection(row):
            diff = (current_time - row['device_time']).total_seconds() / 60
            if diff <= gps_not_updated_min:
                return 'Online'
            elif gps_not_updated_min < diff <= gps_not_updated_max:
                return 'Standby'
            else:
                return 'Offline'

        df['moving_status'] = df.apply(classify_movement, axis=1)
        df['connection_status'] = df.apply(classify_connection, axis=1)

        # Metrics
        total_objects = df['object_label'].nunique()
        moving_count = df[df['moving_status'] == 'Moving'].shape[0]
        stopped_count = df[df['moving_status'] == 'Stopped'].shape[0]
        parked_count = df[df['moving_status'] == 'Parked'].shape[0]

        online_count = df[df['connection_status'] == 'Online'].shape[0]
        standby_count = df[df['connection_status'] == 'Standby'].shape[0]
        offline_count = df[df['connection_status'] == 'Offline'].shape[0]

        # Main metrics
        ind1, ind2, ind3, ind4 = st.columns(4)
        ind1.metric("Total Objects", total_objects)
        ind2.metric("Moving", moving_count)
        ind3.metric("Stopped", stopped_count)
        ind4.metric("Parked", parked_count)

        # Connection statuses
        cs1, cs2, cs3 = st.columns(3)
        cs1.metric("Online", online_count)
        cs2.metric("Standby", standby_count)
        cs3.metric("Offline", offline_count)

        # Pie charts
        pie1_col, pie2_col = st.columns(2)
        with pie1_col:
            fig1 = px.pie(df, names='moving_status', title='Movement Status Distribution')
            st.plotly_chart(fig1)
        with pie2_col:
            fig2 = px.pie(df, names='connection_status', title='Connection Status Distribution')
            st.plotly_chart(fig2)

        # Final table
        display_df = df[['object_label', 'latitude', 'longitude', 'speed', 'connection_status', 'moving_status']]
        display_df.columns = ['Object Label', 'Last Latitude', 'Last Longitude', 'Last Speed', 'Connection Status', 'Moving Status']
        st.dataframe(display_df, use_container_width=True)
    else:
        st.warning("No data retrieved. Check the connection or data availability.")

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
st.title("Fleet Status Dashboard")

with st.form(key="params_form"):
    col1, col2, col3, col4 = st.columns(4)
    with col1:
        max_idle_speed = st.slider("Max Idle Speed (km/h)", 0, 10, 2)
    with col2:
        min_idle_detection = st.slider("Min Idle Detection (minutes)", 0, 10, 3)
    with col3:
        gps_not_updated_min = st.slider("GPS Not Updated Min (minutes)", 0, 10, 2)
    with col4:
        gps_not_updated_max = st.slider("GPS Not Updated Max (minutes)", gps_not_updated_min, 15, 5)

    update_button = st.form_submit_button("Update")

# Data fetching function
@st.cache_data(ttl=300)
def fetch_data():
    try:
        engine = create_engine(f'postgresql://{user}:{password}@{hostname}:{port}/{database}?sslmode=require')
        query = """
            SELECT 
                o.object_label,
                tdc.device_time,
                tdc.latitude / 1e7::decimal AS latitude,
                tdc.longitude / 1e7::decimal AS longitude,
                tdc.speed / 100 AS speed_n,
                tdc.altitude
            FROM 
                raw_telematics_data.tracking_data_core AS tdc
            JOIN 
                raw_business_data.devices AS d ON d.device_id = tdc.device_id
            JOIN 
                raw_business_data.objects AS o ON o.device_id = d.device_id
            WHERE 
                tdc.device_time >= NOW() - INTERVAL '15 minutes'
            ORDER BY 
                tdc.device_time DESC;
        """
        df = pd.read_sql(query, engine)
        df['device_time'] = pd.to_datetime(df['device_time'], utc=True)
        return df
    except Exception as e:
        st.error(f"Connection failed: {e}")
        return pd.DataFrame()

# Fetch and store data when Connect is clicked
if connect_button:
    df = fetch_data()
    if not df.empty:
        st.session_state.df = df
        st.success("Data fetched and ready for processing. Set parameters and click Update")
    else:
        st.warning("No data retrieved.")

# Update aggregations and display when Update is clicked
if update_button:
    if "df" not in st.session_state or st.session_state.df.empty:
        st.warning("No data available. Please connect first.")
    else:
        df = st.session_state.df.copy()
        current_time = datetime.now(timezone.utc)

        # Get the latest record for each object_label
        latest_df = df.sort_values("device_time", ascending=False).groupby("object_label", as_index=False).first()

        # Movement classification
        def classify_movement(row):
            speed = row['speed_n']
            time_diff = (current_time - row['device_time']).total_seconds() / 60
            if speed > max_idle_speed:
                return 'Moving'
            elif time_diff <= min_idle_detection:
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

        latest_df['moving_status'] = latest_df.apply(classify_movement, axis=1)
        latest_df['connection_status'] = latest_df.apply(classify_connection, axis=1)

        # Metrics
        total_objects = latest_df.shape[0]
        moving_count = (latest_df['moving_status'] == 'Moving').sum()
        stopped_count = (latest_df['moving_status'] == 'Stopped').sum()
        parked_count = (latest_df['moving_status'] == 'Parked').sum()

        online_count = (latest_df['connection_status'] == 'Online').sum()
        standby_count = (latest_df['connection_status'] == 'Standby').sum()
        offline_count = (latest_df['connection_status'] == 'Offline').sum()

        # Display metrics
        ind1, ind2, ind3, ind4 = st.columns(4)
        ind1.metric("Total Objects", total_objects)
        ind2.metric("Moving", moving_count)
        ind3.metric("Stopped", stopped_count)
        ind4.metric("Parked", parked_count)

        cs1, cs2, cs3, cs4 = st.columns(4)
        cs2.metric("Online", online_count)
        cs3.metric("Standby", standby_count)
        cs4.metric("Offline", offline_count)

        # Pie charts
        pie1_col, pie2_col = st.columns(2)
        with pie1_col:
            fig1 = px.pie(latest_df, names='moving_status', title='Movement Status Distribution')
            st.plotly_chart(fig1)
        with pie2_col:
            fig2 = px.pie(latest_df, names='connection_status', title='Connection Status Distribution')
            st.plotly_chart(fig2)

        # Final table with Last Device Time column
        display_df = latest_df[[
            'object_label', 'latitude', 'longitude', 'speed_n', 'device_time', 'connection_status', 'moving_status'
        ]]
        display_df.columns = [
            'Object Label', 'Last Latitude', 'Last Longitude', 'Last Speed',
            'Last Device Time', 'Connection Status', 'Moving Status'
        ]
        st.dataframe(display_df, use_container_width=True)

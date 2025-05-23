import streamlit as st
import pandas as pd
from datasets.measurment import get_measurment_data

@st.cache_data(ttl=300)
def load_data(hours, object_labels, sensor_labels):
    return get_measurment_data(st.session_state["conn"], hours, object_labels, sensor_labels)

def run_measurment_dashboard():
    st.header("Measurment Dashboard")
    with st.sidebar:
        st.subheader("Фильтры (Measurment)")
        error_msg = None
        all_objects, all_sensors = [], []
        try:
            df_all = get_measurment_data(st.session_state["conn"], 72)
            if not df_all.empty:
                all_objects = sorted(df_all['object_label'].dropna().unique())
                all_sensors = sorted(df_all['sensor_label'].dropna().unique())
        except Exception as e:
            error_msg = str(e)
        object_labels = st.multiselect("Объекты (object_label)", all_objects, default=all_objects)
        sensor_labels = st.multiselect("Сенсоры (sensor_label)", all_sensors, default=all_sensors)
        hours = st.slider("Период (часы)", 1, 72, 24)
        refresh = st.button("Обновить данные", key="measurment_refresh")
        if error_msg:
            st.error(f"Ошибка при загрузке фильтров: {error_msg}")
        if not all_objects or not all_sensors:
            st.warning("Нет данных для фильтров. Проверьте подключение или наличие данных в БД.")
    if refresh:
        st.info(f"Загрузка данных за {hours} ч. Фильтры: объекты={object_labels}, сенсоры={sensor_labels}")
        df = load_data(hours, object_labels, sensor_labels)
        if df.empty:
            st.warning("Нет данных по выбранным фильтрам")
            return
        st.subheader("Динамика по сенсорам")
        chart_df = df.pivot_table(index='hour_bucket', columns=['object_label','sensor_label'], values='calibrated_volume_avg')
        st.line_chart(chart_df)
        st.subheader("Детализированные данные")
        st.dataframe(df, use_container_width=True)
    else:
        st.info("Настройте фильтры и нажмите 'Обновить данные'") 
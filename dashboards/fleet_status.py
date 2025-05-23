"""
Дашборд для мониторинга движения объектов
"""
import streamlit as st
import pandas as pd
from charts import display_movement_status_chart
from datasets.queries import get_current_status_query
from filters import display_control_params

@st.cache_data(ttl=300)  # Кэширование на 5 минут
def load_current_status(params):
    """
    Загружает данные с учетом параметров фильтрации
    """
    query = get_current_status_query(params)
    return pd.read_sql(query, st.session_state["conn"])

def display_metrics(df):
    """
    Отображение основных метрик
    """
    col1, col2, col3 = st.columns(3)
    with col1:
        st.metric("Total Devices", len(df['device_id'].unique()))
    with col2:
        active_count = (df['connection_status'] == 'active').sum()
        st.metric("Active Devices", active_count)
    with col3:
        offline_count = (df['connection_status'] == 'offline').sum()
        st.metric("Offline Devices", offline_count)

def display_charts():
    """
    Отображение графиков
    """
    col1, col2 = st.columns(2)
    
    with col1:
        st.subheader("Movement Status")
        display_movement_status_chart()
    
    with col2:
        st.subheader("Connection Status")
        # TODO: Добавить график статуса подключения

def display_data_table(df):
    """
    Отображение таблицы с данными
    """
    st.subheader("Current Status")
    display_df = df[[
        'device_id',
        'object_label',
        'first_name',
        'last_name',
        'speed',
        'moving_status',
        'connection_status',
        'last_connect_formatted'
    ]].sort_values('last_connect_formatted')
    
    # Переименовываем колонки для отображения
    display_df.columns = [
        'Device ID',
        'Vehicle',
        'First Name',
        'Last Name',
        'Speed (km/h)',
        'Movement Status',
        'Connection Status',
        'Last Connection'
    ]
    
    st.dataframe(display_df, use_container_width=True)

def run_dashboard():
    """
    Основная функция запуска дашборда
    """
    try:
        # Отображаем параметры управления
        params = display_control_params()
        
        # Загружаем данные только если нажата кнопка Update
        if params['update_button']:
            df = load_current_status(params)
            display_metrics(df)
            display_charts()
            display_data_table(df)
        else:
            st.info("Настройте параметры фильтрации и нажмите 'Update' для обновления данных")

    except Exception as e:
        st.error(f"Ошибка при загрузке данных: {str(e)}")
        st.error("Проверьте подключение к базе данных и настройки.") 
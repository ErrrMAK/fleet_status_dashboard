"""
Основной файл дашборда для мониторинга движения объектов
"""
import streamlit as st
import pandas as pd
from charts import display_movement_status_chart
from db_connection import get_sqlalchemy_engine
from SQL.queries import get_current_status_query
from dotenv import load_dotenv
from filters import display_control_params
from datetime import datetime, timezone

# Загрузка переменных окружения
load_dotenv()

# Настройка страницы
st.set_page_config(
    page_title="Fleet Status Dashboard",
    page_icon="🚗",
    layout="wide",
    initial_sidebar_state="expanded",
    menu_items={
        'Get Help': 'https://github.com/streamlit/streamlit/issues',
        'Report a bug': 'https://github.com/streamlit/streamlit/issues/new',
        'About': 'Fleet Status Dashboard - Real-time vehicle monitoring'
    }
)

# Загрузка данных
@st.cache_data(ttl=300)  # Кэширование на 5 минут
def load_current_status(params):
    """
    Загружает данные с учетом параметров фильтрации
    """
    query = get_current_status_query(params)
    return pd.read_sql(query, get_sqlalchemy_engine())

# Заголовок
st.title("Fleet Status Dashboard")

try:
    # Отображаем параметры управления
    params = display_control_params()
    
    # Загружаем данные только если нажата кнопка Update
    if params['update_button']:
        df = load_current_status(params)
        
        # Основные метрики
        col1, col2, col3 = st.columns(3)
        with col1:
            st.metric("Total Devices", len(df['device_id'].unique()))
        with col2:
            active_count = (df['connection_status'] == 'active').sum()
            st.metric("Active Devices", active_count)
        with col3:
            offline_count = (df['connection_status'] == 'offline').sum()
            st.metric("Offline Devices", offline_count)

        # Графики в двух колонках
        col1, col2 = st.columns(2)
        
        with col1:
            st.subheader("Movement Status")
            display_movement_status_chart()
        
        with col2:
            st.subheader("Connection Status")
            # TODO: Добавить график статуса подключения

        # Таблица с данными
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
    else:
        st.info("Настройте параметры фильтрации и нажмите 'Update' для обновления данных")

except Exception as e:
    st.error(f"Ошибка при загрузке данных: {str(e)}")
    st.error("Проверьте подключение к базе данных и настройки.") 
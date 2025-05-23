"""
Основной файл приложения с общими настройками
"""
import streamlit as st
from dotenv import load_dotenv
import os
import psycopg2
from psycopg2 import OperationalError
import pandas as pd

# Загрузка переменных окружения
load_dotenv()

def init_app():
    """
    Инициализация основных настроек приложения
    """
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

def display_header():
    """
    Отображение заголовка и логотипа
    """
    st.title("Fleet Status Dashboard")
    # TODO: Добавить логотип

def connect_to_db(host, dbname, user, password, port, sslmode='require'):
    """
    Подключение к базе данных
    """
    try:
        st.write("Debug: Attempting to connect to DB with params:", {  # Отладочная информация
            "host": host,
            "dbname": dbname,
            "user": user,
            "port": port
        })
        conn = psycopg2.connect(
            host=host,
            port=int(port),
            user=user,
            password=password,
            dbname=dbname,
            sslmode=sslmode
        )
        st.write("Debug: Connection successful")  # Отладочная информация
        return conn
    except Exception as e:
        st.error(f"Ошибка подключения: {e}")
        return None

def check_connection(conn):
    """
    Проверка соединения с БД
    """
    try:
        cursor = conn.cursor()
        cursor.execute("SELECT 1")
        st.write("Debug: Connection check successful")  # Отладочная информация
        return True
    except Exception as e:
        st.write(f"Debug: Connection check failed: {e}")  # Отладочная информация
        return False

def display_db_connection():
    """
    Отображение формы подключения к БД
    """
    with st.sidebar:
        st.header("Подключение к БД")
        
        # Кнопка для использования .env
        if st.button("Использовать .env"):
            try:
                st.write("Debug: Using .env credentials")  # Отладочная информация
                conn = connect_to_db(
                    host=os.getenv('DB_HOST'),
                    dbname=os.getenv('DB_NAME'),
                    user=os.getenv('DB_USER'),
                    password=os.getenv('DB_PASSWORD'),
                    port=os.getenv('DB_PORT', '5432')
                )
                if conn:
                    st.session_state["conn"] = conn
                    st.success("Подключение установлено!")
                    st.write("Debug: Connection stored in session_state")  # Отладочная информация
            except Exception as e:
                st.error(f"Ошибка при подключении через .env: {e}")
        
        # Ручной ввод параметров
        st.subheader("Или введите параметры вручную:")
        host = st.text_input("Host", value="")
        dbname = st.text_input("Database Name", value="")
        user = st.text_input("Username", value="")
        password = st.text_input("Password", type="password")
        port = st.text_input("Port", value="5432")
        
        connect_button = st.button("Подключиться")
        
        if connect_button:
            conn = connect_to_db(host, dbname, user, password, port)
            if conn:
                st.session_state["conn"] = conn
                st.success("Подключение установлено!")
                st.write("Debug: Connection stored in session_state")  # Отладочная информация
            else:
                st.session_state["conn"] = None
        
        # Отображение статуса подключения
        conn = st.session_state.get("conn", None)
        if conn and check_connection(conn):
            st.success("Статус: Подключено")
            st.write("Debug: Connection is valid")  # Отладочная информация
        elif conn:
            st.error("Статус: Отключено")
            st.session_state.pop("conn")
            conn = None
        else:
            st.warning("Статус: Не подключено")

def check_auth():
    """
    Проверка авторизации пользователя
    """
    # Проверяем наличие подключения к БД
    if "conn" not in st.session_state or not st.session_state["conn"]:
        display_db_connection()
        return False
    return True

def main():
    """
    Основная функция запуска приложения
    """
    init_app()
    display_header()
    
    if check_auth():
        tabs = st.tabs(["Moving Status", "Shifts", "Measurment"])
        with tabs[0]:
            try:
                if "conn" in st.session_state and st.session_state["conn"]:
                    from dashboards.fleet_status import run_dashboard
                    run_dashboard()
                else:
                    st.warning("Нет подключения к базе данных")
            except Exception as e:
                st.error(f"Ошибка в Moving Status: {e}")
        with tabs[1]:
            try:
                if "conn" in st.session_state and st.session_state["conn"]:
                    from dashboards.shifts import run_shifts_dashboard
                    run_shifts_dashboard()
                else:
                    st.warning("Нет подключения к базе данных")
            except Exception as e:
                st.error(f"Ошибка в Shifts: {e}")
        with tabs[2]:
            try:
                if "conn" in st.session_state and st.session_state["conn"]:
                    from dashboards.measurment import run_measurment_dashboard
                    run_measurment_dashboard()
                else:
                    st.warning("Нет подключения к базе данных")
            except Exception as e:
                st.error(f"Ошибка в Measurment: {e}")
    else:
        st.info("Пожалуйста, подключитесь к базе данных для продолжения")

if __name__ == "__main__":
    main() 
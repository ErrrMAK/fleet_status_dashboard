"""
Основной файл приложения с общими настройками
"""
import streamlit as st
from dotenv import load_dotenv
import os

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

def check_auth():
    """
    Проверка авторизации пользователя
    """
    # TODO: Реализовать проверку авторизации
    return True

def main():
    """
    Основная функция запуска приложения
    """
    init_app()
    
    if check_auth():
        display_header()
        # Импортируем и запускаем конкретный дашборд
        from dashboards.fleet_status import run_dashboard
        run_dashboard()
    else:
        st.error("Требуется авторизация")

if __name__ == "__main__":
    main() 
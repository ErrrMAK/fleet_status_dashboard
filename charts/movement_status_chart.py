"""
Модуль для отображения графика статуса движения
"""
import streamlit as st
import pandas as pd
import plotly.express as px
from datasets.queries import get_current_status_query
from db_connection import get_sqlalchemy_engine

def display_movement_status_chart(params=None):
    """
    Отображает круговую диаграмму статуса движения
    
    Args:
        params (dict): Параметры фильтрации
    """
    try:
        # Получаем данные с учетом параметров фильтрации
        query = get_current_status_query(params)
        df = pd.read_sql(query, get_sqlalchemy_engine())
        
        # Создаем круговую диаграмму
        fig = px.pie(
            df,
            names='moving_status',
            title="Movement Status Distribution",
            color='moving_status',
            color_discrete_map={
                'moving': '#2ecc71',  # green
                'stopped': '#f1c40f',  # yellow
                'parked': '#95a5a6'    # gray
            }
        )
        
        # Настраиваем внешний вид
        fig.update_layout(
            paper_bgcolor='rgba(0,0,0,0)',
            plot_bgcolor='rgba(0,0,0,0)',
            margin=dict(t=30, b=0, l=0, r=0)
        )
        
        # Отображаем график
        st.plotly_chart(fig, use_container_width=True)
        
    except Exception as e:
        st.error(f"Ошибка при создании графика: {str(e)}")

if __name__ == "__main__":
    # Для тестирования
    st.set_page_config(layout="wide")
    display_movement_status_chart() 
"""
Дашборд по сменам (shifts)
"""
import streamlit as st
from datasets.shifts import get_shifts_summary
from datetime import datetime, timedelta

def run_shifts_dashboard():
    # Фильтры по дате и параметрам
    col1, col2 = st.columns(2)
    with col1:
        start_date = st.date_input("Start date", datetime.now() - timedelta(days=1))
    with col2:
        end_date = st.date_input("End date", datetime.now())
    col3, col4 = st.columns(2)
    with col3:
        min_speed = st.slider("Минимальная скорость для трека (км/ч)", 0, 10, 3)
    with col4:
        max_time_diff = st.slider("Максимальный разрыв между точками (сек)", 60, 600, 300, step=10)
    # TODO: фильтр по объекту (device_id/object_label) при необходимости
    if st.button("refresh", key="shifts_refresh"):
        df = get_shifts_summary(st.session_state["conn"], start_date, end_date, min_speed=min_speed, max_time_diff=max_time_diff)
        st.dataframe(df, use_container_width=True)
        # Можно добавить plotly/bar chart по активности
        st.subheader("Activity by Object and Date")
        st.bar_chart(df, x="device_id", y="average_speed")
    else:
        st.info("Выберите диапазон дат и параметры, затем нажмите 'Обновить сводную таблицу'") 
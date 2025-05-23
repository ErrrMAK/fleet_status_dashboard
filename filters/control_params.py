"""
Модуль с параметрами управления для дашборда
"""
import streamlit as st

def display_control_params():
    """
    Отображает форму с параметрами управления
    """
    st.subheader("Параметры управления")
    
    col1, col2, col3, col4 = st.columns(4)
    with col1:
        max_idle_speed = st.slider("Max Idle Speed (km/h)", 0, 10, 2)
    with col2:
        min_idle_detection = st.slider("Min Idle Detection (minutes)", 0, 10, 3)
    with col3:
        gps_not_updated_min = st.slider("GPS Not Updated Min (minutes)", 0, 10, 5)
    with col4:
        gps_not_updated_max = st.slider("GPS Not Updated Max (minutes)", gps_not_updated_min, 15, 10)

<<<<<<< HEAD
    update_button = st.button("refresh")
=======
    update_button = st.button("Обновить данные")
>>>>>>> 59fb1d1 (разделен дашборд и апп)
    
    return {
        'max_idle_speed': max_idle_speed,
        'min_idle_detection': min_idle_detection,
        'gps_not_updated_min': gps_not_updated_min,
        'gps_not_updated_max': gps_not_updated_max,
        'update_button': update_button
    } 
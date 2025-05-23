"""
Модуль для работы с данными о сменах
"""
import pandas as pd
import numpy as np
from datetime import datetime, timedelta

def get_shifts_data(conn, start_date=None, end_date=None, device_id=None, min_speed=3, max_time_diff=300):
    """
    Получение и обработка данных о сменах
    
    Args:
        conn: Соединение с БД
        start_date (datetime): Начальная дата
        end_date (datetime): Конечная дата
        device_id (int): ID устройства
        min_speed (int): Минимальная скорость для движения
        max_time_diff (int): Максимальная разница во времени для нового трека
        
    Returns:
        pd.DataFrame: Обработанные данные о сменах
    """
    # Подготовка параметров запроса
    if start_date is None:
        start_date = datetime.now() - timedelta(days=1)
    if end_date is None:
        end_date = datetime.now()
    
    device_filter = f"AND t.device_id = {device_id}" if device_id else ""
    
    # SQL запрос для получения данных
    query = f"""
    WITH filtered_tracking_data AS (
        SELECT *
        FROM raw_telematics_data.tracking_data_core t
        WHERE device_time < '{end_date}'::timestamp
        AND device_time >= '{start_date}'::timestamp
        {device_filter}
        AND t.event_id IN (2, 802, 803, 804, 811)  -- только значимые события
    )
    SELECT
        t.device_id,
        t.device_time,
        t.speed,
        t.latitude,
        t.longitude,
        t.altitude,
        t.event_id
    FROM filtered_tracking_data t
    ORDER BY t.device_id, t.device_time;
    """
    
    # Получаем данные из БД
    df = pd.read_sql(query, conn)
    
    # Конвертируем timestamp в datetime
    df['device_time'] = pd.to_datetime(df['device_time'])
    
    # Добавляем предыдущие значения
    df['prev_device_time'] = df.groupby('device_id')['device_time'].shift(1)
    df['prev_speed'] = df.groupby('device_id')['speed'].shift(1)
    
    # Вычисляем разницу во времени
    df['time_diff'] = (df['device_time'] - df['prev_device_time']).dt.total_seconds()
    
    # Определяем статус движения
    df['moving_status'] = np.where(
        df['speed'].isna(), 'parked',
        np.where(df['speed'] >= min_speed, 'moving', 'stopped')
    )
    
    # Определяем начало нового трека
    df['new_track_flag'] = np.where(
        df['prev_device_time'].isna(), 1,
        np.where(
            df['time_diff'] > max_time_diff, 1,
            np.where(
                (df['speed'] >= min_speed) & (df['prev_speed'] < min_speed) & (df['time_diff'] >= max_time_diff),
                1,
                0
            )
        )
    )
    
    # Назначаем промежуточный track_id
    df['temp_track_id'] = df.groupby('device_id')['new_track_flag'].cumsum()
    
    # Получаем начальные и конечные координаты для каждого трека
    track_start = df.groupby(['device_id', 'temp_track_id']).agg({
        'device_time': 'min',
        'latitude': 'first',
        'longitude': 'first',
        'altitude': 'first'
    }).reset_index()
    
    track_end = df.groupby(['device_id', 'temp_track_id']).agg({
        'device_time': 'max',
        'latitude': 'last',
        'longitude': 'last',
        'altitude': 'last'
    }).reset_index()
    
    # Объединяем начальные и конечные точки
    tracks = pd.merge(
        track_start,
        track_end,
        on=['device_id', 'temp_track_id'],
        suffixes=('_start', '_end')
    )
    
    # Вычисляем статистики по трекам
    track_stats = df.groupby(['device_id', 'temp_track_id']).agg({
        'speed': ['mean', 'max', 'min', 'count']
    }).reset_index()
    
    track_stats.columns = ['device_id', 'temp_track_id', 'avg_speed', 'max_speed', 'min_speed', 'points_in_track']
    
    # Объединяем все данные
    final_tracks = pd.merge(tracks, track_stats, on=['device_id', 'temp_track_id'])
    
    # Явно переименовываю device_time_start/device_time_end
    final_tracks = final_tracks.rename(columns={
        'device_time_start': 'track_start_time',
        'device_time_end': 'track_end_time'
    })
    
    # Вычисляем длительность трека
    final_tracks['track_duration_seconds'] = (final_tracks['track_end_time'] - final_tracks['track_start_time']).dt.total_seconds()
    final_tracks['track_duration'] = final_tracks['track_duration_seconds'].apply(
        lambda x: str(timedelta(seconds=int(x)))
    )
    
    # Фильтруем фейковые треки
    final_tracks = final_tracks[
        (final_tracks['points_in_track'] >= 2) & 
        (final_tracks['max_speed'] >= min_speed)
    ]
    
    # Нормализуем координаты
    for col in ['latitude_start', 'longitude_start', 'altitude_start', 
                'latitude_end', 'longitude_end', 'altitude_end']:
        final_tracks[col] = final_tracks[col] / 1e7
    
    # Нормализуем скорости
    for col in ['avg_speed', 'max_speed', 'min_speed']:
        final_tracks[col] = final_tracks[col] / 1e2
    
    # Добавляем финальный номер трека
    final_tracks['track_number'] = final_tracks.groupby('device_id').cumcount() + 1
    final_tracks['track_id'] = final_tracks['device_id'].astype(str) + '-' + final_tracks['track_number'].astype(str)
    
    # Выбираем и переименовываем нужные колонки
    result = final_tracks[[
        'track_id', 'device_id', 'track_start_time', 'track_end_time',
        'track_duration', 'track_duration_seconds', 'avg_speed', 'max_speed',
        'min_speed', 'latitude_start', 'longitude_start', 'altitude_start',
        'latitude_end', 'longitude_end', 'altitude_end', 'points_in_track'
    ]]
    
    return result 

def get_shifts_summary(conn, start_date=None, end_date=None, device_id=None, min_speed=3, max_time_diff=300):
    """
    Возвращает сводную таблицу по сменам для дашборда (по аналогии с Superset)
    """
    df = get_shifts_data(conn, start_date, end_date, device_id, min_speed, max_time_diff)
    # Получаем object_label (можно джойнить к объектам, если нужно)
    # Для примера: пусть object_label = device_id (или добавить join при необходимости)
    df['date'] = df['track_start_time'].dt.date
    # Группировка по объекту и дате
    summary = df.groupby(['device_id', 'date']).agg(
        activity=('track_duration_seconds', lambda x: str(timedelta(seconds=int(x.sum())))),
        average_speed=('avg_speed', 'mean'),
        max_speed=('max_speed', 'max'),
        activity_start=('track_start_time', 'min'),
        activity_end=('track_end_time', 'max')
    ).reset_index()
    # Пример для object_label (если есть join с объектами)
    # summary = summary.merge(...)
    return summary 
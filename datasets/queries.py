"""
SQL запросы для дашборда мониторинга движения
"""
from datetime import datetime, timezone

def get_current_status_query(params=None):
    """
    Возвращает SQL запрос для получения текущего статуса с учетом параметров фильтрации
    
    Args:
        params (dict): Параметры фильтрации
            - max_idle_speed: максимальная скорость для определения простоя
            - min_idle_detection: минимальное время для определения простоя
            - gps_not_updated_min: минимальное время отсутствия обновления GPS
            - gps_not_updated_max: максимальное время отсутствия обновления GPS
    """
    if params is None:
        params = {
            'max_idle_speed': 2,
            'min_idle_detection': 3,
            'gps_not_updated_min': 5,
            'gps_not_updated_max': 10
        }
    
    return f"""
    WITH latest_data AS (
        SELECT 
            o.object_id,
            o.device_id,
            o.object_label,
            e.first_name,
            e.last_name,
            tdc.speed / 100 AS speed,
            tdc.device_time,
            CASE 
                WHEN tdc.speed / 100 > {params['max_idle_speed']} THEN 'moving'
                WHEN EXTRACT(EPOCH FROM (NOW() - tdc.device_time)) / 60 < {params['min_idle_detection']} THEN 'stopped'
                ELSE 'parked'
            END AS moving_status,
            CASE 
                WHEN EXTRACT(EPOCH FROM (NOW() - tdc.device_time)) / 60 <= {params['gps_not_updated_min']} THEN 'active'
                WHEN EXTRACT(EPOCH FROM (NOW() - tdc.device_time)) / 60 <= {params['gps_not_updated_max']} THEN 'idle'
                ELSE 'offline'
            END AS connection_status,
            to_char(tdc.device_time, 'YYYY-MM-DD HH24:MI:SS') as last_connect_formatted
        FROM 
            raw_telematics_data.tracking_data_core AS tdc
        JOIN 
            raw_business_data.devices AS d ON d.device_id = tdc.device_id
        JOIN 
            raw_business_data.objects AS o ON o.device_id = d.device_id
        LEFT JOIN 
            raw_business_data.employees AS e ON o.object_id = e.object_id
        WHERE 
            tdc.device_time >= NOW() - INTERVAL '15 minutes'
        ORDER BY 
            tdc.device_time DESC
    )
    SELECT DISTINCT ON (device_id) *
    FROM latest_data
    ORDER BY device_id, device_time DESC;
    """

# Запрос для получения текущего статуса объектов
CURRENT_STATUS_QUERY = """
    WITH filtered_tracking_data AS (
    SELECT *
    FROM raw_telematics_data.tracking_data_core
    WHERE device_time > now() - INTERVAL '1 days'
    ),
    latest_tracking AS (
    SELECT DISTINCT ON (device_id)
        device_id,
        device_time,
        event_id,
        speed,
        longitude,
        latitude,
        altitude
    FROM filtered_tracking_data
    WHERE event_id IN (2, 802, 803, 804, 811)
    ORDER BY device_id, device_time DESC
    ),
    latest_positions AS (
    SELECT DISTINCT ON (device_id)
        device_id,
        longitude,
        latitude
    FROM filtered_tracking_data
    ORDER BY device_id, device_time DESC
    ),
    zones_match AS (
    SELECT
        z.zone_label,
        lp.device_id
    FROM raw_business_data.zones z
    JOIN latest_positions lp ON ST_DWithin(
        ST_SetSRID(ST_MakePoint(lp.longitude / 1e7, lp.latitude / 1e7), 4326)::geography,
        ST_SetSRID(ST_MakePoint(z.circle_center_longitude, z.circle_center_latitude), 4326)::geography,
        z.radius
    )
    WHERE z.zone_type = 'circle'
    )

    -- main query:
    SELECT
    o.object_id,
    o.device_id,
    o.object_label,
    e.first_name,
    e.last_name,
    t.device_time,
    t.event_id,
    t.speed / 1e2 AS speed,
    string_agg(DISTINCT zones_match.zone_label, ', ') AS geozones,
    t.longitude / 1e7 AS longitude,
    t.latitude / 1e7 AS latitude,
    t.altitude / 1e7 AS altitude,
    EXTRACT(EPOCH FROM (now() - t.device_time)) AS last_connect,
    TO_CHAR(
        make_interval(secs => EXTRACT(EPOCH FROM (now() - t.device_time))),
        'HH24:MI:SS'
    ) AS last_connect_formatted,
    CASE
        WHEN t.device_time IS NULL THEN 'offline'
        WHEN EXTRACT(EPOCH FROM (now() - t.device_time)) <= 60 THEN 'active'
        WHEN EXTRACT(EPOCH FROM (now() - t.device_time)) <= 300 THEN 'idle'
        ELSE 'offline'
    END AS Connection_status,
    CASE
        WHEN t.device_time IS NULL THEN 'parked'
        WHEN t.speed > 3 AND EXTRACT(EPOCH FROM (now() - t.device_time)) <= 300 THEN 'moving'
        WHEN t.speed <= 3 AND EXTRACT(EPOCH FROM (now() - t.device_time)) <= 300 THEN 'stopped'
        ELSE 'parked'
    END AS moving_status
    FROM raw_business_data.objects AS o
    LEFT JOIN latest_tracking AS t ON o.device_id = t.device_id
    LEFT JOIN raw_business_data.employees AS e ON o.object_id = e.object_id
    LEFT JOIN zones_match ON zones_match.device_id = t.device_id
    GROUP BY
    o.object_id,
    o.device_id,
    o.object_label,
    e.first_name,
    e.last_name,
    t.device_time,
    t.event_id,
    t.speed,
    t.longitude,
    t.latitude,
    t.altitude
    ORDER BY
    t.device_time DESC NULLS LAST
""" 
import pandas as pd
import numpy as np
from typing import Optional, List
import logging
import traceback
logging.basicConfig(filename='measurment_debug.log', level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')

def get_measurment_data(conn, hours: int = 24, object_labels: Optional[List[str]] = None, sensor_labels: Optional[List[str]] = None) -> pd.DataFrame:
    try:
        logging.info(f"get_measurment_data: hours={hours}, object_labels={object_labels}, sensor_labels={sensor_labels}")
        print(f"[DEBUG] get_measurment_data: hours={hours}, object_labels={object_labels}, sensor_labels={sensor_labels}")
        # 1. Сырые данные inputs
        query_inputs = f'''
            SELECT device_id, sensor_name, event_id, device_time, value::FLOAT as raw_value
            FROM raw_telematics_data.inputs
            WHERE device_time >= NOW() - INTERVAL '{hours} hours'
        '''
        df_inputs = pd.read_sql(query_inputs, conn)
        logging.info(f"inputs shape: {df_inputs.shape}")
        print(f"[DEBUG] inputs shape: {df_inputs.shape}")

        # 2. sensor_description
        query_meta = '''
            SELECT device_id, input_label, sensor_id, sensor_label, sensor_type, sensor_units, divider, multiplier, units_type, group_type
            FROM raw_business_data.sensor_description
        '''
        df_meta = pd.read_sql(query_meta, conn)
        logging.info(f"meta shape: {df_meta.shape}")
        print(f"[DEBUG] meta shape: {df_meta.shape}")

        # 3. calibration_data
        query_calib = '''
            SELECT sensor_id, value as cal_value, volume as cal_volume
            FROM raw_business_data.sensor_calibration_data
        '''
        df_calib = pd.read_sql(query_calib, conn)
        logging.info(f"calib shape: {df_calib.shape}")
        print(f"[DEBUG] calib shape: {df_calib.shape}")

        # 4. objects
        query_objects = '''
            SELECT device_id, object_label
            FROM raw_business_data.objects
        '''
        df_objects = pd.read_sql(query_objects, conn)
        logging.info(f"objects shape: {df_objects.shape}")
        print(f"[DEBUG] objects shape: {df_objects.shape}")

        # 5. description_parametrs
        query_desc = '''
            SELECT key, type, description
            FROM raw_business_data.description_parametrs
        '''
        df_desc = pd.read_sql(query_desc, conn)
        logging.info(f"desc shape: {df_desc.shape}")
        print(f"[DEBUG] desc shape: {df_desc.shape}")

        # --- JOINs ---
        df = df_inputs.merge(df_meta, left_on=['device_id', 'sensor_name'], right_on=['device_id', 'input_label'], how='left')
        df['value'] = np.where(df['divider'].fillna(0) != 0, (df['raw_value'] / df['divider']) * df['multiplier'].fillna(1), df['raw_value'])
        df['hour_bucket'] = df['device_time'].dt.floor('H')

        # --- Агрегация по часу ---
        agg = df.groupby([
            'hour_bucket', 'device_id', 'sensor_name', 'event_id', 'sensor_id', 'input_label',
            'sensor_label', 'sensor_type', 'sensor_units', 'units_type', 'group_type'
        ]).agg(
            value_avg = ('value', 'mean'),
            value_min = ('value', 'min'),
            value_max = ('value', 'max')
        ).reset_index()

        # --- Калибровка ---
        def calibrate(row, val_col):
            sid = row['sensor_id']
            val = row[val_col]
            calib = df_calib[df_calib['sensor_id'] == sid]
            if calib.empty:
                return val
            below = calib[calib['cal_value'] <= val]
            above = calib[calib['cal_value'] >= val]
            if below.empty or above.empty:
                return val
            low = below.iloc[below['cal_value'].argmax()]
            high = above.iloc[above['cal_value'].argmin()]
            if high['cal_value'] == low['cal_value']:
                return low['cal_volume']
            return low['cal_volume'] + ((val - low['cal_value']) / (high['cal_value'] - low['cal_value'])) * (high['cal_volume'] - low['cal_volume'])

        agg['calibrated_volume_avg'] = agg.apply(lambda r: calibrate(r, 'value_avg'), axis=1)
        agg['calibrated_volume_min'] = agg.apply(lambda r: calibrate(r, 'value_min'), axis=1)
        agg['calibrated_volume_max'] = agg.apply(lambda r: calibrate(r, 'value_max'), axis=1)

        # --- Добавляем object_label ---
        agg = agg.merge(df_objects, on='device_id', how='left')

        # --- description_parametrs для sensor_units_final ---
        units_desc = df_desc[df_desc['type'] == 'sensor_description_units_type'][['key', 'description']]
        group_desc = df_desc[df_desc['type'] == 'sensor_description_group_type'][['key', 'description']]
        agg = agg.merge(units_desc, left_on='units_type', right_on='key', how='left', suffixes=('', '_units'))
        agg = agg.merge(group_desc, left_on='group_type', right_on='key', how='left', suffixes=('', '_group'))
        agg['sensor_units_final'] = agg['sensor_units'].replace('', np.nan).fillna(agg['description'])

        # --- Фильтрация по object_label и sensor_label ---
        if object_labels:
            agg = agg[agg['object_label'].isin(object_labels)]
        if sensor_labels:
            agg = agg[agg['sensor_label'].isin(sensor_labels)]

        # --- Итоговые колонки ---
        columns = [
            'object_label', 'sensor_label', 'sensor_name', 'hour_bucket', 'sensor_type',
            'sensor_units_final', 'calibrated_volume_min', 'calibrated_volume_max', 'calibrated_volume_avg'
        ]
        result = agg[columns].sort_values(['hour_bucket', 'object_label', 'sensor_label'])
        logging.info(f"result shape: {result.shape}")
        print(f"[DEBUG] result shape: {result.shape}")
        return result
    except Exception as e:
        logging.error(f"Exception in get_measurment_data: {e}")
        print(f"[ERROR] Exception in get_measurment_data: {e}")
        traceback.print_exc()
        return pd.DataFrame() 
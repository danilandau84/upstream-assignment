import os
import re
from datetime import datetime

import pandas as pd
import pyarrow.parquet as pq
from utils import make_dir_if_not_exist
from pandas import DataFrame


def generate_sql_injection_report(column_names: [], detections: []):
    input_directory_path = 'data//raw-data'
    output_directory_path = 'data//sql-injection-detection-report'
    make_dir_if_not_exist(output_directory_path)
    files = os.listdir(input_directory_path)
    for file_name in files:
        file_path = os.path.join(input_directory_path, file_name)
        if os.path.isfile(file_path):
            dataset = pq.ParquetDataset(file_path)
            partition_df = dataset.read().to_pandas()
            partition_detected_values_df = detect_sql_injection(partition_df,column_names, detections)
            if partition_detected_values_df:
                csv_target_file_name = f'{output_directory_path}/{datetime.now().strftime("%Y%m%d%H%M%S")}.csv'
                partition_detected_values_df.to_csv(csv_target_file_name)


def detect_sql_injection(df: DataFrame, column_names: [], detections:[]):
    df_list = []
    for column in column_names:
         for detect_pattern in detections:
            detected_values_df = df[column].apply(detect_value, args=(detect_pattern,)).dropna()
            if not len(detected_values_df):
                continue
            detected_values_df['column_name'] =  column
            df_list.append(detected_values_df)
    if not df_list:
        return None
    return pd.concat(df_list)

def detect_value(value: str, pattern):
    matches =re.findall(pattern=pattern, string=str(value))
    if len(matches) > 0:
        return value
    return None

'''

Reduce CPU of Regex:

1. Using multi-processing techs reducing the CPU 
2. Usage of regex could be a void if columns would have type definition and validation:
   E.g. vin - type of uuid, frontLeftDoor and DriverSeatBelt would be enum   
   

'''
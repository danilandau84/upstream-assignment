import os
from datetime import datetime

import pyarrow.parquet as pq

from utils import save_dataframe_to_file, make_dir_if_not_exist, get_datetime_from_file_name, read_cached_last_timestamp


def run():
    input_directory_path = os.getenv("RAW_DATA_DIR")
    output_directory_path = os.getenv("PURGED_DIR")
    make_dir_if_not_exist(output_directory_path)
    files = os.listdir(input_directory_path)
    last_timestamp_str = read_cached_last_timestamp(os.getenv("CACHE-TIMESTAMP-PATH"))
    cache_last_timestamp = datetime.strptime(last_timestamp_str, "%d/%m/%Y, %H:%M:%S")

    for file_name in files:
        file_datetime = get_datetime_from_file_name(file_name)
        if file_datetime < cache_last_timestamp:
            continue
        file_path = os.path.join(input_directory_path, file_name)
        if os.path.isfile(file_path):
            dataset = pq.ParquetDataset(file_path)
            df = dataset.read().to_pandas()
            df = filter_invalidate_values(df)
            purge_df = align_gear_to_int_type(df)
            save_dataframe_to_file(purge_df,os.path.join(output_directory_path, file_name))


def align_gear_to_int_type(df):
    condition = df['gearPosition'] == 'NEUTRAL'
    df.loc[condition, 'gearPosition'] = '0'
    condition = df['gearPosition'] == 'REVERSE'
    df.loc[condition, 'gearPosition'] = '-1'
    df['gearPosition'] = df['gearPosition'].astype(int)
    return df

def filter_invalidate_values(df):
    df = df[(~df['manufacturer'].str.startswith(' ')) & (df['vin'].notnull()) & (df['gearPosition'].notnull())]
    return df




import os

import pandas as pd
from pip._vendor import requests
from os import path
import pyarrow.parquet as pq
import pyarrow as pa
import logging
from utils import read_cached_last_timestamp, store_cached_last_timestamp, make_dir_if_not_exist, save_dataframe_to_file


def run():
    json_text = request_data_from_source()
    df = pd.read_json(json_text)
    last_timestamp = read_cached_last_timestamp(os.getenv("CACHE-TIMESTAMP-PATH"))
    filtered_df = df[df['timestamp'] > last_timestamp]
    new_row_count = len(filtered_df)
    if new_row_count == 0:
        logging.warning('no new data found')
        return
    logging.info("new rows fetched:" + str(new_row_count))
    dfs_by_partitions = divid_dataframes_by_partition(filtered_df)
    for name, sub_df in dfs_by_partitions.items():
        create_or_merge_parquet_file(sub_df, name)
    store_cached_last_timestamp(filtered_df['timestamp'].max(), os.getenv("CACHE-TIMESTAMP-PATH"))
 

def request_data_from_source() -> str:
    response = requests.get(os.getenv("DATA_REQUEST_URL"))
    if response.status_code == 200:
        return response.text


def create_or_merge_parquet_file(sub_df, value):
    dir_name = os.getenv("RAW_DATA_DIR")
    make_dir_if_not_exist(dir_name)
    file_name = f'{dir_name}/{value}.parquet'
    is_file_exist = path.exists(file_name)
    if is_file_exist:
        new_table = pa.Table.from_pandas(sub_df)
        existing_table = pq.read_table(file_name)
        table = pa.concat_tables([existing_table, new_table])
        pq.write_table(table, file_name)
    else:
        save_dataframe_to_file(sub_df, file_name)


def divid_dataframes_by_partition(filtered_df):
    filtered_df['partition_name'] = filtered_df['timestamp'].dt.strftime('%Y-%m-%d-%H')
    partition_names = filtered_df['partition_name'].unique()
    dfs_by_partitions = {partition_name: filtered_df[filtered_df['partition_name'] == partition_name] for partition_name
                         in partition_names}
    return dfs_by_partitions


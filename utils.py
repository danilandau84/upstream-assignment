import os
from os  import path, makedirs
from datetime import datetime
import pyarrow as pa
import pyarrow.parquet as pq
from pandas import DataFrame

datetime_format = "%m/%d/%Y, %H:%M:%S"

def read_cached_last_timestamp(cache_file_name) -> str:
    if not path.isfile(cache_file_name):
        with open(cache_file_name, 'w+') as f:
            f.write(datetime.min.strftime(datetime_format))
    with open(cache_file_name) as f:
        timestamp_text = f.readline()
        if not timestamp_text:
            return datetime.min.strftime(datetime_format)
        return timestamp_text


def store_cached_last_timestamp(timestamp_to_update: datetime, cache_file_name):
    with open(cache_file_name, 'w') as f:
        f.write(timestamp_to_update.strftime(datetime_format))

def make_dir_if_not_exist(dir_name: str):
    if not path.isdir(dir_name):
        makedirs(dir_name)


def save_dataframe_to_file(df: DataFrame, file_path: str):
    table = (pa.Table.from_pandas(df))
    pq.write_table(table, file_path)

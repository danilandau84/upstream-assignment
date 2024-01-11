import os
from datetime import datetime

import pandas as pd
import pandasql as ps
from utils import make_dir_if_not_exist
import pyarrow.parquet as pq


def run():
    generate_vin_last_state_report()
    generate_fastest_vehicles_report()

def generate_vin_last_state_report():
    input_directory_path = 'data/purged-data'
    output_directory_path = 'data/vin-last-state-report'
    '''
    for efficiency, you can generate new report based on last report 
    and process the recent purged file that haven't analyzed yet
    '''
    df = merge_all_parquets_into_dataframe(input_directory_path, output_directory_path)

    latest_timestamp_per_vin_df = df.groupby('vin')['timestamp'].max().reset_index().rename(columns={'vin': 'Vin', 'timestamp': 'Last_reported_timestamp' })
    report_df = ps.sqldf('select a.Vin, a.Last_reported_timestamp,'
                      'b.frontLeftDoorState as front_left_door_state, b.wipersState as Wipers_state   '
                      'from latest_timestamp_per_vin_df a left join df b '
                      'on b.vin= a.Vin and b.timestamp = a.Last_reported_timestamp '
                      'where b.wipersState is not null and b.frontLeftDoorState is not null')
    csv_target_file_name = f'{output_directory_path}/{datetime.now().strftime("%Y%m%d%H%M%S")}.csv'
    report_df.to_csv(csv_target_file_name)


def merge_all_parquets_into_dataframe(input_directory_path, output_directory_path):
    make_dir_if_not_exist(output_directory_path)
    files = os.listdir(input_directory_path)
    all_partition_dataframes = []
    for file_name in files:
        file_path = os.path.join(input_directory_path, file_name)
        if os.path.isfile(file_path):
            dataset = pq.ParquetDataset(file_path)
            partition_df = dataset.read().to_pandas()
            all_partition_dataframes.append(partition_df)
    df = pd.concat(all_partition_dataframes)
    return df


def generate_fastest_vehicles_report():
    input_directory_path = 'data/purged-data'
    output_directory_path = 'data/fastest_vehicles_per_hour_report'
    make_dir_if_not_exist(output_directory_path)
    files = os.listdir(input_directory_path)
    all_partition_dataframes = []
    for file_name in files:
        file_path = os.path.join(input_directory_path, file_name)
        if os.path.isfile(file_path):
            dataset = pq.ParquetDataset(file_path)
            partition_df = dataset.read().to_pandas()
            df = partition_df.groupby('vin')['velocity'].max().reset_index()
            df = df.sort_values(by='velocity', ascending=False).head(10)
            df['date-hour'] = file_name.replace('.parquet','')
            all_partition_dataframes.append(df)
    report_df = pd.concat(all_partition_dataframes)
    csv_target_file_name = f'{output_directory_path}/{datetime.now().strftime("%Y%m%d%H%M%S")}.csv'
    report_df.to_csv(csv_target_file_name)



import os
import json

import pandas as pd
import numpy as np
from tqdm import tqdm


def json_dump(obj, filename):
    with open(filename, mode='w') as f:
        json.dump(obj, f, ensure_ascii=False)


# Функция для анализа используемой памяти датасетом
def analyse_df_by_chunk(path_to_dataset, path_to_save, chunksize=100_000):
    file_size = os.path.getsize(path_to_dataset)
    total_memory_usage = 0
    df = pd.read_csv(path_to_dataset, chunksize=chunksize)
    
    columns_stats = {}

    for chunk in tqdm(df):
        chunk_memory_usage_stat = chunk.memory_usage(deep=True)
        total_memory_usage += float(chunk_memory_usage_stat.sum())
        for column in chunk:
            if column in columns_stats:
                columns_stats[column]['total_memory'] += float(chunk_memory_usage_stat[column])
            else:
                columns_stats[column] = {
                    'total_memory': float(chunk_memory_usage_stat[column]),
                    'dtype': str(chunk.dtypes[column])
                }
    
    for col in columns_stats.keys():
        columns_stats[col]['memory_space_percentage'] = round(columns_stats[col]['total_memory'] / total_memory_usage * 100, 2)
        columns_stats[col]['total_memory'] = columns_stats[col]['total_memory'] // 1024 # KB
    
    columns_stats = dict(sorted(list(columns_stats.items()), key=lambda x: x[1]['total_memory'], reverse=True))
    results = {
        'file_size': file_size // 1024, # KB
        'file_in_memory_size': total_memory_usage // 1024, # KB
        'columns_stats': columns_stats
    }

    # print(results)
    json_dump(results, path_to_save)

    # print(f'file size           = {file_size // 1024:10} КБ')
    # print(f'file in memory size = {total_memory_usage // 1024:10} КБ')
    # for col in columns_stats.keys():
    #     print(
    #         f'{col:30}: \
    #         {columns_stats[col]["total_memory"]:5} КБ: \
    #         {columns_stats[col]["memory_space_percentage"]:5} %: \
    #         {columns_stats[col]["dtype"]}'
    #     )
    
    return results


def analyse_df(df, file_size, path_to_save):
    memory_usage_stat = df.memory_usage(deep=True)
    total_memory_usage = float(memory_usage_stat.sum())
    columns_stats = {}

    for column in df:
        columns_stats[column] = {
            'total_memory': float(memory_usage_stat[column]) // 1024,
            'memory_space_percentage': round(memory_usage_stat[column] / total_memory_usage * 100, 2),
            'dtype': str(df.dtypes[column])
        }
    
    columns_stats = dict(sorted(list(columns_stats.items()), key=lambda x: x[1]['total_memory'], reverse=True))
    results = {
        'file_size': file_size // 1024, # KB
        'file_in_memory_size': total_memory_usage // 1024, # KB
        'columns_stats': columns_stats
    }

    # print(results)
    json_dump(results, path_to_save)

    # print(f'file size           = {file_size // 1024:10} КБ')
    # print(f'file in memory size = {total_memory_usage // 1024:10} КБ')
    # for col in columns_stats.keys():
    #     print(
    #         f'{col:30}: \
    #         {columns_stats[col]["total_memory"]:5} КБ: \
    #         {columns_stats[col]["memory_space_percentage"]:5} %: \
    #         {columns_stats[col]["dtype"]}'
    #     )
    
    return results

def optimize_dtype_object(df, treashold=0.5):
    for column in df.select_dtypes(include=['object']):
        len_column = len(df[column])
        len_unique = len(df[column].unique())
        if len_unique / len_column < treashold:
            df[column] = df[column].astype('category')
    
    return df


def optimize_dtype_int(df):
    for column in df.select_dtypes(include=['int']):
        is_unsigned = False not in set(df[column] >= 0)
        if is_unsigned:
            df[column] = pd.to_numeric(df[column], downcast='unsigned')
        else:
            df[column] = pd.to_numeric(df[column], downcast='signed')
        
    return df


def optimize_dtype_float(df):
    for column in df.select_dtypes(include=['float']):
        df[column] = pd.to_numeric(df[column], downcast='float')
        
    return df


def optimize_df(df):
    df = optimize_dtype_object(df)
    df = optimize_dtype_int(df)
    df = optimize_dtype_float(df)

    return df


def save_df_dtype(df, usecols, path_to_save):
    params = {
        column_name: df[column_name].dtype.name
        for column_name in usecols
    }
    json_dump(params, path_to_save)
    return params


def read_dtype_param_df(path):
    with open(path, mode='r') as f:
        return json.load(f)
    

def convert_str_to_dtype(dtypes):
    for key in dtypes.keys():
        if dtypes[key] == 'category':
            dtypes[key] = pd.CategoricalDtype
        else:
            dtypes[key] = np.dtype(dtypes[key])
    return dtypes
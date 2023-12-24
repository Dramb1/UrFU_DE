import sys
import os
import json

import pandas as pd
from tqdm import tqdm

lESSON_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(lESSON_DIR)

import utils
import plot


if __name__ == "__main__":
    path_to_file = os.path.join(lESSON_DIR, "data/[2]automotive.csv.zip")
    path_to_file_save = os.path.join(lESSON_DIR, "results/2/memory_usage_dataset_without_optimize.json")
    path_to_file_save_optimize = os.path.join(lESSON_DIR, "results/2/df_optimize.csv")
    path_to_file_save_optimize_memory = os.path.join(lESSON_DIR, "results/2/memory_usage_dataset_with_optimize.json")
    path_to_file_save_dtype_df_optimize = os.path.join(lESSON_DIR, "results/2/dtype_df_optimize.json")
    path_save_filtered_df = os.path.join(lESSON_DIR, "results/2/filter_df.csv")

    file_size = os.path.getsize(path_to_file)
    memory_usage = utils.analyse_df_by_chunk(path_to_file, path_to_file_save, chunksize=500_000)
    print(f"Memory size dataset without optimize: {memory_usage['file_in_memory_size']} MB")
    
    dtypes_df_optimize = {
        'stockNum': pd.StringDtype(),
        'brandName': pd.CategoricalDtype(),
        'modelName': pd.CategoricalDtype(),
        'askPrice': pd.StringDtype(),
        'isNew': pd.CategoricalDtype(),
        'color': pd.CategoricalDtype(),
        'vf_Wheels': pd.StringDtype(),
        'vf_WheelSizeFront': pd.StringDtype(), 
        'vf_Windows': pd.StringDtype(),
        'vf_WheelSizeRear': pd.StringDtype() 
    }

    has_header = True
    total_size = 0
    df_chank = pd.read_csv(
        path_to_file, usecols=lambda x: x in dtypes_df_optimize.keys(), 
        dtype=dtypes_df_optimize,
        chunksize=500_000
    )
    for part in tqdm(df_chank):
        total_size += part.memory_usage(deep=True).sum()
        part.dropna().to_csv(path_save_filtered_df, mode="a", header=has_header, index=False)
        has_header = False

    df = pd.read_csv(path_save_filtered_df)
    df_optimize = utils.optimize_df(df)
    print(f"Memory size dataset with optimize: {df_optimize.memory_usage(deep=True).sum() // (1024**2)} MB")
    utils.analyse_df(df_optimize, file_size, path_to_file_save_optimize_memory)

    dtypes_df_optimize = utils.save_df_dtype(df_optimize, dtypes_df_optimize.keys(), path_to_file_save_dtype_df_optimize)
    
    dtypes_df_optimize = utils.convert_str_to_dtype(dtypes_df_optimize)
    print(dtypes_df_optimize)

    print(df_optimize.info())
    path_save_plot_histogram = os.path.join(lESSON_DIR, "results/2/plot_histogram_brandName.jpg")
    path_save_plot_pie = os.path.join(lESSON_DIR, "results/2/plot_pie_isNew.jpg")
    path_save_plot_linear_graphics = os.path.join(lESSON_DIR, "results/2/plot_linear_graphics_brandName_to_vf_Windows.jpg")
    path_save_plot_boxplot = os.path.join(lESSON_DIR, "results/2/plot_boxplot_isNew_to_askPrice.jpg")
    path_save_plot_correlation = os.path.join(lESSON_DIR, "results/2/plot_correlation.jpg")
    
    plot.plot_histogram(df_optimize, "brandName", path_save_plot_histogram)
    plot.plot_pie(df_optimize, "isNew", path_save_plot_pie)
    plot.plot_linear_graphics(df_optimize, "brandName", "vf_Windows", path_save_plot_linear_graphics)
    plot.plot_boxplot(df_optimize, "isNew", "askPrice", path_save_plot_boxplot)
    plot.plot_correlation(df_optimize, ["askPrice", "vf_Wheels", "vf_WheelSizeFront", "vf_WheelSizeRear", "vf_Windows"], path_save_plot_correlation)
    
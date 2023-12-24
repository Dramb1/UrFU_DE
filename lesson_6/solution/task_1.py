import sys
import os
import json

import pandas as pd

lESSON_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(lESSON_DIR)

import utils
import plot


if __name__ == "__main__":
    path_to_file = os.path.join(lESSON_DIR, "data/[1]game_logs.csv")
    path_to_file_save = os.path.join(lESSON_DIR, "results/1/memory_usage_dataset_without_optimize.json")
    path_to_file_save_optimize = os.path.join(lESSON_DIR, "results/1/df_optimize.csv")
    path_to_file_save_optimize_memory = os.path.join(lESSON_DIR, "results/1/memory_usage_dataset_with_optimize.json")
    path_to_file_save_dtype_df_optimize = os.path.join(lESSON_DIR, "results/1/dtype_df_optimize.json")
    path_save_filtered_df = os.path.join(lESSON_DIR, "results/1/filter_df.csv")

    file_size = os.path.getsize(path_to_file)
    df = pd.read_csv(path_to_file)
    print(f"Memory size dataset without optimize: {df.memory_usage(deep=True).sum() // (1024**2)} MB")
    utils.analyse_df(df, file_size, path_to_file_save)
    df_optimize = utils.optimize_df(df)
    print(f"Memory size dataset with optimize: {df_optimize.memory_usage(deep=True).sum() // (1024**2)} MB")
    utils.analyse_df(df_optimize, file_size, path_to_file_save_optimize_memory)

    columns_to_analyse = [
        "date", "number_of_game", "day_of_week", "park_id",
        "v_manager_name", "length_minutes", "v_hits",
        "h_hits", "h_walks", "h_errors"
    ]

    dtypes_df_optimize = utils.save_df_dtype(df_optimize, columns_to_analyse, path_to_file_save_dtype_df_optimize)

    has_header = True
    total_size = 0
    dtypes_df_optimize = utils.read_dtype_param_df(path_to_file_save_dtype_df_optimize)
    for part in pd.read_csv(
        path_to_file, usecols=lambda x: x in dtypes_df_optimize.keys(), 
        dtype=dtypes_df_optimize,
        chunksize=500_000
    ):
        total_size += part.memory_usage(deep=True).sum()
        part.dropna().to_csv(path_save_filtered_df, mode="a", header=has_header, index=False)
        has_header = False

    dtypes_df_optimize = utils.convert_str_to_dtype(dtypes_df_optimize)
    print(dtypes_df_optimize)
    df_filtered = pd.read_csv(path_save_filtered_df, usecols=lambda x: x in dtypes_df_optimize.keys(),
            dtype=dtypes_df_optimize)
    print(df_filtered.info())
    path_save_plot_histogram = os.path.join(lESSON_DIR, "results/1/plot_histogram_day_of_week.jpg")
    path_save_plot_pie = os.path.join(lESSON_DIR, "results/1/plot_pie_number_of_game.jpg")
    path_save_plot_linear_graphics = os.path.join(lESSON_DIR, "results/1/plot_linear_graphics_day_of_week_to_length_minutes.jpg")
    path_save_plot_boxplot = os.path.join(lESSON_DIR, "results/1/plot_boxplot_number_of_game_to_h_errors.jpg")
    path_save_plot_correlation = os.path.join(lESSON_DIR, "results/1/plot_correlation.jpg")
    
    plot.plot_histogram(df_filtered, "day_of_week", path_save_plot_histogram)
    plot.plot_pie(df_filtered, "number_of_game", path_save_plot_pie)
    plot.plot_linear_graphics(df_filtered, "day_of_week", "length_minutes", path_save_plot_linear_graphics)
    plot.plot_boxplot(df_filtered, "number_of_game", "h_errors", path_save_plot_boxplot)
    plot.plot_correlation(df_filtered, ["v_hits", "h_hits", "h_walks", "h_errors"], path_save_plot_correlation)
    
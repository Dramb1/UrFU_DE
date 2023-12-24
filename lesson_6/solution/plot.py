import os

import matplotlib.pyplot as plt
import seaborn as sns


def plot_histogram(df, column, path_to_save):
    plt.figure(figsize=(30,15))
    plot = df[column].hist(grid=False, edgecolor='black')
    plot.get_figure().savefig(path_to_save)
    plt.close()


def plot_pie(df, column, path_to_save):
    plt.figure()
    groups = df.groupby([column])[column].count()
    circ = groups.plot(kind='pie', y=groups.keys(), autopct='%1.0f%%')
    circ.get_figure().savefig(path_to_save)
    plt.close()


def plot_linear_graphics(df, droup, column, path_to_save):
    plt.figure(figsize=(30,15))
    plt.plot(df.groupby([droup])[column].sum().values, marker='*', color='green')
    plt.savefig(path_to_save)
    plt.close()


def plot_boxplot(df, column1, column2, path_to_save):
    plt.figure(figsize=(30,15))
    plot = sns.boxplot(data=df, x=column1, y=column2)
    plot.get_figure().savefig(path_to_save)
    plt.close()


def plot_correlation(df, columns, path_to_save):
    data = df.copy()
    plt.figure(figsize=(16,16))
    plot = sns.heatmap(data[columns].corr())
    plot.get_figure().savefig(path_to_save)
    plt.close()

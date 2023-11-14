import os
import csv
import json
import re

from bs4 import BeautifulSoup


lESSON_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def get_item(soup: BeautifulSoup) -> dict:
    item = {}

    item["name"] = soup.find_all("h1")[0].get_text().strip()
    
    # get list parameters the product
    parameters = soup.find_all("span", attrs={"class": "tj2"})
    
    # get list info about parameters the product
    parameters_info = soup.find_all("dd", attrs={"class": "j2t"})
    
    for i in range(len(parameters)):
        parameter = parameters[i].get_text().rstrip("/с").rstrip("Мб").replace(",", "").strip()
        parameter_info = parameters_info[i].get_text().rstrip("Гбит/с").rstrip("ТБ").rstrip("об/мин").strip()
        try:
            parameter_info = float(parameter_info)
        except:
            pass
        item[parameter] = parameter_info

    return item


def compute_frequency(stat: dict, count: int) -> dict:
    stat_freq = {}
    for key in stat:
        stat_freq[key] = stat[key] / count

    return stat_freq


if __name__ == '__main__': 
    data = []
    
    list_files = os.listdir(os.path.join(lESSON_DIR, "data/5/1"))
    stat_speed = {"sum": 0.0, "min": -1.0, "max": 0.0}
    stat_form_factor = {}
    for file_name in list_files:
        with open(os.path.join(lESSON_DIR, f"data/5/1/{file_name}")) as f:
            soup = BeautifulSoup(f, 'html.parser')
        
        item = get_item(soup)
        stat_speed["sum"] += item["Скорость вращения шпинделя HDD"]
        if stat_speed["min"] == -1.0:
            stat_speed["min"] = item["Скорость вращения шпинделя HDD"]
        else: 
            stat_speed["min"] = min(stat_speed["min"], item["Скорость вращения шпинделя HDD"])
        stat_speed["max"] = max(stat_speed["max"], item["Скорость вращения шпинделя HDD"])
        if item["Форм-фактор"] not in stat_form_factor.keys():
            stat_form_factor[item["Форм-фактор"]] = 1
        else:
            stat_form_factor[item["Форм-фактор"]] += 1
        data.append(item)

    data = sorted(data, key=lambda x: x["name"])
    stat_speed["avg"] = stat_speed["sum"] / len(data)
    stat_form_factor = compute_frequency(stat_form_factor, len(data))
    
    print(f"Форм-фактор field mark frequency: {stat_form_factor}")
    print(f"Field speed statistics: {stat_speed}")
    
    with open(os.path.join(lESSON_DIR, "results/r_text_5_1_var_1.json"), "w") as file:
        json.dump(data, file, ensure_ascii=False)
    
    filtered_data = []
    for item in data:
        if int(item["Объем"]) > 1:
            filtered_data.append(item)
    
    with open(os.path.join(lESSON_DIR, "results/r_text_5_1_var_1_filtered.json"), "w") as file:
        json.dump(filtered_data, file, ensure_ascii=False)
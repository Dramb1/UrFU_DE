import os
import csv
import json
import re

from bs4 import BeautifulSoup


lESSON_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def get_item(soup: BeautifulSoup) -> dict:
    item = {}

    item["name"] = soup.find_all("name")[0].get_text().strip()
    item["constellation"] = soup.find_all("constellation")[0].get_text().strip()
    item["spectral_class"] = soup.find_all("spectral-class")[0].get_text().strip()
    item["radius"] = float(soup.find_all("radius")[0].get_text().strip())
    item["rotation"] = float(soup.find_all("rotation")[0].get_text().replace("days", "").strip())
    item["age"] = float(soup.find_all("age")[0].get_text().replace("billion years", "").strip())
    item["distance"] = float(soup.find_all("distance")[0].get_text().replace("million km", "").strip())
    item["absolute_magnitude"] = float(soup.find_all("absolute-magnitude")[0].get_text().replace("million km", "").strip())

    return item


def compute_frequency(stat: dict, count: int) -> dict:
    stat_freq = {}
    for key in stat:
        stat_freq[key] = stat[key] / count

    return stat_freq


if __name__ == '__main__': 
    data = []
    
    list_files = os.listdir(os.path.join(lESSON_DIR, "data/3/"))
    stat_age = {"sum": 0.0, "min": -1.0, "max": 0.0}
    stat_constellation = {}
    for file_name in list_files:
        with open(os.path.join(lESSON_DIR, f"data/3/{file_name}")) as f:
            soup = BeautifulSoup(f, 'xml')
        
        item = get_item(soup)
        stat_age["sum"] += item["age"]
        if stat_age["min"] == -1.0:
            stat_age["min"] = item["age"]
        else:
            stat_age["min"] = min(stat_age["min"], item["age"])
        stat_age["max"] = max(stat_age["max"], item["age"])
        if item["constellation"] not in stat_constellation.keys():
            stat_constellation[item["constellation"]] = 1
        else:
            stat_constellation[item["constellation"]] += 1
        data.append(item)

    data = sorted(data, key=lambda x: x["name"])
    stat_age["avg"] = stat_age["sum"] / len(data)
    stat_constellation = compute_frequency(stat_constellation, len(data))
    
    print(f"Сonstellation field mark frequency: {stat_constellation}")
    print(f"Field age statistics: {stat_age}")
    
    with open(os.path.join(lESSON_DIR, "results/r_text_3_var_1.json"), "w") as file:
        json.dump(data, file, ensure_ascii=False)
    
    filtered_data = []
    for item in data:
        if int(item["age"]) > 3.38:
            filtered_data.append(item)
    
    print(len(data))
    print(len(filtered_data))
    with open(os.path.join(lESSON_DIR, "results/r_text_3_var_1_filtered.json"), "w") as file:
        json.dump(filtered_data, file, ensure_ascii=False)
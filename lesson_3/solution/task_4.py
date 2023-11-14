import os
import csv
import json
import re

from bs4 import BeautifulSoup


lESSON_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def get_item(soup: BeautifulSoup) -> dict:
    items = []
    item = {}

    clothings = soup.find_all("clothing")
    
    for clothing in clothings:
        item = {}
        for data in clothing.contents:
            if data.name is not None:
                try:
                    item[data.name] = float(data.get_text().strip())
                except:
                    item[data.name] = data.get_text().strip()
        items.append(item)

    return items


def compute_frequency(stat: dict, count: int) -> dict:
    stat_freq = {}
    for key in stat:
        stat_freq[key] = stat[key] / count

    return stat_freq


if __name__ == '__main__': 
    data = []
    
    list_files = os.listdir(os.path.join(lESSON_DIR, "data/4/"))
    stat_price = {"sum": 0.0, "min": -1, "max": 0.0}
    stat_category = {}
    for file_name in list_files:
        with open(os.path.join(lESSON_DIR, f"data/4/{file_name}")) as f:
            soup = BeautifulSoup(f, 'xml')
        
        items = get_item(soup)
        for item in items:
            stat_price["sum"] += item["price"]
            if stat_price["min"] == -1:
                stat_price["min"] = item["price"]
            else:
                stat_price["min"] = min(stat_price["min"], item["price"])
            stat_price["max"] = max(stat_price["max"], item["price"])
            if item["category"] not in stat_category.keys():
                stat_category[item["category"]] = 1
            else:
                stat_category[item["category"]] += 1
        data += items


    data = sorted(data, key=lambda x: x["id"])
    stat_price["avg"] = stat_price["sum"] / len(data)
    stat_category = compute_frequency(stat_category, len(data))
    
    print(f"Category field mark frequency: {stat_category}")
    print(f"Field price statistics: {stat_price}")
    
    with open(os.path.join(lESSON_DIR, "results/r_text_4_var_1.json"), "w") as file:
        json.dump(data, file, ensure_ascii=False)
    
    filtered_data = []
    for item in data:
        if int(item["price"]) > 558509:
            filtered_data.append(item)
    
    print(len(data))
    print(len(filtered_data))
    with open(os.path.join(lESSON_DIR, "results/r_text_4_var_1_filtered.json"), "w") as file:
        json.dump(filtered_data, file, ensure_ascii=False)
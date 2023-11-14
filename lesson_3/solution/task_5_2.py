import os
import csv
import json
import re

from bs4 import BeautifulSoup


lESSON_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def get_item(soup: BeautifulSoup) -> dict:
    items = []
    item = {}

    products = soup.find_all(
        "tr",
        attrs={"id": "vue-model-short-block"}
    )

    for product in products:
        item = {}
        item["name"] = product.find_all("div", attrs={"class": "list-img h"})[0].img["alt"].strip()
        item["href"] = product.find_all("div", attrs={"class": "list-img h"})[0].a["href"].strip()
        item["src"] = product.find_all("div", attrs={"class": "list-img h"})[0].img["src"].strip()
        
        # get list description the product
        description = product.find_all("div", attrs={"class": "m-s-f2"})[0]
        for data in description.find_all("div"):
            if data.get_text().strip() == "":
                continue

            # Split dscription by name and value
            data_list = data.get_text().strip().split(":")
            name_description, data_description = data_list
            name_description, data_description = name_description.strip(), data_description.strip()
            if "Объем оперативной памяти" == name_description:
                item[name_description] = int(data_description.replace("ГБ", "").strip())
            else:
                item[name_description] = data_description

        items.append(item)

    return items


def compute_frequency(stat: dict, count: int) -> dict:
    stat_freq = {}
    for key in stat:
        stat_freq[key] = stat[key] / count

    return stat_freq


if __name__ == '__main__': 
    data = []
    
    list_files = os.listdir(os.path.join(lESSON_DIR, "data/5/2/"))
    stat_volume_ram = {"sum": 0.0, "min": -1, "max": 0.0, "count": 0}
    stat_series = {}
    for file_name in list_files:
        with open(os.path.join(lESSON_DIR, f"data/5/2/{file_name}")) as f:
            soup = BeautifulSoup(f, 'html.parser')
        
        items = get_item(soup)
        for item in items:
            if "Объем оперативной памяти" in item.keys():
                stat_volume_ram["sum"] += item["Объем оперативной памяти"]
                stat_volume_ram["count"] += 1
                if stat_volume_ram["min"] == -1:
                    stat_volume_ram["min"] = item["Объем оперативной памяти"]
                else:
                    stat_volume_ram["min"] = min(stat_volume_ram["min"], item["Объем оперативной памяти"])
                stat_volume_ram["max"] = max(stat_volume_ram["max"], item["Объем оперативной памяти"])
            if item["Серия"] not in stat_series.keys():
                stat_series[item["Серия"]] = 1
            else:
                stat_series[item["Серия"]] += 1
        data += items


    data = sorted(data, key=lambda x: x["name"])
    stat_volume_ram["avg"] = stat_volume_ram["sum"] / stat_volume_ram["count"]
    stat_volume_ram.pop("count")
    stat_series = compute_frequency(stat_series, len(data))
    
    print(f"Series field mark frequency: {stat_series}")
    print(f"Field volume ram statistics: {stat_volume_ram}")
    
    with open(os.path.join(lESSON_DIR, "results/r_text_5_2_var_1.json"), "w") as file:
        json.dump(data, file, ensure_ascii=False)
    
    filtered_data = []
    for item in data:
        if "Объем оперативной памяти" in item.keys() and item["Объем оперативной памяти"] > 8:
            filtered_data.append(item)
    
    with open(os.path.join(lESSON_DIR, "results/r_text_5_2_var_1_filtered.json"), "w") as file:
        json.dump(filtered_data, file, ensure_ascii=False)
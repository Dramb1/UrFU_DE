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
        "div",
        attrs={"class": "product-item"}
    )

    for product in products:
        item["id"] = product.a["data-id"]
        item["href"] = product.find_all("a")[1]["href"]
        item["src"] = product.find_all("img")[0]["src"]
        item["name"] = product.find_all("span")[0].get_text().strip()
        item["price"] = product.find_all("price")[0].get_text().replace("₽", "").replace(" ", "").strip()
        item["bonus"] = product.find_all("strong")[0].get_text().replace("+ начислим", "").replace("бонусов", "").strip()
        
        item["property"] = {}
        property_prod = product.find_all("li")
        for prop in property_prod:
            item["property"][prop["type"]] = prop.get_text().strip()

        items.append(item)

    return items


def compute_frequency(stat: dict, count: int) -> dict:
    stat_freq = {}
    for key in stat:
        stat_freq[key] = stat[key] / count

    return stat_freq


if __name__ == '__main__': 
    data = []
    
    list_files = os.listdir(os.path.join(lESSON_DIR, "data/2/"))
    stat_price = {"sum": 0.0, "min": -1, "max": 0.0}
    stat_name = {}
    for file_name in list_files:
        with open(os.path.join(lESSON_DIR, f"data/2/{file_name}")) as f:
            soup = BeautifulSoup(f, 'html.parser')
        
        items = get_item(soup)

        for item in items:
            stat_price["sum"] += float(item["price"])
            if stat_price["min"] == -1:
                stat_price["min"] = float(item["price"])
            else:
                stat_price["min"] = min(stat_price["min"], float(item["price"]))
            stat_price["max"] = max(stat_price["max"], float(item["price"]))
            if item["name"] not in stat_name.keys():
                stat_name[item["name"]] = 1
            else:
                stat_name[item["name"]] += 1
        data += items


    data = sorted(data, key=lambda x: x["id"])
    stat_price["avg"] = stat_price["sum"] / len(data)
    stat_name = compute_frequency(stat_name, len(data))
    
    print(f"Name field mark frequency: {stat_name}")
    print(f"Field price statistics: {stat_price}")
    
    with open(os.path.join(lESSON_DIR, "results/r_text_2_var_1.json"), "w") as file:
        json.dump(data, file, ensure_ascii=False)
    
    filtered_data = []
    for item in data:
        if int(item["price"]) > 458955:
            filtered_data.append(item)
    
    with open(os.path.join(lESSON_DIR, "results/r_text_2_var_1_filtered.json"), "w") as file:
        json.dump(filtered_data, file, ensure_ascii=False)
import os
import csv
import json
import re

from bs4 import BeautifulSoup


lESSON_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def get_item(soup: BeautifulSoup) -> dict:
    item = {}

    item["city"] = soup.find_all(
        "span",
        string=re.compile("Город:")
    )[0].get_text().replace("Город:", "").strip()
    item["building"] = soup.find_all(
        "h1",
        attrs={"class": "title", "id": "1"}
    )[0].get_text().replace("Строение:", "").strip()
    
    address = soup.find_all(
        "p",
        attrs={"class": "address-p"}
    )[0].get_text().replace("Улица:", "")
    index_ind = address.find("Индекс")
    index = address[index_ind:].replace("Индекс:", "").strip()
    address = address[:index_ind].strip()
    item["street"] = address
    item["index"] = index

    item["building_info"] = {}
    item["building_info"]["floors"] = soup.find_all(
        "span",
        attrs={"class": "floors"}
    )[0].get_text().replace("Этажи:", "").strip()
    item["building_info"]["year"] = soup.find_all(
        "span",
        attrs={"class": "year"}
    )[0].get_text().replace("Построено в ", "").strip()
    item["building_info"]["parking"] = soup.find_all(
        "span",
        string=re.compile("Парковка:")
    )[0].get_text().replace("Парковка:", "").strip()
    item["img"] = soup.find_all("img")[0]["src"]
    item["score"] = soup.find_all(
        "span",
        string=re.compile("Рейтинг:")
    )[0].get_text().replace("Рейтинг:", "").strip()
    item["views"] = soup.find_all(
        "span",
        string=re.compile("Просмотры:")
    )[0].get_text().replace("Просмотры:", "").strip()

    return item


def compute_frequency(stat: dict, count: int) -> dict:
    stat_freq = {}
    for key in stat:
        stat_freq[key] = stat[key] / count

    return stat_freq


if __name__ == '__main__': 
    data = []
    
    list_files = os.listdir(os.path.join(lESSON_DIR, "data/1/"))
    stat_score = {"sum": 0.0, "min": 5.0, "max": 0.0}
    stat_parking = {}
    for file_name in list_files:
        with open(os.path.join(lESSON_DIR, f"data/1/{file_name}")) as f:
            soup = BeautifulSoup(f, 'html.parser')
        
        item = get_item(soup)
        stat_score["sum"] += float(item["score"])
        stat_score["min"] = min(stat_score["min"], float(item["score"]))
        stat_score["max"] = max(stat_score["max"], float(item["score"]))
        if item["building_info"]["parking"] not in stat_parking.keys():
            stat_parking[item["building_info"]["parking"]] = 1
        else:
            stat_parking[item["building_info"]["parking"]] += 1
        data.append(item)

    data = sorted(data, key=lambda x: x["city"])
    stat_score["avg"] = stat_score["sum"] / len(data)
    stat_parking = compute_frequency(stat_parking, len(data))
    
    print(f"Parking field mark frequency: {stat_parking}")
    print(f"Field score statistics: {stat_score}")
    
    with open(os.path.join(lESSON_DIR, "results/r_text_1_var_1.json"), "w") as file:
        json.dump(data, file, ensure_ascii=False)
    
    filtered_data = []
    for item in data:
        if int(item["building_info"]["year"]) > 2006:
            filtered_data.append(item)
    
    with open(os.path.join(lESSON_DIR, "results/r_text_1_var_1_filtered.json"), "w") as file:
        json.dump(filtered_data, file, ensure_ascii=False)
import os
import json
import csv
from pymongo import MongoClient


lESSON_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def connect_to_db():
    client = MongoClient("mongodb://localhost:27017/")
    collection = client.cities_db
    return collection.cities


def insert_data_to_db(collection, data):
    collection.insert_many(data)


def read_json(path):
    with open(path, 'r', encoding='utf-8') as file:
        items = json.load(file)
    return items


def read_csv(path):
    # Структура csv ['id', 'name', 'state_id', 'state_code', 'country_id', 'country_code', 'latitude', 'longitude']
    with open(path, encoding='utf-8', newline='') as csvfile:
        spamreader = csv.reader(csvfile, delimiter=';', quotechar='|')
        titles = spamreader.__next__()
        items = []
        for row in spamreader:
            item = {}

            for i in range(len(row)):
                try:
                    if "id" == titles[i]:
                        item["id"] = int(row[i])
                    elif "state_id" == titles[i]:
                        item["state_id"] = int(row[i])
                    elif "country_id" == titles[i]:
                        item["country_id"] = int(row[i])
                    elif "latitude" == titles[i] or "longitude" == titles[i]:
                        item[titles[i]] = float(row[i])
                    else:
                        item[titles[i]] = row[i].strip('"')
                except:
                    print(row, i)
            
            items.append(item)
    return items


def sort_by_latitude(collection, limit=10):
    results = []
    for city in collection.find({}, limit=limit).sort({"latitude": 1}):
        city['_id'] = str(city['_id'])
        results.append(city)
    return results


def filter_by_longitude_sorted_by_name(collection):
    results = []
    for city in collection.find({"longitude": {"$gt": 10, "$lt": 30}}, limit=15).sort({"name": -1}):
        city['_id'] = str(city['_id'])
        results.append(city)
    return results


def filter_by_country_code_sorted_by_latitude(collection):
    results = []
    for city in collection.find({"country_code": {"$in": ["US", "RU"]}}, limit=15).sort({"latitude": -1}):
        city['_id'] = str(city['_id'])
        results.append(city)
    return results


def filter_by_country_id_and_state_id_sorted_by_name(collection):
    results = []
    for city in collection.find(
        {
            "country_id": 233,
            "state_id": {"$in": [1443, 1455, 1401]} 
         }, limit=10).sort({"name": 1}):
        city['_id'] = str(city['_id'])
        results.append(city)
    return results


def count_docs(collection):
    return {
        "count": collection.count_documents(
            {
                "$or": [
                    {"latitude": {"$gt": 10, "$lte": 30}},
                    {"longitude": {"$gt": -20, "$lt": 0}}
                ]
            }
        )
    }


def count_by_country_code(collection):
    results = []
    for stat in collection.aggregate(
        [
            {
                "$group": {
                    "_id": "$country_code",
                    "count": {"$sum": 1}
                }
            },
            {"$sort": {"count": -1}}
        ]
    ):
        results.append(stat)
    return results


def statistics_column1_by_column2(collection, column1, column2="result"):
    charact = [{"$group": {
        "_id": f"${column2}" if column2 != "result" else column2,
        "max": {"$max": f"${column1}"},
        "avg": {"$avg": f"${column1}"},
        "min": {"$min": f"${column1}"}}}]
    results = []
    for stat in collection.aggregate(charact):
        results.append(stat)
    return results

def statistic_max_latitude_by_min_longitude(collection):
    results = []
    for stat in collection.aggregate(
        [
            {"$sort": {'longitude': 1, "latitude": -1}},
            {"$limit": 1}
        ]
    ):
        stat['_id'] = str(stat['_id'])
        results.append(stat)
    return results  


def sort_statistics_longitude_by_state_code_with_condition(collection):
    results = []
    for stat in collection.aggregate(
        [
            {"$match": {'state_code': {'$in': ["KYA", "SA", "AK"]}}},
            {
                "$group": {
                "_id": "$name",
                "max": {"$max": "$longitude"},
                "avg": {"$avg": "$longitude"},
                "min": {"$min": "$longitude"}},
            },
            {"$sort": {"_id": -1}},
        ]
    ):
        results.append(stat)
    return results  


def statistics_longitude_by_condition(collection):
    results = []
    for stat in collection.aggregate(
        [
            {
                "$match":
                {
                    'country_code': {'$in': ['RU', "US", "KN"]},
                    '$or': [{'latitude': {"$gt": 0, "$lt": 25}}, {'latitude': {"$gt": -30, "$lt": -10}}]
                }
            },
            {
                "$group": 
                {
                    "_id": "result",
                    "max": {"$max": "$longitude"},
                    "avg": {"$avg": "$longitude"},
                    "min": {"$min": "$longitude"}
                }
            }
        ]
    ):
        results.append(stat)
    return results


def delete_longitude(collection):
    print(
        collection.delete_many(
            {
                "$or":
                [
                    {"longitude": {"$lt": -140}},
                    {"longitude": {'$gt': 140}}
                ]
            }
        )
    )


def update_column_by_pred(collection, column_name):
    filter = {
        "state_code": {'$in': ['GA', "MD", "SC"]},
        "latitude": {"$gt": -35, "$lt": 35}
    }
    update = {"$inc": {column_name: 2.5}}
    print(collection.update_many(filter, update))


def delete_collection_by_pred(collection):
    filter = {
        'country_code': {'$in': ['AT', "EE", "PH"]},
        '$or': [{'latitude': {"$gt": -35, "$lt": 35}}, {'longitude': {"$gt": -40, "$lt": 30}}]
    }
    print(collection.delete_many(filter))


# Добавление новой колонки в таблицу
def add_column(collection, column_name, default_value):
    print(collection.update_many({}, {"$set": {column_name: default_value}}))


# Функция для обновления значения в новой колонке
def update_column(collection, column_name, new_value):
    print(collection.update_many({"country_code": "US"}, {"$inc": {column_name: new_value}}))


if __name__ == "__main__":
    path_to_json = os.path.join(lESSON_DIR, "data/4/cities_1.json")
    path_to_csv = os.path.join(lESSON_DIR, "data/4/cities_2.csv")
    path_to_file_save1 = os.path.join(lESSON_DIR, "results/4/r_task_4_var_01_top10_sort_by_latitude.json")
    path_to_file_save2 = os.path.join(lESSON_DIR, "results/4/r_task_4_var_01_top15_filter_by_longitude_sorted_by_name.json")
    path_to_file_save3 = os.path.join(lESSON_DIR, "results/4/r_task_4_var_01_top10_filter_by_country_id_and_state_id_sorted_by_name.json")
    path_to_file_save4 = os.path.join(lESSON_DIR, "results/4/r_task_4_var_01_count_docs.json")
    path_to_file_save5 = os.path.join(lESSON_DIR, "results/4/r_task_4_var_01_filter_by_country_code_sorted_by_latitude.json")
    path_to_file_save6 = os.path.join(lESSON_DIR, "results/4/r_task_4_var_01_count_by_country_code.json")
    path_to_file_save7 = os.path.join(lESSON_DIR, "results/4/r_task_4_var_01_statistics_latitude_by_country_code.json")
    path_to_file_save8 = os.path.join(lESSON_DIR, "results/4/r_task_4_var_01_statistic_max_latitude_by_min_longitude.json")
    path_to_file_save9 = os.path.join(lESSON_DIR, "results/4/r_task_4_var_01_sort_statistics_longitude_by_state_code_with_condition.json")
    path_to_file_save10 = os.path.join(lESSON_DIR, "results/4/r_task_4_var_01_statistics_longitude_by_condition.json")

    items_cities_1 = read_json(path_to_json)
    items_cities_2 = read_csv(path_to_csv)

    collection = connect_to_db()

    # insert_data_to_db(collection, items_cities_1)
    # insert_data_to_db(collection, items_cities_2)

    results = sort_by_latitude(collection)
    with open(path_to_file_save1, "w", encoding="utf-8") as file:
        json.dump(results, file, ensure_ascii=False)

    results = filter_by_longitude_sorted_by_name(collection)
    with open(path_to_file_save2, "w", encoding="utf-8") as file:
        json.dump(results, file, ensure_ascii=False)

    results = filter_by_country_id_and_state_id_sorted_by_name(collection)
    with open(path_to_file_save3, "w", encoding="utf-8") as file:
        json.dump(results, file, ensure_ascii=False)

    results = count_docs(collection)
    with open(path_to_file_save4, "w", encoding="utf-8") as file:
        json.dump(results, file, ensure_ascii=False)

    results = filter_by_country_code_sorted_by_latitude(collection)
    with open(path_to_file_save5, "w", encoding="utf-8") as file:
        json.dump(results, file, ensure_ascii=False)

    results = count_by_country_code(collection)
    with open(path_to_file_save6, "w", encoding="utf-8") as file:
        json.dump(results, file, ensure_ascii=False)

    # Статистика latitude по country_code
    results = statistics_column1_by_column2(collection, column1="latitude", column2="country_code")
    with open(path_to_file_save7, "w", encoding="utf-8") as file:
        json.dump(results, file, ensure_ascii=False)

    results = statistic_max_latitude_by_min_longitude(collection)
    with open(path_to_file_save8, "w", encoding="utf-8") as file:
        json.dump(results, file, ensure_ascii=False)

    results = sort_statistics_longitude_by_state_code_with_condition(collection)
    with open(path_to_file_save9, "w", encoding="utf-8") as file:
        json.dump(results, file, ensure_ascii=False)

    results = statistics_longitude_by_condition(collection)
    with open(path_to_file_save10, "w", encoding="utf-8") as file:
        json.dump(results, file, ensure_ascii=False)

    delete_longitude(collection)

    delete_collection_by_pred(collection)

    add_column(collection, "temp_column", 0)

    update_column(collection, "temp_column", 1)

    update_column_by_pred(collection, "temp_column")
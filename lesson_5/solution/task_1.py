import os
import json
from pymongo import MongoClient

lESSON_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def connect_to_db():
    client = MongoClient("mongodb://localhost:27017/")
    collection = client.person_db
    return collection.person


def insert_data_to_db(collection, data):
    collection.insert_many(data)


def get_items(path_to_data):
    items = []
    with open(path_to_data, "r", encoding="utf-8") as file:
        item = {}
        for line in file.readlines():
            if "=====" in line:
                items.append(item)
                item = {}
                continue
            key, value = line.split("::")
            value = value.strip()
            item[key] = int(value) if value.isnumeric() else value
    return items


def sort_by_salary(collection):
    results = []
    for person in collection.find({}, limit=10).sort({"salary": -1}):
        person['_id'] = str(person['_id'])
        results.append(person)
    return results


def filter_by_age_sorted_by_salary(collection):
    results = []
    for person in collection.find({"age": {"$lt": 30}}, limit=15).sort({"salary": -1}):
        person['_id'] = str(person['_id'])
        results.append(person)
    return results


def filter_by_city_and_job_sorted_by_age(collection):
    results = []
    for person in collection.find(
        {
            "city": "Лас-Росас",
            "job": {"$in": ["Водитель", "Медсестра", "Строитель"]} 
         }, limit=10).sort({"age": 1}):
        person['_id'] = str(person['_id'])
        results.append(person)
    return results


def count_docs(collection):
    return {
        "count": collection.count_documents(
            {
                "age": {"$gte": 20, "$lte": 40},
                "year": {"$gte": 2019, "$lte": 2022},
                "$or": [
                    {"salary": {"$gt": 50000, "$lte": 75000}},
                    {"salary": {"$gt": 125000, "$lt": 150000}}
                ]
            }
        )
    }


if __name__ == "__main__":
    path_to_file = os.path.join(lESSON_DIR, "data/1/task_1_item.text")
    path_to_file_save = os.path.join(lESSON_DIR, "results/1/r_task_1_var_01_top10_sort_by_salary.json")
    path_to_file_save1 = os.path.join(lESSON_DIR, "results/1/r_task_1_var_01_top15_filtered_by_age_sorted_by_salary.json")
    path_to_file_save2 = os.path.join(lESSON_DIR, "results/1/r_task_1_var_01_top15_filtered_by_city_and_job_sorted_by_age.json")
    path_to_file_save3 = os.path.join(lESSON_DIR, "results/1/r_task_1_var_01_count_docs.json")
    
    items = get_items(path_to_file)
    collection = connect_to_db()

    # insert_data_to_db(collection, items)
    results = sort_by_salary(collection)
    with open(path_to_file_save, "w", encoding="utf-8") as file:
        json.dump(results, file, ensure_ascii=False)

    results = filter_by_age_sorted_by_salary(collection)
    with open(path_to_file_save1, "w", encoding="utf-8") as file:
        json.dump(results, file, ensure_ascii=False)

    results = filter_by_city_and_job_sorted_by_age(collection)
    with open(path_to_file_save2, "w", encoding="utf-8") as file:
        json.dump(results, file, ensure_ascii=False)
    
    results = count_docs(collection)
    with open(path_to_file_save3, "w", encoding="utf-8") as file:
        json.dump(results, file, ensure_ascii=False)

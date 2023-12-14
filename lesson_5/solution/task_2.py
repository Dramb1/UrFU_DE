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
    with open(path_to_data, "r", encoding="utf-8") as file:
        items = json.load(file)
    return items


def count_by_job(collection):
    results = []
    for stat in collection.aggregate(
        [
            {
                "$group": {
                    "_id": "$job",
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

def statistic_max_salary_by_min_age(collection):
    results = []
    for stat in collection.aggregate(
        [
            {"$sort": {'age': 1, "salary": -1}},
            {"$limit": 1}
        ]
    ):
        stat['_id'] = str(stat['_id'])
        results.append(stat)
    return results  


def statistic_max_age_by_min_salary(collection):
    results = []
    for stat in collection.aggregate(
        [
            {"$sort": {'age': -1, "salary": 1}},
            {"$limit": 1}
        ]
    ):
        stat['_id'] = str(stat['_id'])
        results.append(stat)
    return results 


def sort_statistics_age_by_city_with_condition(collection):
    results = []
    for stat in collection.aggregate(
        [
            {"$match": {'salary': {'$gt': 50000}}},
            {
                "$group": {
                "_id": "$city",
                "max": {"$max": "$age"},
                "avg": {"$avg": "$age"},
                "min": {"$min": "$age"}}
            },
            {"$sort": {"_id": -1}},
        ]
    ):
        results.append(stat)
    return results  


def statistics_salary_by_condition(collection):
    results = []
    for stat in collection.aggregate(
        [
            {
                "$match":
                {
                    'city': {'$in': ['Будапешт', "Ташкент", "Прага"]},
                    'job': {'$in': ["IT-специалист", "Строитель", "Бухгалтер"]},
                    '$or': [{'age': {"$gt": 18, "$lt": 25}}, {'age': {"$gt": 50, "$lt": 65}}]
                }
            },
            {
                "$group": 
                {
                    "_id": "result",
                    "max": {"$max": "$salary"},
                    "avg": {"$avg": "$salary"},
                    "min": {"$min": "$salary"}
                }
            }
        ]
    ):
        results.append(stat)
    return results  


def sort_statistics_salary_by_condition(collection):
    results = []
    for stat in collection.aggregate(
        [
            {
                "$match":
                {
                    'job': {'$in': ["IT-специалист"]},
                    'age': {"$gt": 18, "$lt": 25}
                }
            },
            {
                "$group": 
                {
                    "_id": "$city",
                    "max": {"$max": "$salary"},
                    "avg": {"$avg": "$salary"},
                    "min": {"$min": "$salary"}
                }
            },
            {"$sort": {"_id": -1}},
        ]
    ):
        results.append(stat)
    return results


if __name__ == "__main__":
    path_to_file = os.path.join(lESSON_DIR, "data/2/task_2_item.json")
    path_to_file_save = os.path.join(lESSON_DIR, "results/2/r_task_2_var_01_stat_by_salary.json")
    path_to_file_save1 = os.path.join(lESSON_DIR, "results/2/r_task_2_var_01_count_by_job.json")
    path_to_file_save2 = os.path.join(lESSON_DIR, "results/2/r_task_2_var_01_stat_salary_by_city.json")
    path_to_file_save3 = os.path.join(lESSON_DIR, "results/2/r_task_2_var_01_stat_salary_by_job.json")
    path_to_file_save4 = os.path.join(lESSON_DIR, "results/2/r_task_2_var_01_stat_age_by_city.json")
    path_to_file_save5 = os.path.join(lESSON_DIR, "results/2/r_task_2_var_01_stat_age_by_job.json")
    path_to_file_save6 = os.path.join(lESSON_DIR, "results/2/r_task_2_var_01_statistic_max_salary_by_min_age.json")
    path_to_file_save7 = os.path.join(lESSON_DIR, "results/2/r_task_2_var_01_statistic_max_age_by_min_salary.json")
    path_to_file_save8 = os.path.join(lESSON_DIR, "results/2/r_task_2_var_01_sort_statistics_age_by_city_with_condition.json")
    path_to_file_save9 = os.path.join(lESSON_DIR, "results/2/r_task_2_var_01_statistics_salary_by_condition.json")
    path_to_file_save10 = os.path.join(lESSON_DIR, "results/2/r_task_2_var_01_sort_statistics_salary_by_condition.json")
    
    items = get_items(path_to_file)
    collection = connect_to_db()

    # insert_data_to_db(collection, items)
    
    results = statistics_column1_by_column2(collection, column1="salary")
    with open(path_to_file_save, "w", encoding="utf-8") as file:
        json.dump(results, file, ensure_ascii=False)

    results = count_by_job(collection)
    with open(path_to_file_save1, "w", encoding="utf-8") as file:
        json.dump(results, file, ensure_ascii=False)

    results = statistics_column1_by_column2(collection, column1="salary", column2="city")
    with open(path_to_file_save2, "w", encoding="utf-8") as file:
        json.dump(results, file, ensure_ascii=False)

    results = statistics_column1_by_column2(collection, column1="salary", column2="job")
    with open(path_to_file_save3, "w", encoding="utf-8") as file:
        json.dump(results, file, ensure_ascii=False)

    results = statistics_column1_by_column2(collection, column1="age", column2="city")
    with open(path_to_file_save4, "w", encoding="utf-8") as file:
        json.dump(results, file, ensure_ascii=False)

    results = statistics_column1_by_column2(collection, column1="age", column2="job")
    with open(path_to_file_save5, "w", encoding="utf-8") as file:
        json.dump(results, file, ensure_ascii=False)

    results = statistic_max_salary_by_min_age(collection)
    with open(path_to_file_save6, "w", encoding="utf-8") as file:
        json.dump(results, file, ensure_ascii=False)

    results = statistic_max_age_by_min_salary(collection)
    with open(path_to_file_save7, "w", encoding="utf-8") as file:
        json.dump(results, file, ensure_ascii=False)

    results = sort_statistics_age_by_city_with_condition(collection)
    with open(path_to_file_save8, "w", encoding="utf-8") as file:
        json.dump(results, file, ensure_ascii=False)

    results = statistics_salary_by_condition(collection)
    with open(path_to_file_save9, "w", encoding="utf-8") as file:
        json.dump(results, file, ensure_ascii=False)
    
    results = sort_statistics_salary_by_condition(collection)
    with open(path_to_file_save10, "w", encoding="utf-8") as file:
        json.dump(results, file, ensure_ascii=False)
    
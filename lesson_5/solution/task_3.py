import os
import json
import pickle
from pymongo import MongoClient

lESSON_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def connect_to_db():
    client = MongoClient("mongodb://localhost:27017/")
    collection = client.person_db
    return collection.person


def insert_data_to_db(collection, data):
    collection.insert_many(data)


def get_items(path_to_data):
    with open(path_to_data, "rb") as file:
        items = pickle.load(file)
    return items


def delete_salary(collection):
    print(
        collection.delete_many(
            {
                "$or":
                [
                    {"salary": {"$lt": 25000}},
                    {"salary": {'$gt': 175000}}
                ]
            }
        )
    )


def update_age(collection):
    print(collection.update_many({}, {"$inc": {"age": 1}}))


def update_salary_by_column(collection, column, names_column, number):
    print(
        collection.update_many(
            {f'{column}': {"$in": names_column}},
            {"$mul": {"salary": number}}
        )
    )


def update_salary_by_pred(collection):
    filter = {
        "city": {'$in': ['Будапешт', "Ташкент", "Прага"]},
        "job": {'$in': ["IT-специалист", "Строитель", "Бухгалтер"]},
        "age": {"$gte": 18, "$lt": 30}
    }
    update = {"$mul": {"salary": 1.1}}
    print(collection.update_many(filter, update))


def delete_collection_by_pred(collection):
    filter = {
        'city': {'$in': ['Афины', "Минск", "Санкт-Петербург"]},
        'job': {'$in': ["Учитель", "Водитель", "Медсестра"]},
        '$or': [{'age': {"$gt": 18, "$lt": 25}}, {'age': {"$gt": 50, "$lt": 65}}]
    }
    print(collection.delete_many(filter))


if __name__ == "__main__":
    path_to_file = os.path.join(lESSON_DIR, "data/3/task_3_item.pkl")
    
    items = get_items(path_to_file)
    collection = connect_to_db()

    # insert_data_to_db(collection, items)
    
    delete_salary(collection)

    update_age(collection)

    update_salary_by_column(collection, "job", ["IT-специалист", "Строитель", "Бухгалтер"], 1.05)
    
    update_salary_by_column(collection, "city", ['Будапешт', "Ташкент", "Прага"], 1.07)
    
    update_salary_by_pred(collection)

    delete_collection_by_pred(collection)

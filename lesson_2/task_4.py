import os
import json
import pickle

import numpy as np


WS_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


if __name__ == "__main__":
    path_to_file = os.path.join(WS_DIR, "UrFU_DE/lesson_2/data/4/price_info_0.json")
    path_to_pkl = os.path.join(WS_DIR, "UrFU_DE/lesson_2/data/4/products_0.pkl")
    with open(path_to_file, "r") as file:
        price_info = json.load(file)

    with open(path_to_pkl, "rb") as file:
        products = pickle.load(file)

    product_info = {}
    for i in range(len(products)):
        data = products[i]
        product_info[data["name"]] = i

    for data in price_info:
        method = data["method"]
        i = product_info[data["name"]]
        if method == "sum":
            products[i]["price"] += data["param"]
        elif method == "sub":
            products[i]["price"] -= data["param"]
        elif method == "percent+":
            products[i]["price"] *= (1 + data["param"])
        elif method == "percent-":
            products[i]["price"] *= (1 - data["param"])
        products[i]["price"] = round(products[i]["price"], 2)

    path_save = os.path.join(
        WS_DIR, "UrFU_DE/lesson_2/results/r_task_4_var_1.pkl"
    )
    with open(path_save, "wb") as file:
        file.write(pickle.dumps(products))

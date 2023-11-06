import os
import json

import numpy as np
import msgpack


WS_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


if __name__ == "__main__":
    path_to_file = os.path.join(WS_DIR, "task_2/3/products_1.json")
    with open(path_to_file, "r") as file:
        data = json.load(file)

    product = {}
    for sample in data:
        if sample["name"] not in product.keys():
            product[sample["name"]] = {
                "avg": sample["price"], "min": sample["price"], "max": sample["price"]
            }
        else:
            product[sample["name"]]["avg"] = (product[sample["name"]]["avg"] + sample["price"]) / 2
            product[sample["name"]]["max"] = max(sample["price"], product[sample["name"]]["max"])
            product[sample["name"]]["min"] = min(sample["price"], product[sample["name"]]["min"])

    path_save = os.path.join(
        WS_DIR, "UrFU_DE/lesson_2/results/r_task_3_var_1.json"
    )
    path_msgpack_save = os.path.join(
        WS_DIR, "UrFU_DE/lesson_2/results/r_task_3_var_1.msgpack"
    )

    print(product)
    with open(path_save, "w") as file:
        json.dump(product, file)
    
    with open(path_msgpack_save, "wb") as file:
        file.write(msgpack.dumps(product))

    file_stats = os.stat(path_save)
    file_stats_msgpack = os.stat(path_msgpack_save)
    print("SIZE SAVE FILE in Bytes: ", file_stats.st_size)
    print("SIZE SAVE MSGPACK FILE in Bytes: ", file_stats_msgpack.st_size)

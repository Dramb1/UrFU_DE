import numpy as np
import os
import json


WS_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


if __name__ == '__main__':
    path_to_file = os.path.join(WS_DIR, "task_2/1/matrix_1.npy")
    array = np.load(path_to_file)

    res = {
        "sum": 0.0,
        "avr": 0.0,
        "sumMD": 0.0,
        "avrMD": 0.0,
        "sumSD": 0.0,
        "avrSD": 0.0,
        "max": float(array[0, 0]),
        "min": float(array[0, 0])
    }

    for i in range(array.shape[0]):
        for j in range(array.shape[1]):
            res["sum"] += array[i, j]
            if i == j:
                res["sumMD"] += array[i, j]
            elif i + j == array.shape[0]:
                res["sumSD"] += array[i, j]
            res["max"] = max(res["max"], float(array[i, j]))
            res["min"] = min(res["min"], float(array[i, j]))

    res["avr"] = res["sum"] / (array.shape[0] * array.shape[0])
    res["avrMD"] = res["sumMD"] / array.shape[0]
    res["avrSD"] = res["sumSD"] / array.shape[0]

    with open(os.path.join(WS_DIR, "UrFU_DE/lesson_2/results/r_task_1_var_1.json"), "w") as file:
        json.dump(res, file)

    array = np.array(array / res["max"])
    np.save(os.path.join(WS_DIR, "UrFU_DE/lesson_2/results/r_task_1_var_1.npy"), array)

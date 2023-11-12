import os
import csv 
import json
import pickle

import msgpack
import numpy as np

WS_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


def count_frequency(dct, count):
    dct_freq = {}
    for key in dct:
        dct_freq[key] = dct[key] / count
    
    return dct_freq

if __name__ == "__main__":
    path_to_file = os.path.join(WS_DIR, "UrFU_DE/lesson_2/data/task_5.csv")
    path_to_file_save = os.path.join(WS_DIR, "UrFU_DE/lesson_2/results/r_task_5_var_1.csv")
    
    data = {}
    
    fout = open(path_to_file_save, "w", newline="")
    writer  = csv.writer(fout)
    with open(path_to_file, "r") as file:
        reader = csv.reader(file)
        
        title = reader.__next__()
        title = title[:9]
        title.pop(1)
        writer.writerow(title)
        
        data = {}
        for row in reader:
            row = row[:9]
            row.pop(1)
            if row[1] == "":
                continue
            if row[0] not in data.keys():
                data[row[0]] = {
                    "Count": 1,
                    "Data_value": {"min": float(row[1]), "max": float(row[1]), "sum": float(row[1]), "list_value": [float(row[1])]},
                    "STATUS": {row[2]: 1},
                    "UNITS": {row[3]: 1},
                    "MAGNTUDE": {row[4]: 1},
                    "Subject": {row[5]: 1},
                    "Group": {row[6]: 1},
                    "Series_title_1": {row[7]: 1},
                }
            else:
                data[row[0]]["Count"] += 1
                data[row[0]]["Data_value"]["min"] = min(data[row[0]]["Data_value"]["min"], float(row[1]))
                data[row[0]]["Data_value"]["max"] = max(data[row[0]]["Data_value"]["max"], float(row[1]))
                data[row[0]]["Data_value"]["sum"] += float(row[1])
                data[row[0]]["Data_value"]["list_value"].append(float(row[1]))
                for i in range(2, 8):
                    if row[i] not in data[row[0]][title[i]].keys():
                        data[row[0]][title[i]][row[i]] = 1
                    else:
                        data[row[0]][title[i]][row[i]] += 1
            writer.writerow(row)

    for key in data:
        sample = data[key]
        std = np.std(sample["Data_value"]["list_value"])
        sample["Data_value"]["std"] = std
        sample["Data_value"]["avg"] = sample["Data_value"]["sum"] / sample["Count"]
        sample["Data_value"].pop("list_value")
        
        list_key = list(sample.keys())
        for key_sample in list_key[2:]:
            sample[key_sample] = count_frequency(sample[key_sample], sample["Count"])

    print(data)
    fout.close()

    path_save_pkl = os.path.join(
        WS_DIR, "UrFU_DE/lesson_2/results/r_task_5_var_1.pkl"
    )
    with open(path_save_pkl, "wb") as file:
        file.write(pickle.dumps(data))
    
    path_save = os.path.join(
        WS_DIR, "UrFU_DE/lesson_2/results/r_task_5_var_1.json"
    )
    path_msgpack_save = os.path.join(
        WS_DIR, "UrFU_DE/lesson_2/results/r_task_5_var_1.msgpack"
    )
    with open(path_save, "w") as file:
        json.dump(data, file)
    
    with open(path_msgpack_save, "wb") as file:
        file.write(msgpack.dumps(data))

    file_stats_json = os.stat(path_save)
    file_stats_csv = os.stat(path_to_file_save)
    file_stats_msgpack = os.stat(path_msgpack_save)
    file_stats_pickle = os.stat(path_save_pkl)
    print("SIZE SAVE FILE in Bytes: ", file_stats_json.st_size)
    print("SIZE SAVE FILE CSV in Bytes: ", file_stats_csv.st_size)
    print("SIZE SAVE MSGPACK FILE in Bytes: ", file_stats_msgpack.st_size)
    print("SIZE SAVE PICKLE FILE in Bytes: ", file_stats_pickle.st_size)

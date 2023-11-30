'''
CREATE TABLE table1 (
    id          INTEGER    PRIMARY KEY AUTOINCREMENT
                           NOT NULL
                           UNIQUE,
    artist      TEXT (256) NOT NULL,
    song        TEXT (256) NOT NULL,
    duration_ms INTEGER    NOT NULL,
    year        INTEGER    NOT NULL,
    tempo       REAL       NOT NULL,
    genre       TEXT (256) NOT NULL
);
'''

import os
import sqlite3
import json
import pickle


lESSON_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def connect_to_db(path_to_db):
    connection = sqlite3.connect(path_to_db)
    connection.row_factory = sqlite3.Row
    return connection


def insert_data_to_db(connection, data):
    cursor = connection.cursor()
    cursor.executemany(
        """
        INSERT INTO table1 (artist, song, duration_ms, year, tempo, genre) 
        VALUES(:artist, :song, :duration_ms, :year, :tempo, :genre)
        """, data
    )
    connection.commit()
    cursor.close()


def top_views(connection, top=11):
    cursor = connection.cursor()
    res = cursor.execute(
        '''
        SELECT * 
        FROM table1 
        ORDER BY year DESC LIMIT ?
        ''', [top]
    )
    
    items = []
    for row in res.fetchall():
        items.append(dict(row))
    cursor.close()
    
    return items


def statistics_by_duration_ms(connection):
    cursor = connection.cursor()
    res = cursor.execute(
        '''
        SELECT 
            SUM(duration_ms) as sum,
            AVG(duration_ms) as avg,
            MIN(duration_ms) as min, 
            MAX(duration_ms) as max
        FROM table1
        '''
    )
    res = dict(res.fetchone())
    cursor.close()
    return res


def compute_frequency_genre(connection):
    cursor = connection.cursor()
    res = cursor.execute(
        '''
        SELECT 
            CAST(COUNT(*) as REAL) / (SELECT COUNT(*) FROM table1) as count,
            genre
        FROM table1
        GROUP BY genre
        '''
    )

    stat_freq = []
    for row in res.fetchall():
        stat_freq.append(dict(row))

    cursor.close()
    return stat_freq


def top_predicate_views(connection, min_tempo=103, top=16):
    cursor = connection.cursor()
    res = cursor.execute(
        '''
        SELECT * 
        FROM table1 
        WHERE tempo >= ?
        ORDER BY year DESC LIMIT ?
        ''', [min_tempo, top]
    )
    
    items = []
    for row in res.fetchall():
        items.append(dict(row))
    cursor.close()
    
    return items


if __name__ == "__main__":
    path_to_file1 = os.path.join(lESSON_DIR, "data/3/task_3_var_01_part_1.text")
    path_to_file2 = os.path.join(lESSON_DIR, "data/3/task_3_var_01_part_2.pkl")
    path_to_db = os.path.join(lESSON_DIR, "data/second_db")
    path_to_file_save = os.path.join(lESSON_DIR, "results/3/r_task_3_var_01_.json")
    path_to_file_save2 = os.path.join(lESSON_DIR, "results/3/r_task_3_var_01_top11_filtered.json")
    

    with open(path_to_file1, "r") as file:
        data1 = []
        dct = {}
        for st in file.readlines():
            if st == "=====\n":
                data1.append(dct)                
                dct = {}
                continue

            key, value = st.split("::")
            key, value = key.strip(), value.strip()
            if key not in ['artist', 'genre', 'duration_ms', 'song', 'tempo', 'year']:
                continue
            if key in ["duration_ms", "year"]:
                dct[key] = int(value)
            elif key == "tempo":
                dct[key] = float(value)
            else:    
                dct[key.strip()] = value.strip()
    

    with open(path_to_file2, "rb") as file:
        data2 = pickle.load(file)

    for data in data2:
        data.pop("popularity")
        data.pop("energy")
        data.pop("acousticness")
        for key in data:
            if key in ["duration_ms", "year"]:
                data[key] = int(data[key])
            elif key == "tempo":
                data[key] = float(data[key])

    connection = connect_to_db(path_to_db)

    # insert_data_to_db(connection, data1)
    # insert_data_to_db(connection, data2)
    
    data = top_views(connection)
    with open(path_to_file_save, "w") as file:
        json.dump(data, file, ensure_ascii=False)

    duration_ms_stat = statistics_by_duration_ms(connection)
    print(f"Field duration_ms statistics: {duration_ms_stat}")

    stat_genre = compute_frequency_genre(connection)
    print(f"Genre field mark frequency: {stat_genre}")

    data = top_predicate_views(connection)
    with open(path_to_file_save2, "w") as file:
        json.dump(data, file, ensure_ascii=False)
    


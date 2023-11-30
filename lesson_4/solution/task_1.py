'''
CREATE TABLE table1 (
    id         INTEGER    PRIMARY KEY AUTOINCREMENT
                          NOT NULL
                          UNIQUE,
    name       TEXT (256) NOT NULL,
    street     TEXT (256) NOT NULL,
    city       TEXT (256) NOT NULL,
    zipcode    INTEGER    NOT NULL,
    floors     INTEGER    NOT NULL,
    year       INTEGER    NOT NULL,
    parking    TEXT       NOT NULL,
    prob_price INTEGER    NOT NULL,
    views      INTEGER    NOT NULL
);
'''

import os
import pickle
import sqlite3
import json


lESSON_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def connect_to_db(path_to_db):
    connection = sqlite3.connect(path_to_db)
    connection.row_factory = sqlite3.Row
    return connection


def insert_data_to_db(connection, data):
    cursor = connection.cursor()
    cursor.executemany(
        """
        INSERT INTO table1 (name, street, city, zipcode, floors, year, parking, prob_price, views) 
        VALUES(:name, :street, :city, :zipcode, :floors, :year, :parking, :prob_price, :views)
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


def statistics_by_floors(connection):
    cursor = connection.cursor()
    res = cursor.execute(
        '''
        SELECT 
            SUM(floors) as sum,
            AVG(floors) as avg,
            MIN(floors) as min, 
            MAX(floors) as max
        FROM table1
        '''
    )
    res = dict(res.fetchone())
    cursor.close()
    return res


def compute_frequency_city(connection):
    cursor = connection.cursor()
    res = cursor.execute(
        '''
        SELECT 
            CAST(COUNT(*) as REAL) / (SELECT COUNT(*) FROM table1) as count,
            city
        FROM table1
        GROUP BY city
        '''
    )

    stat_freq = []
    for row in res.fetchall():
        stat_freq.append(dict(row))

    cursor.close()
    return stat_freq


def top_predicate_views(connection, min_floors=5, top=11):
    cursor = connection.cursor()
    res = cursor.execute(
        '''
        SELECT * 
        FROM table1 
        WHERE floors >= ?
        ORDER BY year DESC LIMIT ?
        ''', [min_floors, top]
    )
    
    items = []
    for row in res.fetchall():
        items.append(dict(row))
    cursor.close()
    
    return items


if __name__ == "__main__":
    path_to_file = os.path.join(lESSON_DIR, "data/1/task_1_var_01_item.pkl")
    path_to_db = os.path.join(lESSON_DIR, "data/first_db")
    path_to_file_save = os.path.join(lESSON_DIR, "results/1/r_task_1_var_01_top11.json")
    path_to_file_save2 = os.path.join(lESSON_DIR, "results/1/r_task_1_var_01_top11_filtered.json")
    
    # with open(path_to_file, "rb") as file:
    #     data = pickle.load(file)

    connection = connect_to_db(path_to_db)

    # insert_data_to_db(connection, data)
    
    data = top_views(connection)
    with open(path_to_file_save, "w") as file:
        json.dump(data, file, ensure_ascii=False)

    floors_stat = statistics_by_floors(connection)
    print(f"Field floors statistics: {floors_stat}")

    stat_city = compute_frequency_city(connection)
    print(f"City field mark frequency: {stat_city}")

    data = top_predicate_views(connection)
    with open(path_to_file_save2, "w") as file:
        json.dump(data, file, ensure_ascii=False)

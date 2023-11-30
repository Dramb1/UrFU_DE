'''
CREATE TABLE table2 (
    id            INTEGER    UNIQUE
                             PRIMARY KEY AUTOINCREMENT
                             NOT NULL,
    id_table1     INTEGER    REFERENCES table1 (id) 
                             NOT NULL,
    rating        REAL       NOT NULL,
    convenience   INTEGER    NOT NULL,
    security      INTEGER    NOT NULL,
    functionality INTEGER    NOT NULL,
    comment       TEXT (256) NOT NULL
);

'''

import os
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
        INSERT INTO table2 (id_table1, rating, convenience, security, functionality, comment) 
        VALUES(
            (SELECT id FROM table1 WHERE name = :name),
            :rating, :convenience, :security, :functionality, :comment)
        """, data
    )
    connection.commit()
    cursor.close()


def first_query(connection, city="Ташкент"):
    cursor = connection.cursor()
    res = cursor.execute(
    '''
        SELECT table2.* 
        FROM table2, table1
        WHERE table2.id_table1 = table1.id AND table1.city = ?              
    ''', [city])
    
    items = []
    for row in res.fetchall():
        items.append(dict(row))
    cursor.close()
    return items   


def statistics_by_rating(connection, parking=1):
    cursor = connection.cursor()
    res = cursor.execute(
        '''
        SELECT 
            SUM(table2.rating) as sum,
            AVG(table2.rating) as avg,
            MIN(table2.rating) as min, 
            MAX(table2.rating) as max
        FROM table2, table1
        WHERE table2.id_table1 = table1.id AND table1.parking = ?
        ''', [parking]
    )
    
    res = dict(res.fetchone())
    cursor.close()
    return res


def third_query(connection, min_floors=2, max_floors=6):
    cursor = connection.cursor()
    res = cursor.execute(
        '''
        SELECT table2.*
        FROM table2, table1
        WHERE table2.id_table1 = table1.id AND table1.floors > ? AND table1.floors < ? 
        ''', [min_floors, max_floors]
    )

    stat_freq = []
    for row in res.fetchall():
        stat_freq.append(dict(row))

    cursor.close()
    return stat_freq


if __name__ == "__main__":
    path_to_file = os.path.join(lESSON_DIR, "data/2/task_2_var_01_subitem.json")
    path_to_db = os.path.join(lESSON_DIR, "data/first_db")
    path_to_file_save = os.path.join(lESSON_DIR, "results/2/r_task_2_var_01_.json")
    path_to_file_save2 = os.path.join(lESSON_DIR, "results/2/r_task_2_var_01_top11_filtered.json")
    
    # with open(path_to_file, "rb") as file:
    #     data = json.load(file)

    connection = connect_to_db(path_to_db)

    # insert_data_to_db(connection, data)
    
    data = first_query(connection)
    with open(path_to_file_save, "w") as file:
        json.dump(data, file, ensure_ascii=False)

    rating_stat = statistics_by_rating(connection)
    print(f"Field rating statistics: {rating_stat}")

    data = third_query(connection)
    with open(path_to_file_save2, "w") as file:
        json.dump(data, file, ensure_ascii=False)
    
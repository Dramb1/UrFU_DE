'''
CREATE TABLE table1 (
    id          INTEGER    PRIMARY KEY AUTOINCREMENT
                           UNIQUE
                           NOT NULL,
    name        TEXT (256) NOT NULL,
                           UNIQUE,
    price       REAL       NOT NULL,
    quantity    INTEGER    NOT NULL,
    category    TEXT (256) NOT NULL,
    fromCity    TEXT (256) NOT NULL,
    isAvailable TEXT (256) NOT NULL,
    views       INTEGER    NOT NULL,
    count_update INTEGER   DEFAULT (0) 
                           NOT NULL
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
        INSERT INTO table1 (name, price, quantity, category, fromCity, isAvailable, views) 
        VALUES(:name, :price, :quantity, :category, :fromCity, :isAvailable, :views)
        """, data
    )
    connection.commit()
    cursor.close()


def top_views(connection, top=10):
    cursor = connection.cursor()
    res = cursor.execute(
        '''
        SELECT * 
        FROM table1 
        ORDER BY count_update DESC LIMIT ?
        ''', [top]
    )
    
    items = []
    for row in res.fetchall():
        items.append(dict(row))
    cursor.close()
    
    return items


def statistics_by_category_price(connection):
    cursor = connection.cursor()
    res = cursor.execute(
        '''
        SELECT 
            category,
            CAST(COUNT(*) as REAL) as count,
            SUM(price) as sum,
            AVG(price) as avg,
            MIN(price) as min, 
            MAX(price) as max
        FROM table1
        GROUP BY category
        '''
    )

    stat_freq = []
    for row in res.fetchall():
        stat_freq.append(dict(row))

    cursor.close()
    return stat_freq


def statistics_by_category_quantity(connection):
    cursor = connection.cursor()
    res = cursor.execute(
        '''
        SELECT 
            category,
            CAST(COUNT(*) as REAL) as count,
            SUM(quantity) as sum,
            AVG(quantity) as avg,
            MIN(quantity) as min, 
            MAX(quantity) as max
        FROM table1
        GROUP BY category
        '''
    )

    stat_freq = []
    for row in res.fetchall():
        stat_freq.append(dict(row))

    cursor.close()
    return stat_freq


def top_predicate_views(connection, category="fruit", top=15):
    cursor = connection.cursor()
    res = cursor.execute(
        '''
        SELECT * 
        FROM table1 
        WHERE category = ?
        ORDER BY price DESC LIMIT ?
        ''', [category, top]
    )
    
    items = []
    for row in res.fetchall():
        items.append(dict(row))
    cursor.close()
    
    return items


def delete_item(connection, name):
    cursor = connection.cursor()
    cursor.execute(
        '''
        DELETE FROM table1 
        WHERE name = ?
        ''', [name]
    )
    connection.commit()
    cursor.close()


def price_percent(connection, name, percent):
    cursor = connection.cursor()
    cursor.execute(
        '''
        UPDATE table1 
        SET price = ROUND((price * (1 + ?)), 2)
        WHERE name = ?
        ''', [percent, name]
    )
    cursor.execute(
        '''
        UPDATE table1 
        SET count_update = count_update + 1
        WHERE name = ?
        ''', [name]
    )
    connection.commit()
    cursor.close()


def price_abs(connection, name, value):
    cursor = connection.cursor()
    res = cursor.execute(
        '''
        UPDATE table1 
        SET price = price + ?
        WHERE name = ? AND ((price + ?) > 0)
        ''', [value, name, value]
    )
    if res.rowcount > 0:
        cursor.execute(
            '''
            UPDATE table1 
            SET count_update = count_update + 1
            WHERE name = ?
            ''', [name]
        )
        connection.commit()
    cursor.close()


def update_available(connection, name, value):
    cursor = connection.cursor()
    res = cursor.execute(
        '''
        UPDATE table1 
        SET isAvailable = ?
        WHERE name = ?
        ''', [value, name]
    )
    cursor.execute(
        '''
        UPDATE table1 
        SET count_update = count_update + 1
        WHERE name = ?
        ''', [name]
    )
    connection.commit()
    cursor.close()


def update_quantity(connection, name, value):
    cursor = connection.cursor()
    res = cursor.execute(
        '''
        UPDATE table1 
        SET quantity = quantity + ?
        WHERE name = ? AND ((quantity + ?) > 0)
        ''', [value, name, value]
    )
    if res.rowcount > 0:
        cursor.execute(
            '''
            UPDATE table1 
            SET count_update = count_update + 1
            WHERE name = ?
            ''', [name]
        )
        connection.commit()
    cursor.close()


def update_db(connection, item):
    if item["method"] == "remove":
        delete_item(connection, item["name"])
    elif item["method"] == "price_percent":
        price_percent(connection, item["name"], item["param"])
    elif item["method"] == "price_abs":
        price_abs(connection, item["name"], item["param"])
    elif item["method"] == "available":
        update_available(connection, item["name"], item["param"])
    elif "quantity" in item["method"]:
        update_quantity(connection, item["name"], item["param"])
    else:
        print("UNKNOWN METHOD!!!")


if __name__ == "__main__":
    path_to_file1 = os.path.join(lESSON_DIR, "data/4/task_4_var_01_product_data.text")
    path_to_file2 = os.path.join(lESSON_DIR, "data/4/task_4_var_01_update_data.json")
    path_to_db = os.path.join(lESSON_DIR, "data/third_db")
    path_to_file_save = os.path.join(lESSON_DIR, "results/4/r_task_4_var_01_top10.json")
    path_to_file_save2 = os.path.join(lESSON_DIR, "results/4/r_task_4_var_01_top11_filtered.json")
    

    with open(path_to_file1, "r") as file:
        data1 = []
        dct = {}
        unique_name = []
        is_same = False
        for st in file.readlines():
            if st == "=====\n":
                if is_same:
                    is_same = False
                else:
                    if 'category' not in dct:
                        dct['category'] = 'no'
                    data1.append(dct)     
                    unique_name.append(dct["name"])          
                dct = {}
                continue

            key, value = st.split("::")
            key, value = key.strip(), value.strip()
            if key == "name" and value in unique_name:
                is_same = True
            if key in ["quantity", "views"]:
                dct[key] = int(value)
            elif key == "price":
                dct[key] = float(value)
            else:    
                dct[key.strip()] = value.strip()
    
    with open(path_to_file2, "rb") as file:
        data2 = json.load(file)

    connection = connect_to_db(path_to_db)

    # insert_data_to_db(connection, data1)

    # for data in data2:
    #     update_db(connection, data)
    
    data = top_views(connection)
    with open(path_to_file_save, "w") as file:
        json.dump(data, file, ensure_ascii=False)

    category_price_stat = statistics_by_category_price(connection)
    print(f"Field category_price statistics: {category_price_stat}")

    category_quantity_stat = statistics_by_category_quantity(connection)
    print(f"Field category_quantity statistics: {category_quantity_stat}")

    data = top_predicate_views(connection)
    with open(path_to_file_save2, "w") as file:
        json.dump(data, file, ensure_ascii=False)
    


'''
CREATE TABLE countries (
    id              INTEGER    PRIMARY KEY
                               UNIQUE
                               NOT NULL,
    name            TEXT (256) NOT NULL
                               UNIQUE,
    iso3            TEXT (256) NOT NULL,
    iso2            TEXT (256) NOT NULL,
    phone_code      TEXT (256) NOT NULL,
    capital         TEXT (256) NOT NULL,
    currency        TEXT (256) NOT NULL,
    currency_symbol TEXT (256) NOT NULL,
    tld             TEXT (256) NOT NULL,
    native          TEXT (256) NOT NULL,
    region          TEXT (256) NOT NULL,
    subregion       TEXT (256) NOT NULL,
    latitude        REAL       NOT NULL,
    longitude       REAL       NOT NULL,
    count_states    INTEGER    DEFAULT (0) 
                               NOT NULL
);

CREATE TABLE states (
    id           INTEGER    PRIMARY KEY
                            UNIQUE
                            NOT NULL,
    name         TEXT (256) NOT NULL,
    country_id   INTEGER    NOT NULL
                            REFERENCES Countries (id),
    country_code TEXT (256) NOT NULL,
    state_code   TEXT (256) NOT NULL,
    latitude     REAL       NOT NULL,
    longitude    REAL       NOT NULL
);

CREATE TABLE cities (
    id           INTEGER    PRIMARY KEY
                            NOT NULL
                            UNIQUE,
    name         TEXT (256) NOT NULL,
    state_id     INTEGER    REFERENCES states (id) 
                            NOT NULL,
    state_code   TEXT (256) NOT NULL,
    country_id   INTEGER    REFERENCES Countries (id) 
                            NOT NULL,
    country_code TEXT (256) NOT NULL,
    latitude     REAL       NOT NULL,
    longitude    REAL       NOT NULL
);

'''

import os
import csv
import json
import re
import sqlite3
import csv

from bs4 import BeautifulSoup


lESSON_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def read_xml(path) -> dict:
    # Структура xml ['id', 'name', 'country_id', 'country_code', 'state_code', 'latitude', 'longitude']
    with open(path) as file:
        soup = BeautifulSoup(file, 'xml')

        states = soup.find_all("state")
        items = []
        for state in states:
            item = {}
            item["id"] = int(state.find_all("id")[0].get_text().strip())
            item["name"] = state.find_all("name")[0].get_text().strip()
            item["country_id"] = int(state.find_all("country_id")[0].get_text().strip())
            item["country_code"] = state.find_all("country_code")[0].get_text().strip()
            item["state_code"] = state.find_all("state_code")[0].get_text().strip()
            latitude = state.find_all("latitude")[0].get_text().strip()
            longitude = state.find_all("longitude")[0].get_text().strip()
            if latitude == "":
                item["latitude"] = 0.0
            else:
                item["latitude"] = float(state.find_all("latitude")[0].get_text().strip())
            if longitude == "":
                item["longitude"] = 0.0
            else:
                item["longitude"] = float(state.find_all("longitude")[0].get_text().strip())
            
            items.append(item)
    return items


def read_csv(path):
    # Структура csv ['id', 'name', 'state_id', 'state_code', 'country_id', 'country_code', 'latitude', 'longitude']
    with open(path, newline='') as csvfile:
        spamreader = csv.reader(csvfile, delimiter=',', quotechar='|')
        titles = spamreader.__next__()
        items = []
        for row in spamreader:
            item = {}

            # В базе могут быть имена "Ben Badis, Sid Bel Abbés", но разделитель ',' поэтому имя дробиться
            # собираем его до тех пор, пока не встретим числовое поле state_id
            lst = [row[0]]
            name = row[1]
            for i in range(2, len(row)):
                data = row[i]
                if data.isnumeric():
                    state_id_index = i
                    break
                else:
                    name += "," + data
            lst.append(name)
            for i in range(state_id_index, len(row)):
                data = row[i]
                lst.append(data)

            row = lst
            for i in range(len(row)):
                try:
                    if "id" == titles[i]:
                        item["id"] = int(row[i])
                    elif "state_id" == titles[i]:
                        item["state_id"] = int(row[i])
                    elif "country_id" == titles[i]:
                        item["country_id"] = int(row[i])
                    elif "latitude" == titles[i] or "longitude" == titles[i]:
                        item[titles[i]] = float(row[i])
                    else:
                        item[titles[i]] = row[i].strip('"')
                except:
                    print(row, i)
            
            items.append(item)
    return items


def read_json(path) -> dict:
    with open(path, "rb") as file:
        items = json.load(file)

    for item in items:
        item.pop("timezones")
        item.pop("translations")
        item.pop("emoji")
        item.pop("emojiU")
        if item["native"] == None:
            item["native"] = ""
        item["latitude"] = float(item["latitude"])
        item["longitude"] = float(item["longitude"])
    return items


def connect_to_db(path_to_db):
    connection = sqlite3.connect(path_to_db)
    connection.row_factory = sqlite3.Row
    return connection


def insert_data_countries_to_db(connection, data):
    cursor = connection.cursor()
    cursor.executemany(
        """
        INSERT INTO countries (
            id, name, iso3, iso2, phone_code, capital, currency, currency_symbol, tld, native, region, subregion,
            latitude, longitude
        ) 
        VALUES(
            :id, :name, :iso3, :iso2, :phone_code, :capital, :currency, :currency_symbol, :tld, :native, :region,
            :subregion, :latitude, :longitude
        )
        """, data
    )
    connection.commit()
    cursor.close()


def insert_data_cities_to_db(connection, data):
    cursor = connection.cursor()
    cursor.executemany(
        """
        INSERT INTO cities (
            id, name, state_id, state_code, country_id, country_code, latitude, longitude
        ) 
        VALUES(
            :id, :name, :state_id, :state_code, :country_id, :country_code, :latitude, :longitude
        )
        """, data
    )
    connection.commit()
    cursor.close()


def insert_data_states_to_db(connection, data):
    cursor = connection.cursor()
    cursor.executemany(
        """
        INSERT INTO states (
            id, name, country_id, country_code, state_code, latitude, longitude
        ) 
        VALUES(
            :id, :name, :country_id, :country_code, :state_code, :latitude, :longitude
        )
        """, data
    )
    connection.commit()
    cursor.close()


def top_views_countries_by_name(connection, top=10):
    cursor = connection.cursor()
    res = cursor.execute(
        '''
        SELECT * 
        FROM countries 
        ORDER BY name LIMIT ?
        ''', [top]
    )
    
    items = []
    for row in res.fetchall():
        items.append(dict(row))
    cursor.close()
    
    return items


def count_states_by_countries(connection):
    cursor = connection.cursor()
    res = cursor.execute(
        '''
        SELECT 
            countries.name,
            CAST(COUNT(*) as REAL) as count_states
        FROM countries, states
        WHERE countries.id = states.country_id
        GROUP BY countries.name
        '''
    )

    stat_freq = []
    for row in res.fetchall():
        stat_freq.append(dict(row))

    cursor.close()
    return stat_freq


def update_countries(connection, data):
    cursor = connection.cursor()
    for sample in data:
        value = sample['count_states']
        name = sample['name']
        cursor.execute(
            '''
            UPDATE countries 
            SET count_states = ?
            WHERE name = ?
            ''', [value, name]
        )
    connection.commit()
    cursor.close()


def compute_frequency_capital_by_countries(connection):
    cursor = connection.cursor()
    res = cursor.execute(
        '''
        SELECT 
            CAST(COUNT(*) as REAL) / (SELECT COUNT(*) FROM countries) as count,
            currency
        FROM countries
        GROUP BY currency
        '''
    )

    stat_freq = []
    for row in res.fetchall():
        stat_freq.append(dict(row))

    cursor.close()
    return stat_freq


def statistics_by_latitude_states(connection):
    cursor = connection.cursor()
    res = cursor.execute(
        '''
        SELECT 
            SUM(latitude) as sum,
            AVG(latitude) as avg,
            MIN(latitude) as min, 
            MAX(latitude) as max
        FROM states
        '''
    )
    res = dict(res.fetchone())
    cursor.close()
    return res


def find_asia_cities(connection):
    # Ищем страны принадлежащие региону "Азия" и находящиеся по широте от 30 до 40 и долготе от 0 до 30
    cursor = connection.cursor()
    res = cursor.execute(
        '''
        SELECT countries.name as countries_name, states.name as states_name, cities.name as cities_name
        FROM countries, states, cities
        WHERE (
            cities.country_id = countries.id AND cities.state_id = states.id AND
            countries.region = "Asia" AND states.latitude > 30.0 AND states.latitude < 40.0 AND
            states.longitude > 0.0 AND states.longitude < 30.0
        )
        '''
    )
    
    items = []
    for row in res.fetchall():
        items.append(dict(row))
    cursor.close()
    
    return items

if __name__ == "__main__":
    path_to_file_xml = os.path.join(lESSON_DIR, "data/5/states.xml")
    path_to_file_csv = os.path.join(lESSON_DIR, "data/5/cities.csv")
    path_to_file_json = os.path.join(lESSON_DIR, "data/5/countries.json")
    path_to_db = os.path.join(lESSON_DIR, "data/5/db")
    path_to_file_save = os.path.join(lESSON_DIR, "results/5/r_task_5_top10_name_countries.json")
    path_to_file_save1 = os.path.join(lESSON_DIR, "results/5/r_task_5_count_states_by_countries.json")
    path_to_file_save2 = os.path.join(lESSON_DIR, "results/5/r_task_5_frequency_capital_by_countries.json")
    path_to_file_save3 = os.path.join(lESSON_DIR, "results/5/r_task_5_asia_cities.json")
    
    items_xml = read_xml(path_to_file_xml)
    items_csv = read_csv(path_to_file_csv)
    items_json = read_json(path_to_file_json)

    connection = connect_to_db(path_to_db)

    # insert_data_countries_to_db(connection, items_json)
    # insert_data_states_to_db(connection, items_xml)
    # insert_data_cities_to_db(connection, items_csv)

    data = top_views_countries_by_name(connection)
    with open(path_to_file_save, "w") as file:
        json.dump(data, file, ensure_ascii=False)

    data = count_states_by_countries(connection)
    with open(path_to_file_save1, "w") as file:
        json.dump(data, file, ensure_ascii=False)

    update_countries(connection, data)

    data = compute_frequency_capital_by_countries(connection)
    with open(path_to_file_save2, "w") as file:
        json.dump(data, file, ensure_ascii=False)

    data = statistics_by_latitude_states(connection)
    print(f"Field latitude_states statistics: {data}")

    data = find_asia_cities(connection)
    with open(path_to_file_save3, "w") as file:
        json.dump(data, file, ensure_ascii=False)

    print("States: ", items_xml[0])
    print("Cities", items_csv[0])
    print("Countries", items_json[0])
import os
import csv
import json

import requests
from bs4 import BeautifulSoup


WS_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


if __name__ == '__main__': 
    response = requests.get("https://jsonplaceholder.typicode.com/todos")
    todos: list[dict] = json.loads(response.text)
    soup = BeautifulSoup('', 'html.parser')
    
    table = soup.new_tag('table')
    soup.append(table)
    tr = soup.new_tag('tr')
    table.append(tr)

    for key in todos[0]:
        th = soup.new_tag('th')
        th.string = key
        tr.append(th)

    for todo in todos:
        tr = soup.new_tag('tr')
        table.append(tr)
        for value in todo.values():
            td = soup.new_tag('td')
            td.string = str(value)
            tr.append(td)

    with open(os.path.join("r_text_6_var_1"), "w") as f:
        f.write(soup.prettify())

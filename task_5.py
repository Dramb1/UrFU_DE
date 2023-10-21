import os
import csv

from bs4 import BeautifulSoup


WS_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def write_row_csv(writer, row_data: list) -> None:
    writer.writerow([
        row_data[0].text,
        row_data[1].text,
        row_data[2].text,
        row_data[3].text,
        row_data[4].text,
        ])


if __name__ == '__main__': 
    f = open(os.path.join("r_text_5_var_1"), "w")
    writer = csv.writer(f)
    
    with open(os.path.join(WS_DIR, "задания/5/text_5_var_1")) as f:
        soup = BeautifulSoup(f, 'html.parser')

    list_data = soup.findAll('tr')

    name_rows = list_data[0]
    row_data = name_rows.find_all('th')
    write_row_csv(writer, row_data)

    for data in list_data[1:]:
        row_data = data.find_all('td')
        write_row_csv(writer, row_data)
    
    f.close()

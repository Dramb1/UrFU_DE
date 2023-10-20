import os
import csv


WS_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
TREASHOLD = 25 + (1 % 10)


if __name__ == '__main__': 
    lst_res = []
    avg_sel = 0
    with open(os.path.join(WS_DIR, "задания/4/text_4_var_1"), newline='') as csvfile:
        spamreader = csv.reader(csvfile, delimiter=' ', quotechar='|')
        for row in spamreader:
            lst = row[0].split(',')
            lst.pop()
            lst[-1] = lst[-1][:-1]
            avg_sel += float(lst[-1])
            lst_res.append(lst)

    avg_sel /= len(lst_res)
    ans = []
    for res in lst_res:
        if float(res[-1]) >= avg_sel and int(res[-2]) > TREASHOLD:
            ans.append(res)
    sort_ans = sorted(ans, key=lambda x: int(x[0]))
    
    with open(os.path.join("r_text_4_var_1"), "w") as f:
        writer = csv.writer(f)
        for data in sort_ans:
            writer.writerow(data)

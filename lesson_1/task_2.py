import os


WS_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


if __name__ == '__main__':
    fin = open(os.path.join(WS_DIR, "задания/2/text_2_var_1"), "r")
    fout = open(os.path.join("r_text_2_var_1"), "w")
    for st in fin.readlines():
        lst_nums = list(map(int, st.split("/")))
        fout.write(f"{sum(lst_nums) / len(lst_nums)}\n")

    fin.close()
    fout.close()

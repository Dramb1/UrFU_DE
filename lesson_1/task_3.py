import os


WS_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
TREASHOLD = 50 + 1


def find_indices(lst: list, value: str) -> list[int]:
    indices = []
    for idx, val in enumerate(lst):
        if val == value:
            indices.append(idx)
    return indices


if __name__ == '__main__':
    fin = open(os.path.join(WS_DIR, "задания/3/text_3_var_1"), "r")
    fout = open(os.path.join("r_text_3_var_1"), "w")
    for st in fin.readlines():
        lst_nums = st.split(",")
        inds = find_indices(lst_nums, "NA")
        if inds != []:
            for ind in inds:
               lst_nums[ind] = str((int(lst_nums[ind - 1]) + int(lst_nums[ind + 1])) / 2)
        lst_nums = list(map(float, lst_nums))
        
        lst_res = []
        for num in lst_nums:
            if num ** 0.5 >= TREASHOLD:
                lst_res.append(str(num))
        
        fout.write(f"{','.join(lst_res)}")

    fin.close()
    fout.close()

import numpy as np
import os


WS_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
TREASHOLD = 500 + 1


if __name__ == "__main__":
    path_to_file = os.path.join(WS_DIR, "task_2/2/matrix_1_2.npy")
    array = np.load(path_to_file)

    index_x = []
    index_y = []
    value_z = []

    for i in range(array.shape[0]):
        for j in range(array.shape[1]):
            if array[i, j] > TREASHOLD:
                index_y.append(i)
                index_x.append(j)
                value_z.append(array[i, j])

    path_save = os.path.join(
        WS_DIR, "UrFU_DE/lesson_2/results/r_task_2_var_1.npz"
    )
    np.savez(path_save, x=index_x, y=index_y, z=value_z)
    
    path_savez_comp = os.path.join(
        WS_DIR, "UrFU_DE/lesson_2/results/r_task_2_var_1_compressed.npz"
    )
    np.savez_compressed(path_savez_comp, x=index_x, y=index_y, z=value_z)

    file_stats = os.stat(path_save)
    file_stats_compressed = os.stat(path_savez_comp)
    print("SIZE SAVEZ FILE in Bytes: ", file_stats.st_size)
    print("SIZE SAVEZ_COMPRESSED FILE in Bytes: ", file_stats_compressed.st_size)

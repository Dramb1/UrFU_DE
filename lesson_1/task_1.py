import os
import string


WS_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def string_preprocessing(st1: str) -> str:
    for c in string.punctuation:
        st1 = st1.replace(c, ' ')
    st1 = st1.replace('\n', ' ')
    while '  ' in st1:
        st1 = st1.replace('  ', ' ')
    
    return st1.lstrip().rstrip()


def word_count(st1: str, separator: str = ' ') -> [set, list]:
    st1 = st1.lower()

    list_words = st1.split(separator)
    list_count_words = []

    set_words = set(list_words)
    for word in set_words:
        list_count_words.append(list_words.count(word))

    return set_words, list_count_words


def create_dict(set_words: set, list_count_words: list) -> dict:
    dct = {word: list_count_words[i] for i, word in enumerate(set_words)}
    
    return dict(sorted(dct.items(), key=lambda x: x[1], reverse=True))


def print_result(dct):
    with open("r_text_1_var_1", "w") as f:
        for word, count in dct.items():
            f.write(f"{word}:{count}\n")


if __name__ == '__main__':
    with open(os.path.join(WS_DIR, "задания/1/text_1_var_1"), "r") as f:
        st = " ".join(f.readlines())
    st = string_preprocessing(st)
    set_words, list_count_words = word_count(st)
    dct = create_dict(set_words, list_count_words)
    print_result(dct)

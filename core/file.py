from functools import lru_cache

import jsonpickle


@lru_cache(maxsize=0)
def read(filename: str):
    with open(filename, 'r') as file:
        string = file.read()
    data = jsonpickle.decode(string)

    return data


def write(filename, frozen):
    with open(filename, 'a') as file:
        file.write(frozen)
        # json.dump(self.items, file, cls=MyEncoder,  indent=4)


def cache_stats():
    return read.cache_info()
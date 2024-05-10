import json
import os
import random


class NumberPool:

    def __init__(self, numbers):
        self.__numbers = numbers

    def override(self, n):
        if n not in self.__numbers:
            self.__numbers.append(n)

    def add(self, n):
        if n not in self.__numbers:
            self.__numbers.append(n)

    def remove(self, n):
        if n in self.__numbers:
            self.__numbers.remove(n)

    def all_numbers(self):
        return self.__numbers

    def get(self, index):
        return self.__numbers[index]

    def random_get(self):
        return random.choice(self.__numbers)


def singleton(cls):
    instances = {}

    def getinstance():
        if cls not in instances:
            instances[cls] = cls()
        return instances[cls]

    return getinstance


@singleton
class NumberPoolCache(NumberPool):
    __FILE_KEY = "cache_number_pool.json"
    __cache: NumberPool

    def __init__(self):
        self.init_cache_file()
        super().__init__(self.__get_from_file())

    def init_cache_file(self):
        if not os.path.exists(self.__FILE_KEY):
            with open(self.__FILE_KEY, 'w') as f:
                json.dump([], f)

    def __save_to_file__(self):
        with open(self.__FILE_KEY, 'w') as f:
            json.dump(self.all_numbers(), f)

    def __get_from_file(self):
        with open(self.__FILE_KEY, 'r') as f:
            load = json.load(f)
            return load

    def insert(self, n):
        super().add(n)
        self.__save_to_file__()

    def delete(self, n):
        super().remove(n)
        self.__save_to_file__()

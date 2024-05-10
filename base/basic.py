#!/usr/bin/python3
import sys

# note
"""
note
"""

print("Hello Python!")

a = 1

if a == 1:
    print("TRUE")
elif a == 3:
    print("FALSE")
else:
    print("ERROR")

total = "a" + \
        "b" + \
        "c"

totals = ["a", "b",
          "c"]

word = "word"
word2 = 'word'
paragraph = """this is sentence"""

print(paragraph[3:8])
print(paragraph[3], end=" ")
print(paragraph * 2, end=" ")
print(paragraph + " go out")

# s = input("\n\n 按下1开始")
# print(s)

for i in sys.argv:
    print("py route is", i)

num = 1
s = "str"
numList = [1, 2, 3]
t = ('a', 1)
strSet = {"a", "b"}
d = {1: "one"}

print(d.keys())
print(d.values())

numList.append(8)
for n in numList:
    print("num:", n)

for index, value in enumerate(numList):
    print("enumerate numList")
    print(index)
    print(value)

filterN = [n for n in numList if n % 2 == 0]
mapping = {n: n * 10 for n in numList}

if 1 in filterN:
    print("exist num 1")

newStr = f'Hello {s}'

dic = dict()
dic[1] = 'one'
print(dic)

for key, value in dic.items():
    print("dic content")
    print(key)
    print(value)

match num:
    case 1:
        print("match", 1)
    case 2:
        print("match", 2)
    case _:
        print("non match")

while num != 1:
    print("while", num)
else:
    print("while else")

for n in numList:
    print("for: ", n)
else:
    print("execute loop ending")


class Fruit:
    price = 20

    def announce(self):
        print("my price", self.price)


class Apple(Fruit):
    def run(self):
        self.announce()


Apple().run()

from enum import Enum


class Color(Enum):
    RED = 'red'
    GREEN = 'green'
    BLUE = 'blue'


# color = Color(input("Enter your choice of 'red', 'blue' or 'green': "))
#
# match color:
#     case Color.RED:
#         print("I see red!")
#     case Color.GREEN:
#         print("Grass is green")
#     case Color.BLUE:
#         print("I'm feeling the blues :(")


def lbd(*n):
    sum = 0
    for num in n:
        sum += num
    return lambda x, y: (x + y) * sum


f = lbd(1, 2, 3)

print(f(2, 4))

print('aaa {s} {num}')
print(f'aaa {s} {num}')


def my_function():
    """1231241No1412"""
    q = 1


print(my_function.__doc__)
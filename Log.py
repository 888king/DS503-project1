import random
import pandas


accesses = ['just viewed', 'left a note', 'added a friendship']

whos = []
pages = []
types = []
times = []
for i in range(10000000):
    whos.append(random.randint(1, 200000))
    pages.append(random.randint(1, 200000))
    types.append(accesses[random.randint(0, 2)])
    times.append(random.randint(1, 1000000))
AccessLog = pandas.DataFrame({
    'AccessID': range(1, 10000001),
    'ByWho': whos,
    'WhatPage': pages,
    'TypeOfAccess': types,
    'AccessTime': times
})
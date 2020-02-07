import random
import pandas

n = 200
accesses = ['just viewed', 'left a note', 'added a friendship']

whos = []
pages = []
types = []
times = []
for i in range(n):
    whos.append(random.randint(1, 200000))
    pages.append(random.randint(1, 200000))
    types.append(accesses[random.randint(0, 2)])
    times.append(random.randint(1, 1000000))
    print(i+1, ' records have been generated.')
AccessLog = pandas.DataFrame({
    'AccessID': range(1, n+1),
    'ByWho': whos,
    'WhatPage': pages,
    'TypeOfAccess': types,
    'AccessTime': times
})
print(AccessLog)
AccessLog.to_csv('./AccessLog.csv')

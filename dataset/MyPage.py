import names
import random
import pandas
import pycountry

n = 2000

people = list()
for x in range(1, n):
  people.append(names.get_full_name()[:20])

nums = [x for x in range(125)]
random.seed(10)
random.shuffle(nums)
b = list()
code = list()
for x in range(0, 50):
    country = list(pycountry.countries)[nums[x]]
    b.append(country.name[:20])

people_country = list()
count_code = list()
for x in range(1, n):
    random_index = randrange(len(b))
    people_country.append(b[random_index])
    count_code.append(random_index)

hobby = pandas.read_csv('hobbies.csv')
hobby_list = hobby["hobby"].tolist()
people_hobby = list()
for x in range(1, n):
    people_hobby.append(random.choice(hobby_list))

MyPage = pandas.DataFrame({
    'ID': range(1,n),
    'Name': people,
    'Nationality': people_country,
    'Country_Code': count_code,
    'Hobby': people_hobby})


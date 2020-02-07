import names
import random
import pandas
import pycountry

n = 200
random.seed(10)
hobby_list = pandas.read_csv('hobbies.csv')['hobby'].tolist()
hobby_length = len(hobby_list)
country_length = len(pycountry.countries)

people = []
countries = []
codes = []
hobbies = []
for i in range(n):
    people.append(names.get_full_name())
    code = random.randint(1, country_length)
    codes.append(code)
    countries.append(list(pycountry.countries)[code-1].name)
    hobbies.append(hobby_list[random.randint(0, hobby_length-1)])
MyPage = pandas.DataFrame({
    'ID': range(1, n+1),
    'Name': people,
    'Nationality': countries,
    'CountryCode': codes,
    'Hobby': hobbies
})
print(MyPage['Nationality'])

'''people = list()
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
for x in range(n):
    random_index = random.randint(1, len(b))
    people_country.append(b[random_index - 1])
    count_code.append(random_index)

hobby = pandas.read_csv('hobbies.csv')
hobby_list = hobby["hobby"].tolist()
people_hobby = list()
for x in range(n):
    people_hobby.append(random.choice(hobby_list))

MyPage = pandas.DataFrame({
    'ID': range(1,n+1),
    'Name': people,
    'Nationality': people_country,
    'Country_Code': count_code,
    'Hobby': people_hobby})
MyPage.to_csv('./MyPage.csv')'''

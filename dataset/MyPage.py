import names
import random
import pandas
import pycountry

n = 200000
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
    print(i + 1
          , ' records have been generated.')
MyPage = pandas.DataFrame({
    'ID': range(1, n+1),
    'Name': people,
    'Nationality': countries,
    'CountryCode': codes,
    'Hobby': hobbies
})
print(MyPage['Nationality'])
MyPage.to_csv('./MyPage.csv')
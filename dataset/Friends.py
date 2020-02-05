import random
import pandas


relationships = ['collegefriend', 'girlfriend', 'boyfriend', 'family', 'highschoolfriend', 'childhoodfriend', 'colleague']

IDs = []
friends = []
dates = []
descs = []
for i in range(20000000):
    ID = random.randint(1, 200000)
    IDs.append(ID)
    friend = random.randint(1, 200000)
    while(friend == ID):
        friend = random.randint(1, 200000)
    friends.append(friend)
    dates.append(random.randint(1, 1000000))
    descs.append(relationships[random.randint(0, 6)])
AllFriends = pandas.DataFrame({
    'FriendRel': range(1, 20000001),
    'PersonID': IDs,
    'MyFriend': friends,
    'DateofFriendship': dates,
    'Desc': descs
})
print(AllFriends)
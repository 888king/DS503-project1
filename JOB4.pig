MyPage = LOAD 'MyPage.csv' USING PigStorage(',') as (ID:int, Name: chararray, Nationality: chararray, CountryCode: int, Hobby: chararray);

Friends = LOAD 'AllFriends.csv' USING PigStorage(',') as (FriendRel:int, PersonID: int, MyFriend: int, DateofFriendship: int, Desc: chararray);

Friend_circle = group Friends by MyFriend;
Friends_count = FOREACH Friend_circle GENERATE group as MyFriend, COUNT(Friends) as count;

A = JOIN Friends_count BY MyFriend, MyPage BY ID;
out = FOREACH A GENERATE ID, Name, count;

store out  into 'job4';

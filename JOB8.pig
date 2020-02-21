AllFriends = LOAD 'AllFriends.csv' USING PigStorage(',') as (FriendRel:int, PersonID: int, MyFriend: int, DateofFriendship: int, Desc:chararray);

friends_grp = group Friends by PersonID;
friend_count = FOREACH friends_grp GENERATE group as PersonID, COUNT(Friends) as cnt;

groups = group friend_count ALL;

happyness = FOREACH groups GENERATE FLATTEN(friend_count.PersonID) as PersonID ,AVG(friend_count.cnt) as average;

list = JOIN happyness BY PersonID, friend_count BY PersonID;

fltr = FILTER list by cnt > average;

out = FOREACH fltr GENERATE all_get::PersonID, cnt;

store out into 'job8';

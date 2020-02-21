AllFriends = LOAD 'AllFriends.csv' USING PigStorage(',') as (FriendRel:int, PersonID: int, MyFriend: int, DateofFriendship: int, Desc:chararray);

AccessLog = LOAD 'AccessLog.csv' USING PigStorage(',') as (AccessId:int, ByWho: int, WhatPage: int, TypeOfAccess: chararray, AccessTime:int);

frndgrp = group AllFriends by (PersonID, MyFriend);
flat_frnd = FOREACH frndgrp GENERATE FLATTEN(group) as (PersonID, MyFriend);

accessgrp = group AccessLog by (ByWho, WhatPage);
flat_access = FOREACH accessgrp GENERATE FLATTEN(group) as (ByWho, WhatPage);

access  = JOIN flat_friend BY (PersonID, MyFriend) LEFT OUTER, flat_access BY (ByWho, WhatPage);
filtr = filter access BY (ByWho Is Null);

out = foreach filtr generate PersonID,WhatPage;
store out into 'job7';

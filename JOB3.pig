AccessLog = LOAD 'AccessLog.csv' USING PigStorage(',') as (AccessId:int, ByWho: chararray, WhatPage: int, TypeOfAccess: chararray, AccessTime:int);

pages = Group AccessLog by WhatPage;

Accesses = FOREACH pages GENERATE group as WhatPage, COUNT(AccessLog) as n;

ordered = order Accesses by n DESC;
out = limit ordered 10;
store out into 'job3' using PigStorage(',');

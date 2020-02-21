MyPage = load 'MyPage.csv' using PigStorage(',') AS (ID: int, Name: chararray, Nationality: chararray, CountryCode: int, Hobby: chararray);
AccessLog = LOAD 'AccessLog.csv' USING PigStorage(',') AS (AccessId:int, ByWho:int, WhatPage: int, TypeOfAccess: chararray, AccessTime:int);

People_Log = GROUP AccessLog by ByWho;

access_details = FOREACH People_Log GENERATE group as ByWho, MAX(AccessLog.AccessTime) as max, MIN(AccessLog.AccessTime) as min;

duration = FOREACH access_details GENERATE ByWho, max, min, min+8000 as tenure;

expired = filter duration by max<tenure;

people = FOREACH expired GENERATE ByWho;
joins = join MyPage BY ID, people BY ByWho;

out = FOREACH joins GENERATE ID, Name;
store out into 'job6';



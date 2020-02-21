MyPage = LOAD 'MyPage.csv' USING PigStorage(',') as (ID:int, Name: chararray, Nationality: chararray, CountryCode: int, Hobby:chararray);

Grouped_Nat = GROUP MyPage by Nationality;

Nat_count = FOREACH Grouped_Nat GENERATE FLATTEN(group) as Nationality, COUNT(MyPage) as n;
Out = FOREACH Nat_count GENERATE Nationality, n;
store Out into 'job2' using PigStorage(',');

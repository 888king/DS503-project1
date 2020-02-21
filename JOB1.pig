MyPage = LOAD 'MyPage.csv' USING PigStorage(',') as (ID:int, Name: chararray, Nationality: chararray, CountryCode: int, Hobby:chararray);

MyPage_Nat = FILTER MyPage BY Nationality == 'China';
Out = FOREACH MyPage_Nat GENERATE Name, Hobby, Country;
store Out into 'job1';
